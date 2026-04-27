# SPDX-FileCopyrightText: 2026 Weibo, Inc.
#
# SPDX-License-Identifier: Apache-2.0

"""
Celery tasks for document format conversion.

Converts documents (PDF, PPTX, etc.) to Markdown format before RAG indexing.
These tasks run on dedicated conversion workers via a separate queue,
isolated from the main Celery worker.

Conversion worker startup (on dedicated conversion machine):
    uv run celery -A app.core.celery_app worker \
        --queues=knowledge_conversion \
        --concurrency=2

The main worker (uv run celery -A app.core.celery_app worker) does NOT
consume conversion tasks because task_routes directs them to the
knowledge_conversion queue.
"""

import asyncio
import io
import logging
import os
import re
import zipfile
from typing import Optional, Tuple
from urllib.parse import quote

import httpx

from app.core.celery_app import celery_app
from app.core.config import settings
from app.core.distributed_lock import distributed_lock
from app.db.session import SessionLocal

logger = logging.getLogger(__name__)

# Lazy import boto3 for S3 operations
_s3_client = None


def _get_s3_client():
    """Get or create S3 client for image upload."""
    global _s3_client
    if _s3_client is None and settings.WORKER_CONVERSION_S3_ENABLED:
        try:
            import boto3

            _s3_client = boto3.client(
                "s3",
                endpoint_url=settings.WORKER_CONVERSION_S3_ENDPOINT,
                aws_access_key_id=settings.WORKER_CONVERSION_S3_ACCESS_KEY,
                aws_secret_access_key=settings.WORKER_CONVERSION_S3_SECRET_KEY,
                region_name=settings.WORKER_CONVERSION_S3_REGION_NAME,
            )
            logger.info(
                f"[S3] S3 client initialized for endpoint: {settings.WORKER_CONVERSION_S3_ENDPOINT}"
            )
        except Exception as e:
            logger.error(f"[S3] Failed to initialize S3 client: {e}")
            raise
    return _s3_client


CONVERSION_LOCK_TIMEOUT = settings.KNOWLEDGE_CONVERSION_LOCK_TIMEOUT_SECONDS
CONVERSION_LOCK_EXTEND = settings.KNOWLEDGE_CONVERSION_LOCK_EXTEND_INTERVAL_SECONDS
CONVERSION_RETRY_DELAY = settings.KNOWLEDGE_CONVERSION_LOCK_RETRY_DELAY_SECONDS


_MINERU_MIME_TYPES = {
    "pdf": "application/pdf",
    "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "doc": "application/msword",
    "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    "ppt": "application/vnd.ms-powerpoint",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "xls": "application/vnd.ms-excel",
}


def _convert_to_markdown(
    binary_data: bytes, file_extension: str, base_dir_name: Optional[str] = None
) -> Tuple[bytes, list]:
    """
    Convert document binary to Markdown format via MinerU API.

    Supported formats: pdf, docx, doc, pptx, ppt, xlsx, xls (anything MinerU accepts).

    Args:
        binary_data: Original file binary content
        file_extension: Original file extension (e.g., ".pdf", ".docx")
        base_dir_name: Base directory name for S3 upload (derived from original filename)

    Returns:
        Tuple of (markdown content as bytes, uploaded_image_urls list)

    Raises:
        RuntimeError: If the file type is unsupported or MinerU fails
    """
    ext = file_extension.lstrip(".").lower()

    if ext not in _MINERU_MIME_TYPES:
        raise RuntimeError(
            f"Conversion for '{ext}' is not supported. "
            f"Supported types: {', '.join(_MINERU_MIME_TYPES)}"
        )

    return asyncio.run(_convert_with_mineru_async(binary_data, ext, base_dir_name))


async def _convert_with_mineru_async(
    binary_data: bytes, file_extension: str, base_dir_name: Optional[str] = None
) -> Tuple[bytes, list]:
    """
    Async implementation of document to Markdown conversion using MinerU API.

    Args:
        binary_data: File binary content
        file_extension: File extension without dot (e.g., "pdf", "docx", "pptx")
        base_dir_name: Base directory name for S3 upload (derived from original filename)

    Returns:
        Tuple of (markdown content as bytes, uploaded_image_urls list)
    """
    if not settings.MINERU_API_BASE_URL:
        raise RuntimeError("MINERU_API_BASE_URL is not configured")

    mime_type = _MINERU_MIME_TYPES.get(file_extension, "application/octet-stream")
    filename = f"document.{file_extension}"
    base_url = settings.MINERU_API_BASE_URL.rstrip("/")
    task_id = None

    async with httpx.AsyncClient() as client:
        # Step 1: Submit task
        try:
            submit_url = f"{base_url}/tasks"
            data = {
                "backend": settings.MINERU_BACKEND,
                "parse_method": settings.MINERU_PARSE_METHOD,
                "lang_list": settings.MINERU_LANG_LIST,
                "formula_enable": "true" if settings.MINERU_FORMULA_ENABLE else "false",
                "table_enable": "true" if settings.MINERU_TABLE_ENABLE else "false",
                "return_md": "true",
                "return_images": "true",
                "response_format_zip": "true",
            }

            files = {"files": (filename, binary_data, mime_type)}

            logger.info(f"[MinerU] Submitting task to {submit_url}")
            response = await client.post(
                submit_url, data=data, files=files, timeout=60.0
            )
            response.raise_for_status()

            result = response.json()
            task_id = (
                result.get("task_id") if isinstance(result, dict) else result.strip('"')
            )
            logger.info(f"[MinerU] Task submitted: {task_id}")

        except Exception as e:
            raise RuntimeError(f"Failed to submit MinerU task: {e}")

        # Step 2: Poll for completion
        start_time = asyncio.get_event_loop().time()
        max_wait = settings.MINERU_MAX_WAIT_SECONDS
        poll_interval = settings.MINERU_POLL_INTERVAL_SECONDS

        while True:
            elapsed = asyncio.get_event_loop().time() - start_time
            if elapsed > max_wait:
                raise RuntimeError(f"MinerU task timeout after {max_wait}s: {task_id}")

            try:
                status_url = f"{base_url}/tasks/{task_id}"
                status_resp = await client.get(status_url, timeout=10.0)
                status_resp.raise_for_status()

                status_data = status_resp.json()
                status = (
                    status_data.get("status", "").lower()
                    if isinstance(status_data, dict)
                    else status_data.strip('"').lower()
                )

                if status in ["completed", "done", "success"]:
                    logger.info(f"[MinerU] Task completed: {task_id}")
                    break
                elif status in ["failed", "error"]:
                    raise RuntimeError(f"MinerU task failed: {task_id}")
                else:
                    logger.debug(f"[MinerU] Task status: {status}, waiting...")
                    await asyncio.sleep(poll_interval)

            except Exception as e:
                if isinstance(e, RuntimeError):
                    raise
                logger.warning(f"[MinerU] Status check error: {e}")
                await asyncio.sleep(poll_interval)

        # Step 3: Download result
        try:
            result_url = f"{base_url}/tasks/{task_id}/result"
            logger.info(f"[MinerU] Downloading result from {result_url}")

            result_resp = await client.get(result_url, timeout=120.0)
            result_resp.raise_for_status()

            content_type = result_resp.headers.get("content-type", "")

            if "application/json" in content_type:
                raise RuntimeError("MinerU returned JSON instead of ZIP")

            # Extract markdown from ZIP (with optional S3 image upload)
            markdown_bytes, uploaded_images = _extract_markdown_from_zip(
                result_resp.content, base_dir_name
            )
            return markdown_bytes, uploaded_images

        except Exception as e:
            raise RuntimeError(f"Failed to download MinerU result: {e}")


def _upload_image_to_s3(
    image_data: bytes, object_name: str, content_type: str = "image/jpeg"
) -> Optional[str]:
    """Upload image to S3 and return the public URL."""
    try:
        s3_client = _get_s3_client()
        if s3_client is None:
            return None

        bucket = settings.WORKER_CONVERSION_S3_BUCKET_NAME

        # Upload using the original object_name (S3 supports UTF-8)
        s3_client.upload_fileobj(
            io.BytesIO(image_data),
            bucket,
            object_name,
            ExtraArgs={"ContentType": content_type},
        )

        # Construct public URL with proper URL encoding for Chinese characters
        # Split path and encode each segment to handle Chinese characters
        endpoint = settings.WORKER_CONVERSION_S3_ENDPOINT.rstrip("/")

        # URL encode the path segments for Chinese character support
        # e.g., "我的知识库/文档名/images/xxx.jpg" -> "%E6%88%91%E7%9A%84%E7%9F%A5%E8%AF%86%E5%BA%93/%E6%96%87%E6%A1%A3%E5%90%8D/images/xxx.jpg"
        path_parts = object_name.split("/")
        encoded_parts = [quote(part, safe="") for part in path_parts]
        encoded_path = "/".join(encoded_parts)

        public_url = f"{endpoint}/{bucket}/{encoded_path}"

        logger.info(f"[S3] Uploaded image: {object_name} -> {public_url}")
        return public_url

    except Exception as e:
        logger.error(f"[S3] Failed to upload image {object_name}: {e}")
        return None


def _extract_markdown_from_zip(
    zip_content: bytes, base_dir_name: Optional[str] = None
) -> Tuple[bytes, list]:
    """
    Extract markdown content from MinerU result ZIP.

    Args:
        zip_content: ZIP file binary content
        base_dir_name: Base directory name for S3 upload (derived from original filename)

    Returns:
        Tuple of (markdown content as UTF-8 encoded bytes, uploaded_image_urls list)
    """
    uploaded_images: list = []

    try:
        with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
            # Find markdown files in the ZIP
            md_files = [f for f in z.namelist() if f.endswith(".md")]

            if not md_files:
                raise RuntimeError("No markdown file found in MinerU result")

            # Read the first markdown file (usually the main content)
            md_file = md_files[0]
            logger.info(f"[MinerU] Extracting markdown: {md_file}")

            content = z.read(md_file).decode("utf-8")

            # If S3 is enabled, upload images and replace references
            if settings.WORKER_CONVERSION_S3_ENABLED and base_dir_name:
                # Find all image files in the ZIP for logging/debugging
                all_image_files = [
                    f
                    for f in z.namelist()
                    if f.lower().endswith(
                        (
                            ".png",
                            ".jpg",
                            ".jpeg",
                            ".gif",
                            ".webp",
                            ".svg",
                            ".bmp",
                            ".tiff",
                        )
                    )
                ]
                logger.info(
                    f"[S3] Found {len(all_image_files)} images in ZIP: {all_image_files}"
                )

                def process_image_upload(
                    zf: zipfile.ZipFile,
                    img_path: str,
                    alt_text: str,
                    base_dir: str,
                    original_ref: Optional[str],
                ) -> str:
                    """Helper to process single image upload."""
                    # Try to find the image in ZIP with various path strategies
                    possible_paths = [img_path]

                    # Strategy 1: Try with ./ prefix
                    possible_paths.append(f"./{img_path}")

                    # Strategy 2: Try just the filename (flat structure)
                    flat_name = os.path.basename(img_path)
                    possible_paths.append(flat_name)

                    # Strategy 3: Try common MinerU directory prefixes
                    # MinerU often puts images in document/ocr/images/ or similar
                    if not img_path.startswith("document/"):
                        possible_paths.append(f"document/{img_path}")
                        possible_paths.append(f"document/ocr/{img_path}")
                        possible_paths.append(f"./document/{img_path}")
                        possible_paths.append(f"./document/ocr/{img_path}")

                    # Strategy 4: Search for partial path match
                    # If img_path is "images/xxx.jpg", find any path ending with it
                    if "/" in img_path:
                        path_suffix = img_path
                        flat_name_only = os.path.basename(img_path)
                        for zip_path in zf.namelist():
                            if zip_path.endswith(path_suffix) or zip_path.endswith(
                                flat_name_only
                            ):
                                if zip_path not in possible_paths:
                                    possible_paths.append(zip_path)

                    # Remove duplicates while preserving order
                    seen = set()
                    unique_paths = []
                    for p in possible_paths:
                        if p not in seen:
                            seen.add(p)
                            unique_paths.append(p)
                    possible_paths = unique_paths

                    logger.debug(
                        f"[S3] Trying paths for {img_path}: {possible_paths[:5]}..."
                    )  # Log first 5

                    for try_path in possible_paths:
                        if try_path in zf.namelist():
                            try:
                                img_data = zf.read(try_path)

                                ext = os.path.splitext(try_path)[1].lower()
                                content_type_map = {
                                    ".png": "image/png",
                                    ".jpg": "image/jpeg",
                                    ".jpeg": "image/jpeg",
                                    ".gif": "image/gif",
                                    ".webp": "image/webp",
                                    ".svg": "image/svg+xml",
                                    ".bmp": "image/bmp",
                                    ".tiff": "image/tiff",
                                    ".tif": "image/tiff",
                                }
                                content_type = content_type_map.get(ext, "image/jpeg")

                                # For S3 object name, use relative path from ZIP root or just the basename
                                # Prefer to preserve directory structure if it exists
                                s3_object_name = f"{base_dir}/{try_path}"
                                s3_url = _upload_image_to_s3(
                                    img_data, s3_object_name, content_type
                                )

                                if s3_url:
                                    uploaded_images.append((try_path, s3_url))
                                    return (
                                        f"![{alt_text}]({s3_url})"
                                        if alt_text
                                        else s3_url
                                    )

                            except Exception as e:
                                logger.warning(
                                    f"[S3] Failed to process image {try_path}: {e}"
                                )
                                continue

                    logger.warning(f"[S3] Image not found or upload failed: {img_path}")
                    return original_ref if original_ref else img_path

                # Process markdown image references: ![alt](path)
                md_img_pattern = r"!\[([^\]]*)\]\(([^)]+)\)"

                def replace_md_image_ref(match):
                    alt_text = match.group(1)
                    img_path = match.group(2)

                    if img_path.startswith(("http://", "https://")):
                        return match.group(0)

                    img_path = img_path.lstrip("./").lstrip("/")
                    return process_image_upload(
                        z, img_path, alt_text, base_dir_name, match.group(0)
                    )

                content = re.sub(md_img_pattern, replace_md_image_ref, content)

                # Process HTML img tags: <img src="path" ...>
                html_img_pattern = r'<img[^>]+src=["\']([^"\']+)["\'][^>]*>'

                def replace_html_image_ref(match):
                    img_path = match.group(1)

                    if img_path.startswith(("http://", "https://")):
                        return match.group(0)

                    img_path = img_path.lstrip("./").lstrip("/")
                    s3_url = process_image_upload(z, img_path, "", base_dir_name, None)
                    if s3_url and s3_url != match.group(0):
                        return match.group(0).replace(match.group(1), s3_url)
                    return match.group(0)

                content = re.sub(
                    html_img_pattern,
                    replace_html_image_ref,
                    content,
                    flags=re.IGNORECASE,
                )

                logger.info(
                    f"[S3] Uploaded {len(uploaded_images)} images for document: {base_dir_name}"
                )

            return content.encode("utf-8"), uploaded_images

    except zipfile.BadZipFile:
        raise RuntimeError("Invalid ZIP file from MinerU result")


@celery_app.task(
    bind=True,
    name="app.tasks.conversion_tasks.convert_document",
    queue=settings.KNOWLEDGE_CONVERSION_QUEUE,
    max_retries=settings.KNOWLEDGE_CONVERSION_LOCK_MAX_RETRIES,
    default_retry_delay=CONVERSION_RETRY_DELAY,
    # Conversion with S3 image upload can take longer than default timeout
    # Set longer limits: 30 minutes soft, 35 minutes hard
    soft_time_limit=9000,
    time_limit=10000,
)
def convert_document_task(
    self,
    document_id: int,
    attachment_id: int,
    knowledge_base_id: str,
    knowledge_base_name: str,
    index_generation: int,
    user_id: int,
    user_name: str,
    file_extension: str,
    original_filename: str,
    # Pass-through parameters for index_document_task
    retriever_name: str,
    retriever_namespace: str,
    embedding_model_name: str,
    embedding_model_namespace: str,
    splitter_config_dict: Optional[dict] = None,
    trigger_summary: bool = True,
):
    """
    Convert a document to Markdown format and then dispatch indexing.

    State machine flow:
        QUEUED -> CONVERTING -> (overwrite attachment) -> QUEUED -> index_document_task

    This task:
    1. Acquires a distributed lock for the document
    2. Transitions state: QUEUED -> CONVERTING
    3. Loads the attachment binary from storage
    4. Converts the binary to Markdown via _convert_to_markdown()
    5. Overwrites the attachment with Markdown content (file_extension -> .md)
    6. Transitions state: CONVERTING -> QUEUED
    7. Dispatches index_document_task to the default queue
    """
    from app.services.knowledge.index_state_machine import (
        mark_document_conversion_started,
        mark_document_conversion_succeeded,
        mark_document_index_failed,
    )

    task_id = getattr(self.request, "id", "unknown")
    retry_count = getattr(self.request, "retries", 0)
    worker_hostname = getattr(self.request, "hostname", "unknown")

    logger.info(
        f"[Conversion] Task started: task_id={task_id}, "
        f"worker={worker_hostname}, retry={retry_count}/{self.max_retries}, "
        f"document_id={document_id}, file_ext={file_extension}, "
        f"index_generation={index_generation}"
    )

    # Acquire distributed lock to prevent concurrent conversion
    lock_name = f"knowledge:convert_document:{document_id}"
    with distributed_lock.acquire_watchdog_context(
        lock_name,
        expire_seconds=CONVERSION_LOCK_TIMEOUT,
        extend_interval_seconds=CONVERSION_LOCK_EXTEND,
    ) as acquired:
        if not acquired:
            if retry_count < self.max_retries:
                logger.warning(
                    f"[Conversion] Lock held, scheduling retry: "
                    f"task_id={task_id}, document_id={document_id}, "
                    f"retry={retry_count + 1}/{self.max_retries}, "
                    f"countdown={CONVERSION_RETRY_DELAY}s"
                )
                raise self.retry(
                    exc=RuntimeError(
                        f"conversion_lock_held:{document_id}:{index_generation}"
                    ),
                    countdown=CONVERSION_RETRY_DELAY,
                )

            logger.warning(
                f"[Conversion] Lock retry exhausted: task_id={task_id}, "
                f"document_id={document_id}, index_generation={index_generation}"
            )
            return {
                "status": "skipped",
                "reason": "lock_retry_exhausted",
                "document_id": document_id,
                "index_generation": index_generation,
            }

        # State transition: QUEUED -> CONVERTING
        with SessionLocal() as db:
            start_decision = mark_document_conversion_started(
                db=db,
                document_id=document_id,
                generation=index_generation,
            )

        if not start_decision.should_execute:
            logger.info(
                f"[Conversion] Skipped: task_id={task_id}, "
                f"document_id={document_id}, reason={start_decision.reason}"
            )
            return {
                "status": "skipped",
                "reason": start_decision.reason,
                "document_id": document_id,
                "index_generation": index_generation,
            }

        try:
            # Load attachment binary from storage
            from app.models.subtask_context import ContextType, SubtaskContext
            from app.services.context.context_service import context_service

            with SessionLocal() as db:
                context = (
                    db.query(SubtaskContext)
                    .filter(
                        SubtaskContext.id == attachment_id,
                        SubtaskContext.context_type == ContextType.ATTACHMENT.value,
                    )
                    .first()
                )
                if not context:
                    raise ValueError(f"Attachment {attachment_id} not found")

                binary_data = context_service.get_attachment_binary_data(
                    db=db, context=context
                )
                if binary_data is None:
                    raise ValueError(f"Attachment {attachment_id} has no binary data")

            logger.info(
                f"[Conversion] Loaded attachment binary: "
                f"attachment_id={attachment_id}, size={len(binary_data)} bytes"
            )

            # Generate S3 path: {knowledge_base_name}/{filename_without_ext}
            # S3 object path format: {knowledge_base_name}/{filename}/images/xxx.jpg
            filename_without_ext = os.path.splitext(original_filename)[0]
            s3_base_path = f"{knowledge_base_name}/{filename_without_ext}"

            # Convert to Markdown (with optional S3 image upload)
            markdown_bytes, uploaded_images = _convert_to_markdown(
                binary_data, file_extension, s3_base_path
            )

            logger.info(
                f"[Conversion] Conversion completed: "
                f"document_id={document_id}, "
                f"original_size={len(binary_data)}, "
                f"markdown_size={len(markdown_bytes)}, "
                f"images_uploaded={len(uploaded_images)}"
            )

            # Overwrite attachment with Markdown content
            # The .md filename causes overwrite_attachment to update
            # type_data.file_extension to ".md", which the indexer uses
            md_filename = f"{original_filename}.md"
            with SessionLocal() as db:
                context_service.overwrite_attachment(
                    db=db,
                    context_id=attachment_id,
                    user_id=user_id,
                    filename=md_filename,
                    binary_data=markdown_bytes,
                )

            logger.info(
                f"[Conversion] Attachment overwritten: "
                f"attachment_id={attachment_id}, "
                f"new_filename={md_filename}"
            )

            # State transition: CONVERTING -> QUEUED
            with SessionLocal() as db:
                succeeded = mark_document_conversion_succeeded(
                    db=db,
                    document_id=document_id,
                    generation=index_generation,
                )

            if not succeeded:
                logger.warning(
                    f"[Conversion] State transition to QUEUED failed (stale): "
                    f"document_id={document_id}, "
                    f"index_generation={index_generation}"
                )
                return {
                    "status": "skipped",
                    "reason": "stale_conversion",
                    "document_id": document_id,
                    "index_generation": index_generation,
                }

            # Dispatch indexing task to the default queue
            from app.tasks.knowledge_tasks import index_document_task

            async_result = index_document_task.delay(
                knowledge_base_id=knowledge_base_id,
                attachment_id=attachment_id,
                retriever_name=retriever_name,
                retriever_namespace=retriever_namespace,
                embedding_model_name=embedding_model_name,
                embedding_model_namespace=embedding_model_namespace,
                user_id=user_id,
                user_name=user_name,
                document_id=document_id,
                index_generation=index_generation,
                splitter_config_dict=splitter_config_dict,
                trigger_summary=trigger_summary,
            )

            logger.info(
                f"[Conversion] Completed and indexing dispatched: "
                f"task_id={task_id}, document_id={document_id}, "
                f"index_generation={index_generation}, "
                f"index_task_id={async_result.id}"
            )
            return {
                "status": "converted",
                "document_id": document_id,
                "index_generation": index_generation,
                "index_task_id": async_result.id,
            }

        except Exception as exc:
            # Mark as failed on any exception
            with SessionLocal() as db:
                mark_document_index_failed(
                    db=db,
                    document_id=document_id,
                    generation=index_generation,
                )

            logger.error(
                f"[Conversion] Error: task_id={task_id}, "
                f"document_id={document_id}, "
                f"index_generation={index_generation}, error={exc}",
                exc_info=True,
            )
            raise
