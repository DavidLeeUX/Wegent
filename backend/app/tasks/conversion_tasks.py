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
import zipfile
from typing import Optional

import httpx

from app.core.celery_app import celery_app
from app.core.config import settings
from app.core.distributed_lock import distributed_lock
from app.db.session import SessionLocal

logger = logging.getLogger(__name__)

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


def _convert_to_markdown(binary_data: bytes, file_extension: str) -> bytes:
    """
    Convert document binary to Markdown format via MinerU API.

    Supported formats: pdf, docx, doc, pptx, ppt, xlsx, xls (anything MinerU accepts).

    Args:
        binary_data: Original file binary content
        file_extension: Original file extension (e.g., ".pdf", ".docx")

    Returns:
        Markdown content as UTF-8 encoded bytes

    Raises:
        RuntimeError: If the file type is unsupported or MinerU fails
    """
    ext = file_extension.lstrip(".").lower()

    if ext not in _MINERU_MIME_TYPES:
        raise RuntimeError(
            f"Conversion for '{ext}' is not supported. "
            f"Supported types: {', '.join(_MINERU_MIME_TYPES)}"
        )

    return asyncio.run(_convert_with_mineru_async(binary_data, ext))


async def _convert_with_mineru_async(binary_data: bytes, file_extension: str) -> bytes:
    """
    Async implementation of document to Markdown conversion using MinerU API.

    Args:
        binary_data: File binary content
        file_extension: File extension without dot (e.g., "pdf", "docx", "pptx")
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

            # Extract markdown from ZIP
            return _extract_markdown_from_zip(result_resp.content)

        except Exception as e:
            raise RuntimeError(f"Failed to download MinerU result: {e}")


def _extract_markdown_from_zip(zip_content: bytes) -> bytes:
    """
    Extract markdown content from MinerU result ZIP.

    Args:
        zip_content: ZIP file binary content

    Returns:
        Markdown content as UTF-8 encoded bytes
    """
    try:
        with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
            # Find markdown files in the ZIP
            md_files = [f for f in z.namelist() if f.endswith(".md")]

            if not md_files:
                raise RuntimeError("No markdown file found in MinerU result")

            # Read the first markdown file (usually the main content)
            md_file = md_files[0]
            logger.info(f"[MinerU] Extracting markdown: {md_file}")

            content = z.read(md_file)
            return content

    except zipfile.BadZipFile:
        raise RuntimeError("Invalid ZIP file from MinerU result")


@celery_app.task(
    bind=True,
    name="app.tasks.conversion_tasks.convert_document",
    queue=settings.KNOWLEDGE_CONVERSION_QUEUE,
    max_retries=settings.KNOWLEDGE_CONVERSION_LOCK_MAX_RETRIES,
    default_retry_delay=CONVERSION_RETRY_DELAY,
)
def convert_document_task(
    self,
    document_id: int,
    attachment_id: int,
    knowledge_base_id: str,
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

            # Convert to Markdown
            markdown_bytes = _convert_to_markdown(binary_data, file_extension)

            logger.info(
                f"[Conversion] Conversion completed: "
                f"document_id={document_id}, "
                f"original_size={len(binary_data)}, "
                f"markdown_size={len(markdown_bytes)}"
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
