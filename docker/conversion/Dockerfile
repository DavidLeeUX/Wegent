# SPDX-FileCopyrightText: 2025 Weibo, Inc.
#
# SPDX-License-Identifier: Apache-2.0

FROM ghcr.io/wecode-ai/wegent-base-python3.12:1.2.1

# Set working directory
WORKDIR /app

# Copy local workspace dependencies so backend can resolve them via
# `pyproject.toml` [tool.uv.sources] relative paths from /app/backend:
# - wegent-shared = { path = "../shared" } -> /app/shared
# - wegent-chat-shell = { path = "../chat_shell" } -> /app/chat_shell
# - wegent-knowledge-engine = { path = "../knowledge_engine" } -> /app/knowledge_engine
COPY shared /app/shared
COPY chat_shell /app/chat_shell
COPY knowledge_engine /app/knowledge_engine

# Copy pyproject.toml and install dependencies
COPY backend/pyproject.toml /app/backend/pyproject.toml
RUN cd /app/backend && uv pip install --system --no-cache -r pyproject.toml

# Copy application code
COPY backend /app

# Create .env file (if it doesn't exist)
RUN if [ ! -f .env ]; then cp .env.example .env; fi

# Conversion Worker Configuration
# Queue: knowledge_conversion (dedicated queue for document conversion tasks)
# Concurrency: 2 (CPU-intensive tasks, limited by OCR processing capacity)
# Log level: info (production), debug (verbose troubleshooting)

ENV CELERY_QUEUE=knowledge_conversion
ENV CELERY_CONCURRENCY=10
ENV CELERY_LOGLEVEL=info
ENV CELERY_HOSTNAME=conversion-worker@%h

# Start command: Celery worker for knowledge conversion tasks
# Uses exec form for proper signal handling (SIGTERM)
CMD ["sh", "-c", "exec celery -A app.core.celery_app worker \
    --queues=${CELERY_QUEUE} \
    --concurrency=${CELERY_CONCURRENCY} \
    --hostname=${CELERY_HOSTNAME} \
    --loglevel=${CELERY_LOGLEVEL}"]
