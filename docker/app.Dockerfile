# Application image: simulator, loader, stream processor and dashboard (different commands).
FROM python:3.11.18-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv

RUN pip install --no-cache-dir uv==0.11.22

WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --locked --no-dev --extra loader --extra stream --extra dashboard --no-install-project

COPY src ./src
COPY infra ./infra
RUN uv sync --locked --no-dev --extra loader --extra stream --extra dashboard

ENV PATH="/app/.venv/bin:${PATH}"
