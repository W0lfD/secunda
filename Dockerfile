FROM python:3.12.12-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN pip install --no-cache-dir uv==0.11.27
COPY pyproject.toml uv.lock ./
COPY src ./src
RUN uv sync --locked --no-dev
ENV PATH="/app/.venv/bin:$PATH"
