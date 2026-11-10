FROM python:3.12.12-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN pip install --no-cache-dir uv==0.11.27
COPY pyproject.toml uv.lock ./
COPY src ./src
RUN uv sync --locked --no-dev
COPY alembic.ini ./
COPY migrations ./migrations
COPY tools ./tools
RUN useradd --no-create-home --uid 10001 payments
USER payments
ENV PATH="/app/.venv/bin:$PATH"
CMD ["uvicorn", "payment_service.main:app", "--host", "0.0.0.0", "--port", "8000"]
