from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from hmac import compare_digest
from typing import Annotated
from urllib.parse import urlsplit
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import HttpUrl
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from starlette.middleware.base import RequestResponseEndpoint
from starlette.responses import Response

from payment_service.config import Settings, normalize_origin
from payment_service.database import create_engine
from payment_service.schemas import (
    Currency,
    IdempotencyKey,
    PaymentCreate,
    PaymentDetails,
    PaymentResponse,
    PaymentStatus,
)
from payment_service.service import IdempotencyConflict, PaymentService


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    async with request.app.state.sessions() as session:
        yield session


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()  # type: ignore[call-arg]

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_engine(settings)
        app.state.sessions = async_sessionmaker(engine, expire_on_commit=False)
        try:
            yield
        finally:
            await engine.dispose()

    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)

    @app.middleware("http")
    async def authenticate(request: Request, call_next: RequestResponseEndpoint) -> Response:
        key = request.headers.get("X-API-Key")
        if key is None or not compare_digest(
            key.encode("utf-8"), settings.api_key.get_secret_value().encode("utf-8")
        ):
            return JSONResponse(status_code=401, content={"detail": "Invalid API key"})
        return await call_next(request)

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        errors = [
            {key: value for key, value in error.items() if key not in {"input", "ctx"}}
            for error in exc.errors()
        ]
        return JSONResponse(status_code=422, content={"detail": errors})

    session_dep = Depends(get_session)

    @app.get("/openapi.json", include_in_schema=False)
    async def openapi() -> JSONResponse:
        return JSONResponse(app.openapi())

    @app.get("/docs", include_in_schema=False)
    async def docs() -> HTMLResponse:
        return get_swagger_ui_html(openapi_url="/openapi.json", title="API docs")

    @app.get("/ready")
    async def ready(session: Annotated[AsyncSession, session_dep]) -> dict[str, str]:
        await session.execute(text("SELECT 1"))
        return {"status": "ready"}

    @app.post("/api/v1/payments", status_code=202, response_model=PaymentResponse)
    async def create_payment(
        data: PaymentCreate,
        idempotency_key: Annotated[IdempotencyKey, Header()],
        session: Annotated[AsyncSession, session_dep],
    ) -> PaymentResponse:
        url = urlsplit(str(data.webhook_url))
        if normalize_origin(f"{url.scheme}://{url.netloc}") not in settings.webhook_allowed_origins:
            raise HTTPException(status_code=422, detail="Webhook origin is not allowed")
        try:
            payment = await PaymentService(session).create(idempotency_key, data)
        except IdempotencyConflict as exc:
            raise HTTPException(status_code=409, detail="Idempotency conflict") from exc
        return PaymentResponse(
            payment_id=payment.id,
            status=PaymentStatus(payment.status),
            created_at=payment.created_at,
        )

    @app.get("/api/v1/payments/{payment_id}", response_model=PaymentDetails)
    async def get_payment(
        payment_id: UUID, session: Annotated[AsyncSession, session_dep]
    ) -> PaymentDetails:
        payment = await PaymentService(session).get(payment_id)
        if payment is None:
            raise HTTPException(status_code=404, detail="Payment not found")
        return PaymentDetails(
            payment_id=payment.id,
            status=PaymentStatus(payment.status),
            created_at=payment.created_at,
            idempotency_key=payment.idempotency_key,
            amount=payment.amount,
            currency=Currency(payment.currency),
            description=payment.description,
            metadata=payment.payment_metadata,
            webhook_url=HttpUrl(payment.webhook_url),
            processed_at=payment.processed_at,
        )

    return app
