import json
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Any
from uuid import UUID

from pydantic import AfterValidator, BaseModel, Field, HttpUrl, field_serializer


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


Money = Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2, allow_inf_nan=False)]


def validate_key(value: str) -> str:
    if not value.strip() or any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError("idempotency key must be nonblank and contain no control characters")
    return value


IdempotencyKey = Annotated[str, Field(min_length=1, max_length=255), AfterValidator(validate_key)]


class PaymentCreate(BaseModel):
    amount: Money
    currency: Currency
    description: str
    metadata: dict[str, Any]
    webhook_url: HttpUrl

    @field_serializer("amount")
    def serialize_amount(self, value: Decimal) -> str:
        return format(value, ".2f")

    def canonical_json(self) -> str:
        return json.dumps(self.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


class WorkEnvelope(BaseModel):
    event_id: UUID
    payment_id: UUID
    work_version: int = Field(ge=0)


class PaymentResponse(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime


class PaymentDetails(PaymentResponse):
    amount: Money
    currency: Currency
    description: str
    metadata: dict[str, Any]
    webhook_url: HttpUrl
    processed_at: datetime | None = None

    @field_serializer("amount")
    def serialize_amount(self, value: Decimal) -> str:
        return format(value, ".2f")


class WebhookNotification(BaseModel):
    event_id: UUID
    payment_id: UUID
    amount: Money
    currency: Currency
    status: PaymentStatus
    processed_at: datetime

    @field_serializer("amount")
    def serialize_amount(self, value: Decimal) -> str:
        return format(value, ".2f")
