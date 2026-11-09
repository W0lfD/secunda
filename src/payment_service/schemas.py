import json
import math
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Any
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    Field,
    HttpUrl,
    field_serializer,
    field_validator,
)


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


def reject_url_userinfo(value: Any) -> Any:
    url = str(value)
    authority = urlsplit(url).netloc
    if "\\" in url or not authority or "@" in authority:
        raise ValueError("webhook URL must have an authority without userinfo")
    return value


WebhookUrl = Annotated[HttpUrl, BeforeValidator(reject_url_userinfo)]


class PaymentCreate(BaseModel):
    amount: Money
    currency: Currency
    description: str
    metadata: dict[str, Any]
    webhook_url: WebhookUrl

    @field_validator("metadata")
    @classmethod
    def validate_metadata(cls, value: dict[str, Any]) -> dict[str, Any]:
        def check(item: Any) -> None:
            if isinstance(item, float) and not math.isfinite(item):
                raise ValueError("metadata must contain finite numbers")
            if isinstance(item, dict):
                for nested in item.values():
                    check(nested)
            elif isinstance(item, list):
                for nested in item:
                    check(nested)

        check(value)
        return value

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
    idempotency_key: str
    amount: Money
    currency: Currency
    description: str
    metadata: dict[str, Any]
    webhook_url: WebhookUrl
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
