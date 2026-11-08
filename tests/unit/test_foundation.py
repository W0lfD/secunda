from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from pydantic import TypeAdapter, ValidationError

from payment_service.config import Settings
from payment_service.schemas import (
    IdempotencyKey,
    PaymentCreate,
    PaymentResponse,
    WebhookNotification,
    WorkEnvelope,
)


def payment(**overrides: object) -> PaymentCreate:
    values: dict[str, object] = {
        "amount": "12.50",
        "currency": "USD",
        "description": "order",
        "metadata": {"b": 2, "a": 1},
        "webhook_url": "https://example.com/hook",
    }
    return PaymentCreate.model_validate(values | overrides)


@pytest.mark.parametrize("amount", ["0", "-1", "1.001", "10000000000000000", "NaN"])
def test_invalid_amount_rejected(amount: str) -> None:
    with pytest.raises(ValidationError):
        payment(amount=amount)


def test_currency_and_url_validation() -> None:
    for values in ({"currency": "GBP"}, {"webhook_url": "ftp://example.com"}):
        with pytest.raises(ValidationError):
            payment(**values)


@pytest.mark.parametrize("key", ["", " ", "a" * 256, "with\nnewline"])
def test_invalid_idempotency_key_rejected(key: str) -> None:
    with pytest.raises(ValidationError):
        TypeAdapter(IdempotencyKey).validate_python(key)


def test_canonical_request_normalizes_amount_and_object_order() -> None:
    first = payment(amount="12.5", metadata={"b": 2, "a": {"y": 2, "x": 1}})
    second = payment(amount="12.50", metadata={"a": {"x": 1, "y": 2}, "b": 2})
    assert first.canonical_json() == second.canonical_json()
    assert first.canonical_json() != payment(amount="12.51").canonical_json()


def test_settings_reject_invalid_secrets_origins_and_timeouts() -> None:
    base = {"api_key": "a" * 32, "webhook_allowed_origins": ["https://example.com"]}
    assert Settings(**base).webhook_allowed_origins == ["https://example.com"]
    for overrides in (
        {"api_key": ""},
        {"webhook_allowed_origins": ["https://example.com/path"]},
        {"webhook_allowed_origins": ["https://user@example.com"]},
        {"webhook_allowed_origins": ["https://bad host"]},
        {"webhook_allowed_origins": []},
        {"webhook_timeout_seconds": 0},
        {"webhook_timeout_seconds": 301},
    ):
        with pytest.raises(ValidationError):
            Settings(**(base | overrides))


def test_wire_schemas_serialize_money_as_fixed_decimal_string() -> None:
    timestamp = datetime(2025, 1, 1, tzinfo=UTC)
    ident = UUID("00000000-0000-0000-0000-000000000001")
    response = PaymentResponse(payment_id=ident, status="pending", created_at=timestamp)
    envelope = WorkEnvelope(event_id=ident, payment_id=ident, work_version=0)
    notification = WebhookNotification(
        event_id=ident,
        payment_id=ident,
        amount=Decimal("12.5"),
        currency="USD",
        status="succeeded",
        processed_at=timestamp,
    )
    assert response.model_dump(mode="json")["payment_id"] == str(ident)
    assert envelope.model_dump(mode="json")["work_version"] == 0
    assert notification.model_dump(mode="json")["amount"] == "12.50"
