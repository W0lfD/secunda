import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest

from payment_service.gateway import Gateway
from payment_service.schemas import WebhookNotification
from payment_service.webhook import WebhookClient

pytestmark = pytest.mark.unit


def notification():
    return WebhookNotification(
        event_id=uuid4(),
        payment_id=uuid4(),
        amount=Decimal("1.00"),
        currency="USD",
        status="succeeded",
        processed_at=datetime.now(UTC),
    )


@pytest.mark.parametrize(
    "url", ["https://evil.example/callback", "https://user:secret@example.com/cb"]
)
async def test_adapter_refuses_disallowed_origin_before_http(url):
    called = []

    async def http(request):
        called.append(request)
        return httpx.Response(200)

    webhook = WebhookClient(["https://example.com"], 0.1, transport=httpx.MockTransport(http))
    try:
        with pytest.raises(ValueError):
            await webhook.send(url, notification())
        assert called == []
    finally:
        await webhook.close()


async def test_redirect_is_failure_without_following_location():
    urls = []

    async def http(request):
        urls.append(str(request.url))
        return httpx.Response(302, headers={"location": "https://evil.example/"})

    webhook = WebhookClient(["https://example.com"], 0.1, transport=httpx.MockTransport(http))
    try:
        with pytest.raises(httpx.HTTPStatusError):
            await webhook.send("https://example.com/cb", notification())
        assert urls == ["https://example.com/cb"]
    finally:
        await webhook.close()


async def test_total_webhook_deadline_covers_slow_response():
    async def http(request):
        await asyncio.Event().wait()

    webhook = WebhookClient(["https://example.com"], 0.01, transport=httpx.MockTransport(http))
    try:
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(webhook.send("https://example.com", notification()), 1)
    finally:
        await webhook.close()


@pytest.mark.parametrize(
    ("sample", "outcome"), [(0.0, "succeeded"), (0.899, "succeeded"), (0.9, "failed")]
)
async def test_gateway_probability_boundary_and_delay(monkeypatch, sample, outcome):
    delays = []

    async def sleep(delay):
        delays.append(delay)

    monkeypatch.setattr("payment_service.gateway.asyncio.sleep", sleep)
    monkeypatch.setattr(
        "payment_service.gateway.random.uniform", lambda low, high: (low + high) / 2
    )
    monkeypatch.setattr("payment_service.gateway.random.random", lambda: sample)
    gateway = Gateway()
    assert await gateway.process(None) == outcome
    assert delays == [3.5]
    await gateway.close()
