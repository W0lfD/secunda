import asyncio
import os
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from payment_service.app import create_app
from payment_service.config import Settings

pytestmark = pytest.mark.integration
KEY = "a" * 32
URL = os.environ.get("TEST_DATABASE_URL", "")
BODY = {
    "amount": "12.00",
    "currency": "USD",
    "description": "order",
    "metadata": {"a": 1, "b": 2},
    "webhook_url": "https://example.com/hook",
}


@pytest.fixture
async def client():
    if not URL or not URL.rsplit("/", 1)[-1].endswith("_test"):
        pytest.skip("TEST_DATABASE_URL must point to an explicit *_test database")
    engine = create_async_engine(URL)
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE outbox, payments CASCADE"))
    app = create_app(
        Settings(
            api_key=KEY,
            webhook_allowed_origins=["https://example.com"],
            database_url=URL,
            database_timeout_seconds=0.5,
        )
    )
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        yield client
    await engine.dispose()


async def test_create_get_and_idempotency(client):
    headers = {"X-API-Key": KEY, "Idempotency-Key": "abc"}
    response = await client.post("/api/v1/payments", headers=headers, json=BODY)
    assert response.status_code == 202
    payment_id = response.json()["payment_id"]
    assert response.json()["status"] == "pending"
    details = await client.get("/api/v1/payments/" + payment_id, headers=headers)
    assert details.status_code == 200
    assert details.json()["idempotency_key"] == "abc"
    assert details.json()["amount"] == "12.00"
    assert details.json()["metadata"] == {"a": 1, "b": 2}
    equivalent = {**BODY, "amount": "12.0", "metadata": {"b": 2, "a": 1}}
    assert (await client.post("/api/v1/payments", headers=headers, json=equivalent)).json()[
        "payment_id"
    ] == payment_id
    assert (
        await client.post("/api/v1/payments", headers=headers, json={**BODY, "amount": "13"})
    ).status_code == 409


async def test_auth_and_validation(client):
    assert (await client.get("/openapi.json")).status_code == 401
    assert (await client.get("/docs")).status_code == 401
    assert (await client.get("/ready")).status_code == 401
    assert (await client.get("/ready", headers={"X-API-Key": KEY})).status_code == 200
    assert (
        await client.post("/api/v1/payments", headers={"X-API-Key": KEY}, json=BODY)
    ).status_code == 422
    assert (
        await client.post(
            "/api/v1/payments",
            headers={"X-API-Key": KEY, "Idempotency-Key": "x"},
            json={**BODY, "webhook_url": "https://other.example/hook"},
        )
    ).status_code == 422
    assert (
        await client.get(f"/api/v1/payments/{uuid4()}", headers={"X-API-Key": KEY})
    ).status_code == 404


async def test_concurrent_identical_requests_commit_one_payment_and_outbox(client):
    headers = {"X-API-Key": KEY, "Idempotency-Key": "concurrent"}
    responses = await asyncio.gather(
        *(client.post("/api/v1/payments", headers=headers, json=BODY) for _ in range(20))
    )
    assert all(response.status_code == 202 for response in responses)
    assert len({response.json()["payment_id"] for response in responses}) == 1
    engine = create_async_engine(URL)
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM payments"))).scalar_one() == 1
        assert (await conn.execute(text("SELECT count(*) FROM outbox"))).scalar_one() == 1
    await engine.dispose()


async def test_outbox_failure_rolls_back_payment(client, monkeypatch):
    from payment_service.repositories import OutboxRepository

    def fail_outbox(self, payment_id):
        raise RuntimeError("outbox failure")

    monkeypatch.setattr(OutboxRepository, "add_initial", fail_outbox)
    with pytest.raises(RuntimeError, match="outbox failure"):
        await client.post(
            "/api/v1/payments", headers={"X-API-Key": KEY, "Idempotency-Key": "rollback"}, json=BODY
        )
    engine = create_async_engine(URL)
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM payments"))).scalar_one() == 0
        assert (await conn.execute(text("SELECT count(*) FROM outbox"))).scalar_one() == 0
    await engine.dispose()


async def test_auth_precedes_malformed_json(client):
    for headers in ({}, {"X-API-Key": "wrong"}):
        response = await client.post(
            "/api/v1/payments", headers={**headers, "Content-Type": "application/json"}, content="{"
        )
        assert response.status_code == 401


@pytest.mark.parametrize("number", ["NaN", "Infinity", "-Infinity"])
async def test_nested_nonfinite_metadata_is_rejected(client, number):
    content = (
        '{"amount":"12.00","currency":"USD","description":"order",'
        '"metadata":{"nested":[{"number":' + number + "}]},"
        '"webhook_url":"https://example.com/hook"}'
    )
    response = await client.post(
        "/api/v1/payments",
        headers={"X-API-Key": KEY, "Idempotency-Key": "bad", "Content-Type": "application/json"},
        content=content,
    )
    assert response.status_code == 422


async def test_repeat_returns_current_terminal_status(client):
    headers = {"X-API-Key": KEY, "Idempotency-Key": "terminal"}
    first = await client.post("/api/v1/payments", headers=headers, json=BODY)
    engine = create_async_engine(URL)
    async with engine.begin() as conn:
        await conn.execute(
            text("UPDATE payments SET status = 'succeeded' WHERE idempotency_key = 'terminal'")
        )
    await engine.dispose()
    repeat = await client.post("/api/v1/payments", headers=headers, json=BODY)
    assert repeat.status_code == 202
    assert repeat.json()["payment_id"] == first.json()["payment_id"]
    assert repeat.json()["status"] == "succeeded"


async def test_conflicting_insert_wait_is_bounded(client):
    from sqlalchemy.exc import DBAPIError

    engine = create_async_engine(URL)
    async with engine.connect() as conn, conn.begin():
        await conn.execute(
            text("""INSERT INTO payments (id, idempotency_key, canonical_request,
                amount, currency, description, metadata, webhook_url)
                VALUES (:id, 'locked', '{}', 1, 'USD', 'x', '{}', 'https://example.com')"""),
            {"id": uuid4()},
        )
        with pytest.raises(DBAPIError, match="lock timeout|statement timeout"):
            await asyncio.wait_for(
                client.post(
                    "/api/v1/payments",
                    headers={"X-API-Key": KEY, "Idempotency-Key": "locked"},
                    json=BODY,
                ),
                timeout=2,
            )
    await engine.dispose()
