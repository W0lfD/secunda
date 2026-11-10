import asyncio
import json
import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from payment_service.consumer import PaymentConsumer
from payment_service.models import Outbox, Payment
from payment_service.schemas import PaymentStatus
from payment_service.webhook import WebhookClient

pytestmark = pytest.mark.integration
DB = os.getenv("TEST_DATABASE_URL", "")


@pytest.fixture
async def engine():
    if not DB.rsplit("/", 1)[-1].endswith("_test"):
        pytest.skip("explicit isolated test database required")
    engine = create_async_engine(DB)
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE outbox, payments CASCADE"))
    yield engine
    await engine.dispose()


async def seed(engine):
    payment_id = uuid4()
    async with engine.begin() as conn:
        await conn.execute(
            text("""INSERT INTO payments
        (id,idempotency_key,canonical_request,amount,currency,description,metadata,webhook_url)
        VALUES (:id,:key,'{}',12.30,'USD','x','{}','https://example.com/callback')"""),
            {"id": payment_id, "key": str(payment_id)},
        )
    return {"event_id": str(uuid4()), "payment_id": str(payment_id), "work_version": 0}


async def state(engine):
    async with async_sessionmaker(engine)() as session:
        payment = (await session.scalars(select(Payment))).one()
        events = list((await session.scalars(select(Outbox).order_by(Outbox.work_version))).all())
        return payment, events


class Gateway:
    def __init__(self, result=PaymentStatus.SUCCEEDED):
        self.result = result
        self.calls = 0

    async def process(self, payment):
        self.calls += 1
        if isinstance(self.result, BaseException):
            raise self.result
        return self.result


class Callback:
    def __init__(self, statuses=(200,)):
        self.statuses = iter(statuses)
        self.payloads = []
        self.completed = None

    async def __call__(self, request):
        self.payloads.append(json.loads(request.content))
        result = next(self.statuses)
        self.completed = datetime.now(UTC)
        if isinstance(result, Exception):
            raise result
        return httpx.Response(result)


def make_consumer(engine, gateway, callback):
    webhook = WebhookClient(["https://example.com"], 0.1, transport=httpx.MockTransport(callback))
    return PaymentConsumer(engine, gateway, webhook), webhook


@pytest.mark.parametrize("result", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_terminal_result_committed_before_webhook_and_duplicates_ignored(engine, result):
    body = await seed(engine)
    gateway = Gateway(result)
    callback = Callback()

    async def check_committed(request):
        payment, _ = await state(engine)
        assert payment.status == result and payment.processed_at
        return await callback(request)

    consumer, webhook = make_consumer(engine, gateway, check_committed)
    try:
        assert await consumer.process(json.dumps(body).encode()) == "ack"
        assert await consumer.process(json.dumps(body).encode()) == "ack"
        payment, events = await state(engine)
        assert payment.status == result
        assert payment.webhook_sent_at and payment.work_version == 0
        assert not events and gateway.calls == 1
        assert len(callback.payloads) == 1
        payload = callback.payloads[0]
        assert payload["amount"] == "12.30" and payload["status"] == result
        assert payload["payment_id"] == body["payment_id"] and payload["event_id"]
    finally:
        await webhook.close()


@pytest.mark.parametrize("failure", [500, httpx.ReadTimeout("timeout")])
async def test_webhook_retry_preserves_outcome_and_notification_id(engine, failure):
    body = await seed(engine)
    gateway, callback = Gateway(), Callback([failure, 200])
    consumer, webhook = make_consumer(engine, gateway, callback)
    try:
        assert await consumer.process(json.dumps(body).encode()) == "ack"
        payment, events = await state(engine)
        assert payment.status == "succeeded" and payment.webhook_attempts == 1
        assert payment.work_version == 1 and not payment.webhook_sent_at
        assert len(events) == 1 and events[0].kind == "work"
        assert events[0].available_at >= callback.completed + timedelta(seconds=1)
        assert await consumer.process(json.dumps(events[0].payload).encode()) == "ack"
        payment, _ = await state(engine)
        assert payment.webhook_sent_at and gateway.calls == 1
        assert callback.payloads[0] == callback.payloads[1]
    finally:
        await webhook.close()


@pytest.mark.parametrize("stage", ["processing", "webhook"])
async def test_three_stage_failures_create_one_dead_continuation(engine, stage):
    body = await seed(engine)
    gateway = Gateway(
        RuntimeError("gateway failed") if stage == "processing" else PaymentStatus.SUCCEEDED
    )
    callback = Callback([500, 500, 500])
    consumer, webhook = make_consumer(engine, gateway, callback)
    try:
        for attempt in range(1, 4):
            started = datetime.now(UTC)
            assert await consumer.process(json.dumps(body).encode()) == "ack"
            payment, events = await state(engine)
            assert getattr(payment, stage + "_attempts") == attempt
            assert payment.work_version == attempt and len(events) == attempt
            assert events[-1].kind == ("dead" if attempt == 3 else "work")
            assert events[-1].available_at >= started + timedelta(
                seconds=0 if attempt == 3 else attempt
            )
            body = events[-1].payload
        assert await consumer.process(json.dumps(body).encode()) == "ack"
        payment, events = await state(engine)
        assert len(events) == 3
        assert payment.status == ("pending" if stage == "processing" else "succeeded")
        assert gateway.calls == (3 if stage == "processing" else 1)
    finally:
        await webhook.close()


@pytest.mark.parametrize("poison", [b"{", b"{}", b"[]"])
async def test_malformed_work_rejected(engine, poison):
    consumer, webhook = make_consumer(engine, Gateway(), Callback())
    try:
        assert await consumer.process(poison) == "reject"
    finally:
        await webhook.close()


async def test_unknown_future_and_stale_work(engine):
    body = await seed(engine)
    gateway = Gateway()
    consumer, webhook = make_consumer(engine, gateway, Callback())
    try:
        assert (
            await consumer.process(json.dumps({**body, "payment_id": str(uuid4())}).encode())
            == "reject"
        )
        assert await consumer.process(json.dumps({**body, "work_version": 1}).encode()) == "reject"
        async with engine.begin() as conn:
            await conn.execute(text("UPDATE payments SET work_version=1"))
        assert await consumer.process(json.dumps(body).encode()) == "ack"
        assert gateway.calls == 0
    finally:
        await webhook.close()


async def test_callback_accepted_then_commit_failure_replays_same_notification(engine):
    body = await seed(engine)
    callback, gateway = Callback([200, 200]), Gateway()
    consumer, webhook = make_consumer(engine, gateway, callback)
    async with engine.begin() as conn:
        await conn.execute(
            text("""CREATE FUNCTION test_fail_delivery() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN IF NEW.webhook_sent_at IS NOT NULL THEN RAISE EXCEPTION 'delivery commit failed'; END IF;
        RETURN NEW; END $$""")
        )
        await conn.execute(
            text(
                "CREATE TRIGGER test_fail_delivery BEFORE UPDATE ON payments FOR EACH ROW EXECUTE FUNCTION test_fail_delivery()"
            )
        )
    try:
        with pytest.raises(Exception, match="delivery commit failed"):
            await consumer.process(json.dumps(body).encode())
        payment, events = await state(engine)
        assert payment.status == "succeeded" and payment.webhook_attempts == 0
        assert payment.work_version == 0 and not events and not payment.webhook_sent_at
    finally:
        async with engine.begin() as conn:
            await conn.execute(text("DROP TRIGGER test_fail_delivery ON payments"))
            await conn.execute(text("DROP FUNCTION test_fail_delivery()"))
    try:
        assert await consumer.process(json.dumps(body).encode()) == "ack"
        assert callback.payloads[0] == callback.payloads[1] and gateway.calls == 1
    finally:
        await webhook.close()


async def test_cancellation_does_not_consume_attempt(engine):
    body = await seed(engine)
    consumer, webhook = make_consumer(engine, Gateway(asyncio.CancelledError()), Callback())
    try:
        with pytest.raises(asyncio.CancelledError):
            await consumer.process(json.dumps(body).encode())
        payment, events = await state(engine)
        assert payment.status == "pending" and payment.processing_attempts == 0
        assert payment.work_version == 0 and not events
    finally:
        await webhook.close()


async def test_gateway_total_timeout_becomes_processing_retry(engine):
    body = await seed(engine)

    class SlowGateway:
        async def process(self, payment):
            await asyncio.Event().wait()

    consumer, webhook = make_consumer(engine, SlowGateway(), Callback())
    consumer.gateway_timeout = 0.01
    try:
        assert await asyncio.wait_for(consumer.process(json.dumps(body).encode()), 1) == "ack"
        payment, events = await state(engine)
        assert payment.processing_attempts == 1 and payment.work_version == 1
        assert payment.status == "pending" and len(events) == 1
    finally:
        await webhook.close()


async def test_processing_and_webhook_have_separate_attempt_budgets(engine):
    body = await seed(engine)
    gateway, callback = Gateway(RuntimeError("processing outage")), Callback([500, 200])
    consumer, webhook = make_consumer(engine, gateway, callback)
    try:
        for _ in range(2):
            assert await consumer.process(json.dumps(body).encode()) == "ack"
            _, events = await state(engine)
            body = events[-1].payload
        gateway.result = PaymentStatus.FAILED
        assert await consumer.process(json.dumps(body).encode()) == "ack"
        payment, events = await state(engine)
        assert payment.processing_attempts == 2 and payment.webhook_attempts == 1
        assert payment.status == "failed" and payment.work_version == 3
        assert events[-1].kind == "work"
        assert await consumer.process(json.dumps(events[-1].payload).encode()) == "ack"
        payment, _ = await state(engine)
        assert payment.webhook_sent_at and payment.status == "failed"
    finally:
        await webhook.close()


async def test_failed_retry_commit_rolls_back_counter_and_continuation(engine):
    body = await seed(engine)
    consumer, webhook = make_consumer(
        engine, Gateway(RuntimeError("integration error")), Callback()
    )
    async with engine.begin() as conn:
        await conn.execute(
            text("""CREATE FUNCTION test_fail_retry() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN RAISE EXCEPTION 'retry commit failed'; END $$""")
        )
        await conn.execute(
            text(
                "CREATE TRIGGER test_fail_retry BEFORE INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION test_fail_retry()"
            )
        )
    try:
        with pytest.raises(Exception, match="retry commit failed"):
            await consumer.process(json.dumps(body).encode())
        payment, events = await state(engine)
        assert payment.processing_attempts == 0 and payment.work_version == 0 and not events
    finally:
        async with engine.begin() as conn:
            await conn.execute(text("DROP TRIGGER test_fail_retry ON outbox"))
            await conn.execute(text("DROP FUNCTION test_fail_retry()"))
        await webhook.close()


async def test_nonterminal_gateway_response_is_retryable_error(engine):
    body = await seed(engine)
    consumer, webhook = make_consumer(engine, Gateway(PaymentStatus.PENDING), Callback())
    try:
        assert await consumer.process(json.dumps(body).encode()) == "ack"
        payment, events = await state(engine)
        assert payment.status == "pending" and payment.processed_at is None
        assert payment.processing_attempts == 1 and len(events) == 1
    finally:
        await webhook.close()


async def test_restart_after_terminal_commit_resumes_webhook(engine):
    body = await seed(engine)
    entered = asyncio.Event()
    gateway = Gateway()

    async def interrupted(request):
        entered.set()
        await asyncio.Event().wait()

    first, first_webhook = make_consumer(engine, gateway, interrupted)
    task = asyncio.create_task(first.process(json.dumps(body).encode()))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        payment, events = await state(engine)
        assert payment.status == "succeeded" and not payment.webhook_sent_at and not events
        callback = Callback()
        resumed, resumed_webhook = make_consumer(engine, gateway, callback)
        try:
            assert await resumed.process(json.dumps(body).encode()) == "ack"
            payment, _ = await state(engine)
            assert payment.webhook_sent_at and gateway.calls == 1
            assert len(callback.payloads) == 1
        finally:
            await resumed_webhook.close()
    finally:
        await first_webhook.close()


async def test_competing_consumers_and_lost_retry_ack_create_one_continuation(engine):
    body = await seed(engine)
    gateway, callback = Gateway(), Callback([500, 200])
    first, first_webhook = make_consumer(engine, gateway, callback)
    second, second_webhook = make_consumer(engine, gateway, callback)
    try:
        encoded = json.dumps(body).encode()
        assert await asyncio.gather(first.process(encoded), second.process(encoded)) == [
            "ack",
            "ack",
        ]
        payment, events = await state(engine)
        assert payment.status == "succeeded" and payment.webhook_attempts == 1
        assert gateway.calls == 1 and len(callback.payloads) == 1
        assert len(events) == 1 and events[0].work_version == 1
        # Redelivery of the original after the retry commit is stale.
        assert await second.process(encoded) == "ack"
        _, events = await state(engine)
        assert len(events) == 1 and len(callback.payloads) == 1
        retry = json.dumps(events[0].payload).encode()
        assert await asyncio.gather(first.process(retry), second.process(retry)) == ["ack", "ack"]
        payment, events = await state(engine)
        assert payment.webhook_sent_at and len(events) == 1
        assert gateway.calls == 1 and len(callback.payloads) == 2
    finally:
        await first_webhook.close()
        await second_webhook.close()
