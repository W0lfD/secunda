import json
import os
from datetime import timedelta
from uuid import uuid4

import aio_pika
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from payment_service.outbox_relay import Publisher, create_broker, relay_once

pytestmark = pytest.mark.integration
DB = os.getenv("TEST_DATABASE_URL", "")
RABBIT = os.getenv("TEST_RABBITMQ_URL", "")


@pytest.fixture
async def resources():
    if not DB.rsplit("/", 1)[-1].endswith("_test") or not RABBIT.rsplit("/", 1)[-1].endswith(
        "_test"
    ):
        pytest.skip("explicit isolated test database and RabbitMQ vhost required")
    engine = create_async_engine(DB)
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE outbox, payments CASCADE"))
    publisher = Publisher(RABBIT, 1)
    await publisher.start()
    connection = await aio_pika.connect_robust(RABBIT)
    channel = await connection.channel()
    for name in ("payments.new", "payments.dlq"):
        queue = await channel.get_queue(name)
        await queue.purge()
    yield engine, publisher, channel
    await connection.close()
    await publisher.stop()
    await engine.dispose()


async def insert_event(engine, kind="work", delay=timedelta()):
    payment_id, event_id = uuid4(), uuid4()
    payload = {"event_id": str(event_id), "payment_id": str(payment_id), "work_version": 0}
    async with engine.begin() as conn:
        await conn.execute(
            text("""INSERT INTO payments (id,idempotency_key,canonical_request,amount,currency,description,metadata,webhook_url)
        VALUES (:id,:key,'{}',1,'USD','x','{}','https://example.com')"""),
            {"id": payment_id, "key": str(payment_id)},
        )
        await conn.execute(
            text("""INSERT INTO outbox (id,payment_id,work_version,kind,payload,available_at)
        VALUES (:id,:payment_id,0,:kind,CAST(:payload AS jsonb),now()+CAST(:delay AS interval))"""),
            {
                "id": event_id,
                "payment_id": payment_id,
                "kind": kind,
                "payload": json.dumps(payload),
                "delay": delay,
            },
        )
    return event_id, payload


@pytest.mark.parametrize(
    ("kind", "queue_name"), [("work", "payments.new"), ("dead", "payments.dlq")]
)
async def test_relay_routes_persistent_envelope(resources, kind, queue_name):
    engine, publisher, channel = resources
    event_id, payload = await insert_event(engine, kind)
    assert await relay_once(engine, publisher)
    queue = await channel.get_queue(queue_name)
    message = await queue.get(timeout=1)
    assert json.loads(message.body) == payload
    assert message.message_id == str(event_id)
    assert message.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
    await message.ack()
    async with engine.connect() as conn:
        assert (
            await conn.execute(
                text("SELECT published_at IS NOT NULL FROM outbox WHERE id=:id"), {"id": event_id}
            )
        ).scalar_one()


async def test_due_row_is_processed():
    if not DB.rsplit("/", 1)[-1].endswith("_test"):
        pytest.skip("explicit test database required")
    engine = create_async_engine(DB)
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE outbox, payments CASCADE"))
    await insert_event(engine)
    assert await relay_once(engine, Publisher("unused"))
    await engine.dispose()


async def test_unroutable_mandatory_publish_raises(resources):
    from aio_pika.exceptions import DeliveryError

    _, publisher, channel = resources
    queue = await channel.get_queue("payments.new")
    await queue.unbind("payments", routing_key="payments.new")
    try:
        with pytest.raises(DeliveryError):
            await publisher.publish({"event_id": "1"}, "work", "1")
    finally:
        await queue.bind("payments", routing_key="payments.new")


async def test_failure_is_rescheduled_and_recovers(resources):
    engine, publisher, channel = resources
    event_id, payload = await insert_event(engine)
    offline = Publisher("amqp://payments:payments@127.0.0.1:1/payments_test", 0.2)
    with pytest.raises(OSError):
        await offline.start()
    assert await relay_once(engine, offline)
    await offline.stop()
    async with engine.begin() as conn:
        row = (
            await conn.execute(
                text(
                    "SELECT publish_attempts,last_error,published_at,available_at > now() FROM outbox WHERE id=:id"
                ),
                {"id": event_id},
            )
        ).one()
        assert row[0] == 1 and row[1] and row[2] is None and row[3]
        await conn.execute(
            text("UPDATE outbox SET available_at=now()-interval '1 second' WHERE id=:id"),
            {"id": event_id},
        )
    assert await relay_once(engine, publisher)
    message = await (await channel.get_queue("payments.new")).get(timeout=1)
    assert json.loads(message.body) == payload
    await message.ack()


async def test_future_row_is_not_published(resources):
    engine, publisher, channel = resources
    await insert_event(engine, delay=timedelta(hours=1))
    assert not await relay_once(engine, publisher)
    assert await (await channel.get_queue("payments.new")).get(timeout=0.1, fail=False) is None


async def test_confirm_then_database_failure_replays_same_event(resources):
    engine, publisher, channel = resources
    event_id, payload = await insert_event(engine)
    async with engine.begin() as conn:
        await conn.execute(
            text("""CREATE OR REPLACE FUNCTION test_reject_mark() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN RAISE EXCEPTION 'mark failed'; END $$""")
        )
        await conn.execute(
            text(
                "CREATE TRIGGER test_reject_mark BEFORE UPDATE ON outbox FOR EACH ROW EXECUTE FUNCTION test_reject_mark()"
            )
        )
    try:
        with pytest.raises(Exception, match="mark failed"):
            await relay_once(engine, publisher)
    finally:
        async with engine.begin() as conn:
            await conn.execute(text("DROP TRIGGER test_reject_mark ON outbox"))
            await conn.execute(text("DROP FUNCTION test_reject_mark()"))
    assert await relay_once(engine, publisher)
    queue = await channel.get_queue("payments.new")
    for _ in range(2):
        message = await queue.get(timeout=1)
        assert message.message_id == str(event_id)
        assert json.loads(message.body) == payload
        await message.ack()


async def test_concurrent_relays_skip_locked_row(resources):
    import asyncio

    engine, publisher, channel = resources
    await insert_event(engine)
    entered, release = asyncio.Event(), asyncio.Event()

    class BlockingPublisher:
        async def publish(self, payload, kind, event_id):
            entered.set()
            await release.wait()
            await publisher.publish(payload, kind, event_id)

    task = asyncio.create_task(relay_once(engine, BlockingPublisher()))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        assert not await asyncio.wait_for(relay_once(engine, publisher), 1)
    finally:
        release.set()
        assert await task
    queue = await channel.get_queue("payments.new")
    message = await queue.get(timeout=1)
    await message.ack()
    assert await queue.get(timeout=0.1, fail=False) is None


async def test_publisher_uses_supplied_broker(resources):
    _, _, channel = resources
    broker = create_broker(RABBIT, 1)
    publisher = Publisher(RABBIT, 1, broker=broker)
    assert publisher.broker is broker
    await publisher.start()
    try:
        await publisher.publish({"event_id": "shared"}, "work", "shared")
        message = await (await channel.get_queue("payments.new")).get(timeout=1)
        assert message.message_id == "shared"
        await message.ack()
    finally:
        await publisher.stop()
