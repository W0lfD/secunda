import asyncio
import json
import os

import aio_pika
import pytest
from sqlalchemy import text
from test_consumer import Callback, Gateway, make_consumer, seed, state
from test_consumer import engine as engine  # noqa: PLC0414 - pytest fixture re-export

from payment_service.outbox_relay import Publisher, create_broker
from payment_service.worker import serve

pytestmark = pytest.mark.integration
RABBIT = os.getenv("TEST_RABBITMQ_URL", "")


@pytest.fixture
async def broker_resources():
    if not RABBIT.rsplit("/", 1)[-1].endswith("_test"):
        pytest.skip("explicit isolated test RabbitMQ vhost required")
    setup = Publisher(RABBIT, 1)
    await setup.start()
    connection = await aio_pika.connect_robust(RABBIT)
    channel = await connection.channel()
    work, dead = await channel.get_queue("payments.new"), await channel.get_queue("payments.dlq")
    await work.purge()
    await dead.purge()
    yield channel, work, dead
    await connection.close()
    await setup.stop()


async def eventually(check):
    async with asyncio.timeout(3):
        while not await check():
            await asyncio.sleep(0.01)


async def test_real_subscriber_ack_and_poison_native_dlx(engine, broker_resources):
    channel, work, dead = broker_resources
    body = await seed(engine)
    callback = Callback()
    consumer, webhook = make_consumer(engine, Gateway(), callback)
    publisher = Publisher(RABBIT, 1, broker=create_broker(RABBIT, 1))
    stop = asyncio.Event()
    task = asyncio.create_task(serve(engine, publisher, consumer, stop))
    try:
        exchange = await channel.get_exchange("payments")
        await exchange.publish(
            aio_pika.Message(json.dumps(body).encode(), content_type="application/json"),
            "payments.new",
        )

        async def delivered():
            payment, _ = await state(engine)
            return payment.webhook_sent_at is not None

        await eventually(delivered)
        for payload in [b"{", b"{}", json.dumps({**body, "work_version": 9}).encode()]:
            await exchange.publish(
                aio_pika.Message(payload, content_type="application/json"), "payments.new"
            )
        received = []

        async def dead_received():
            message = await dead.get(fail=False)
            if message:
                received.append(message)
            return len(received) == 3

        await eventually(dead_received)
        for message in received:
            assert message.headers["x-death"][0]["reason"] == "rejected"
            await message.ack()
    finally:
        stop.set()
        await asyncio.wait_for(task, 3)
        await webhook.close()
    assert await work.get(fail=False) is None
    assert len(callback.payloads) == 1


async def test_database_failure_closes_worker_without_ack_then_redelivers(engine, broker_resources):
    channel, work, _ = broker_resources
    body = await seed(engine)
    callback = Callback([200, 200])
    consumer, webhook = make_consumer(engine, Gateway(), callback)
    async with engine.begin() as conn:
        await conn.execute(
            text("""CREATE FUNCTION test_worker_fail() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN IF NEW.webhook_sent_at IS NOT NULL THEN RAISE EXCEPTION 'worker commit failed'; END IF;
        RETURN NEW; END $$""")
        )
        await conn.execute(
            text(
                "CREATE TRIGGER test_worker_fail BEFORE UPDATE ON payments FOR EACH ROW EXECUTE FUNCTION test_worker_fail()"
            )
        )
    publisher = Publisher(RABBIT, 1, broker=create_broker(RABBIT, 1))
    task = asyncio.create_task(serve(engine, publisher, consumer, asyncio.Event()))
    try:
        exchange = await channel.get_exchange("payments")
        await exchange.publish(aio_pika.Message(json.dumps(body).encode()), "payments.new")
        with pytest.raises(Exception, match="worker commit failed"):
            await asyncio.wait_for(task, 3)
    finally:
        async with engine.begin() as conn:
            await conn.execute(text("DROP TRIGGER test_worker_fail ON payments"))
            await conn.execute(text("DROP FUNCTION test_worker_fail()"))
    message = await work.get(timeout=1)
    assert message.redelivered and json.loads(message.body) == body
    assert await consumer.process(message.body) == "ack"
    await message.ack()
    assert callback.payloads[0] == callback.payloads[1]
    await webhook.close()


async def test_relay_failure_is_supervised_and_closes_subscriber(
    engine, broker_resources, monkeypatch
):
    from payment_service import worker

    async def broken_relay(engine, publisher):
        raise RuntimeError("relay failed")

    monkeypatch.setattr(worker, "relay_once", broken_relay)
    consumer, webhook = make_consumer(engine, Gateway(), Callback())
    publisher = Publisher(RABBIT, 1, broker=create_broker(RABBIT, 1))
    try:
        with pytest.raises(RuntimeError, match="relay failed"):
            await asyncio.wait_for(serve(engine, publisher, consumer, asyncio.Event()), 3)
        _, work, _ = broker_resources
        queue = await work.declare()
        assert queue.consumer_count == 0
    finally:
        await webhook.close()


async def test_prefetch_blocks_second_delivery_while_first_holds_lock(engine, broker_resources):
    channel, _, dead = broker_resources
    body = await seed(engine)
    entered, release, poison_entered = asyncio.Event(), asyncio.Event(), asyncio.Event()

    class BlockingGateway:
        async def process(self, payment):
            from payment_service.schemas import PaymentStatus

            entered.set()
            await release.wait()
            return PaymentStatus.SUCCEEDED

    consumer, webhook = make_consumer(engine, BlockingGateway(), Callback())
    original_process = consumer.process

    async def observe_delivery(body):
        if body == b"{":
            poison_entered.set()
        return await original_process(body)

    consumer.process = observe_delivery
    publisher = Publisher(RABBIT, 1, broker=create_broker(RABBIT, 1))
    stop = asyncio.Event()
    task = asyncio.create_task(serve(engine, publisher, consumer, stop))
    try:
        exchange = await channel.get_exchange("payments")
        await exchange.publish(aio_pika.Message(json.dumps(body).encode()), "payments.new")
        await asyncio.wait_for(entered.wait(), 3)
        await exchange.publish(
            aio_pika.Message(b"{", content_type="application/json"), "payments.new"
        )
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(poison_entered.wait(), 0.1)
        assert await dead.get(fail=False) is None
        release.set()
        received = []

        async def rejected():
            message = await dead.get(fail=False)
            if message:
                received.append(message)
            return bool(received)

        await eventually(rejected)
        await received[0].ack()
    finally:
        release.set()
        stop.set()
        await asyncio.wait_for(task, 3)
        await webhook.close()
