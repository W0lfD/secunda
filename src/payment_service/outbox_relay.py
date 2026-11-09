import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue
from faststream.rabbit.schemas import Channel
from pamqp.commands import Basic
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from payment_service.repositories import OutboxRepository

WORK_EXCHANGE = RabbitExchange("payments", durable=True)
DEAD_EXCHANGE = RabbitExchange("payments.dlx", durable=True)
WORK_QUEUE = RabbitQueue(
    "payments.new",
    durable=True,
    arguments={
        "x-dead-letter-exchange": "payments.dlx",
        "x-dead-letter-routing-key": "payments.dead",
    },
)
DEAD_QUEUE = RabbitQueue("payments.dlq", durable=True)


def create_broker(url: str, timeout: float = 10) -> RabbitBroker:
    return RabbitBroker(
        url,
        timeout=timeout,
        default_channel=Channel(publisher_confirms=True, on_return_raises=True),
    )


class Publisher:
    def __init__(self, url: str, timeout: float = 10, broker: RabbitBroker | None = None):
        self.timeout = timeout
        self.broker = broker if broker is not None else create_broker(url, timeout)

    async def start(self) -> None:
        async with asyncio.timeout(self.timeout):
            await self.broker.start()
            work_exchange = await self.broker.declare_exchange(WORK_EXCHANGE)
            dead_exchange = await self.broker.declare_exchange(DEAD_EXCHANGE)
            work_queue = await self.broker.declare_queue(WORK_QUEUE)
            dead_queue = await self.broker.declare_queue(DEAD_QUEUE)
            await work_queue.bind(work_exchange, routing_key="payments.new")
            await dead_queue.bind(dead_exchange, routing_key="payments.dead")

    async def stop(self) -> None:
        await self.broker.stop()

    async def publish(self, payload: dict[str, Any], kind: str, event_id: str) -> None:
        if kind == "work":
            exchange, routing_key = WORK_EXCHANGE, "payments.new"
        elif kind == "dead":
            exchange, routing_key = DEAD_EXCHANGE, "payments.dead"
        else:
            raise ValueError(f"unknown outbox kind: {kind}")
        async with asyncio.timeout(self.timeout):
            confirmation = await self.broker.publish(
                payload,
                exchange=exchange,
                routing_key=routing_key,
                mandatory=True,
                persist=True,
                message_id=event_id,
                timeout=self.timeout,
            )
            if not isinstance(confirmation, Basic.Ack):
                raise RuntimeError("publication was not confirmed")  # noqa: TRY004


async def relay_once(engine: AsyncEngine, publisher: Publisher) -> bool:
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as session, session.begin():
        row = await OutboxRepository(session).lock_ready()
        if row is None:
            return False
        try:
            await publisher.publish(row.payload, row.kind, str(row.id))
        except Exception as exc:  # noqa: BLE001 - only publication failures are retried
            row.publish_attempts += 1
            row.last_error = str(exc)
            row.available_at = datetime.now(UTC) + timedelta(
                seconds=min(30, 2 ** min(row.publish_attempts - 1, 5))
            )
        else:
            row.published_at = datetime.now(UTC)
            row.last_error = None
        return True
