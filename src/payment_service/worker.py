import asyncio
import logging
import signal
from contextlib import AsyncExitStack

from faststream import AckPolicy
from faststream.rabbit import RabbitMessage
from faststream.rabbit.schemas import Channel
from sqlalchemy.ext.asyncio import AsyncEngine

from payment_service.config import Settings
from payment_service.consumer import PaymentConsumer
from payment_service.database import create_engine
from payment_service.gateway import Gateway
from payment_service.logging import configure_logging
from payment_service.outbox_relay import (
    WORK_EXCHANGE,
    WORK_QUEUE,
    Publisher,
    create_broker,
    relay_once,
)
from payment_service.webhook import WebhookClient

logger = logging.getLogger(__name__)


async def relay_loop(engine: AsyncEngine, publisher: Publisher) -> None:
    while True:
        if not await relay_once(engine, publisher):
            await asyncio.sleep(0.2)


async def serve(
    engine: AsyncEngine,
    publisher: Publisher,
    consumer: PaymentConsumer,
    stop: asyncio.Event,
) -> None:
    """One connection lifetime. Failures leave delivery unacked until connection closes."""
    failed: asyncio.Future[None] = asyncio.get_running_loop().create_future()

    @publisher.broker.subscriber(
        WORK_QUEUE,
        WORK_EXCHANGE,
        channel=Channel(prefetch_count=1),
        ack_policy=AckPolicy.MANUAL,
        decoder=lambda message: message.body,
    )
    async def consume(message: RabbitMessage) -> None:
        try:
            result = await consumer.process(message.body)
            if result == "reject":
                await message.reject(requeue=False)
            else:
                await message.ack()
        except Exception as exc:  # noqa: BLE001 - supervisor owns recovery
            # The supervisor closes the channel and backs off; no immediate nack loop.
            if not failed.done():
                failed.set_exception(exc)

    tasks: list[asyncio.Task[None] | asyncio.Task[bool]] = []
    try:
        await publisher.start()
        tasks = [
            asyncio.create_task(relay_loop(engine, publisher)),
            asyncio.create_task(stop.wait()),
        ]
        done, _ = await asyncio.wait([*tasks, failed], return_when=asyncio.FIRST_COMPLETED)
        for completed in done:
            completed.result()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        # Retrieve a simultaneous subscriber error even if another task failed first.
        if failed.done():
            failed.exception()
        else:
            failed.cancel()
        await publisher.stop()


async def run_worker(settings: Settings, stop: asyncio.Event | None = None) -> None:
    stop = stop if stop is not None else asyncio.Event()
    loop = asyncio.get_running_loop()
    async with AsyncExitStack() as resources:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
            resources.callback(loop.remove_signal_handler, sig)
        engine = create_engine(settings)
        resources.push_async_callback(engine.dispose)
        gateway = Gateway()
        resources.push_async_callback(gateway.close)
        webhook = WebhookClient(settings.webhook_allowed_origins, settings.webhook_timeout_seconds)
        resources.push_async_callback(webhook.close)
        consumer = PaymentConsumer(engine, gateway, webhook, settings.gateway_timeout_seconds)
        delay = 1
        while not stop.is_set():
            publisher = Publisher(
                settings.rabbitmq_url,
                settings.broker_timeout_seconds,
                broker=create_broker(settings.rabbitmq_url, settings.broker_timeout_seconds),
            )
            try:
                await serve(engine, publisher, consumer, stop)
            except Exception as exc:  # noqa: BLE001 - supervisor owns recovery
                logger.error(
                    "worker_restart error=%s backoff_seconds=%s", type(exc).__name__, delay
                )
                try:
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                except TimeoutError:
                    pass
                delay = min(30, delay * 2)


def main() -> None:
    configure_logging()
    asyncio.run(run_worker(Settings()))  # type: ignore[call-arg]  # Settings reads environment


if __name__ == "__main__":
    main()
