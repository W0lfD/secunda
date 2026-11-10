import asyncio

import pytest

from payment_service import worker
from payment_service.config import Settings

pytestmark = pytest.mark.unit


async def test_worker_reconnect_backoff_and_resources_closed(monkeypatch):
    stop = asyncio.Event()
    attempts, delays, closed = [], [], []

    class Engine:
        async def dispose(self):
            closed.append("database")

    class Gateway:
        async def close(self):
            closed.append("gateway")

    class Webhook:
        def __init__(self, *args):
            pass

        async def close(self):
            closed.append("http")

    async def fail_serve(engine, publisher, consumer, stop):
        attempts.append(publisher)
        raise OSError("offline")

    async def fake_wait_for(coro, timeout):
        coro.close()
        delays.append(timeout)
        if len(delays) == 7:
            stop.set()
        raise TimeoutError

    monkeypatch.setattr(worker, "create_engine", lambda settings: Engine())
    monkeypatch.setattr(worker, "Gateway", Gateway)
    monkeypatch.setattr(worker, "WebhookClient", Webhook)
    monkeypatch.setattr(worker, "serve", fail_serve)
    monkeypatch.setattr(worker.asyncio, "wait_for", fake_wait_for)
    settings = Settings(api_key="x" * 32, webhook_allowed_origins=["https://example.com"])
    await worker.run_worker(settings, stop)
    assert delays == [1, 2, 4, 8, 16, 30, 30]
    assert len({id(publisher) for publisher in attempts}) == 7
    assert set(closed) == {"database", "gateway", "http"}


async def test_cleanup_failure_still_disposes_other_resources(monkeypatch):
    stop = asyncio.Event()
    stop.set()
    closed = []

    class Engine:
        async def dispose(self):
            closed.append("database")

    class Gateway:
        async def close(self):
            closed.append("gateway")

    class Webhook:
        def __init__(self, *args):
            pass

        async def close(self):
            raise RuntimeError("http close failed")

    monkeypatch.setattr(worker, "create_engine", lambda settings: Engine())
    monkeypatch.setattr(worker, "Gateway", Gateway)
    monkeypatch.setattr(worker, "WebhookClient", Webhook)
    settings = Settings(api_key="x" * 32, webhook_allowed_origins=["https://example.com"])
    with pytest.raises(RuntimeError, match="http close failed"):
        await worker.run_worker(settings, stop)
    assert set(closed) == {"database", "gateway"}
