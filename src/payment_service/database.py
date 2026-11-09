from math import ceil

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from payment_service.config import Settings


def create_engine(settings: Settings) -> AsyncEngine:
    timeout = settings.database_timeout_seconds
    milliseconds = str(max(1, ceil(timeout * 1000)))
    return create_async_engine(
        settings.database_url,
        pool_timeout=timeout,
        connect_args={
            "timeout": timeout,
            "server_settings": {"statement_timeout": milliseconds, "lock_timeout": milliseconds},
        },
    )
