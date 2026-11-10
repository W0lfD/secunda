import asyncio
from urllib.parse import urlsplit

import httpx
from pydantic import TypeAdapter

from payment_service.config import normalize_origin
from payment_service.schemas import WebhookNotification, WebhookUrl


class WebhookClient:
    def __init__(
        self,
        origins: list[str],
        timeout: float,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ):
        self.origins = {normalize_origin(origin) for origin in origins}
        self.timeout = timeout
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout),
            follow_redirects=False,
            trust_env=False,
            transport=transport,
        )

    async def send(self, url: str, notification: WebhookNotification) -> None:
        validated = TypeAdapter(WebhookUrl).validate_python(url)
        parsed = urlsplit(str(validated))
        if normalize_origin(f"{parsed.scheme}://{parsed.netloc}") not in self.origins:
            raise ValueError("webhook origin is not allowed")
        async with asyncio.timeout(self.timeout):
            response = await self.client.post(
                str(validated), json=notification.model_dump(mode="json")
            )
            response.raise_for_status()

    async def close(self) -> None:
        await self.client.aclose()
