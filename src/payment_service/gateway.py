import asyncio
import random

from payment_service.models import Payment
from payment_service.schemas import PaymentStatus


class Gateway:
    async def process(self, payment: Payment) -> PaymentStatus:
        await asyncio.sleep(random.uniform(2, 5))
        return PaymentStatus.SUCCEEDED if random.random() < 0.9 else PaymentStatus.FAILED

    async def close(self) -> None:
        pass
