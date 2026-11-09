from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.models import Payment
from payment_service.repositories import OutboxRepository, PaymentRepository
from payment_service.schemas import PaymentCreate


class IdempotencyConflict(Exception):
    pass


class PaymentService:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.payments = PaymentRepository(session)
        self.outbox = OutboxRepository(session)

    async def create(self, key: str, data: PaymentCreate) -> Payment:
        async with self.session.begin():
            payment = await self.payments.create(key, data)
            if payment is None:
                payment = await self.payments.get_by_key(key)
                if payment.canonical_request != data.canonical_json():
                    raise IdempotencyConflict
            else:
                self.outbox.add_initial(payment.id)
        return payment

    async def get(self, payment_id: UUID) -> Payment | None:
        return await self.payments.get(payment_id)
