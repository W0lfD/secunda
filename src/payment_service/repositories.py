from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.models import Outbox, Payment
from payment_service.schemas import PaymentCreate


class PaymentRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create(self, key: str, data: PaymentCreate) -> Payment | None:
        statement = (
            insert(Payment)
            .values(
                id=uuid4(),
                idempotency_key=key,
                canonical_request=data.canonical_json(),
                amount=data.amount,
                currency=data.currency.value,
                description=data.description,
                payment_metadata=data.metadata,
                webhook_url=str(data.webhook_url),
            )
            .on_conflict_do_nothing(index_elements=["idempotency_key"])
            .returning(Payment)
        )
        return (await self.session.execute(statement)).scalar_one_or_none()

    async def get_by_key(self, key: str) -> Payment:
        return (
            await self.session.execute(select(Payment).where(Payment.idempotency_key == key))
        ).scalar_one()

    async def lock(self, payment_id: UUID) -> Payment | None:
        statement = select(Payment).where(Payment.id == payment_id).with_for_update()
        return (await self.session.execute(statement)).scalar_one_or_none()

    async def get(self, payment_id: UUID) -> Payment | None:
        return await self.session.get(Payment, payment_id)


class OutboxRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    def add_initial(self, payment_id: UUID) -> None:
        event_id = uuid4()
        self.session.add(
            Outbox(
                id=event_id,
                payment_id=payment_id,
                work_version=0,
                payload={
                    "event_id": str(event_id),
                    "payment_id": str(payment_id),
                    "work_version": 0,
                },
            )
        )

    async def lock_ready(self) -> Outbox | None:
        from sqlalchemy import func

        statement = (
            select(Outbox)
            .where(Outbox.published_at.is_(None), Outbox.available_at <= func.now())
            .order_by(Outbox.available_at, Outbox.id)
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        return (await self.session.execute(statement)).scalar_one_or_none()
