import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import Literal
from uuid import NAMESPACE_URL, uuid4, uuid5

from pydantic import ValidationError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from payment_service.gateway import Gateway
from payment_service.models import Outbox, Payment
from payment_service.repositories import PaymentRepository
from payment_service.schemas import Currency, PaymentStatus, WebhookNotification, WorkEnvelope
from payment_service.webhook import WebhookClient

logger = logging.getLogger(__name__)
Disposition = Literal["ack", "reject"]


class PaymentConsumer:
    def __init__(
        self,
        engine: AsyncEngine,
        gateway: Gateway,
        webhook: WebhookClient,
        gateway_timeout: float = 10,
    ):
        self.sessions = async_sessionmaker(engine, expire_on_commit=False)
        self.gateway = gateway
        self.webhook = webhook
        self.gateway_timeout = gateway_timeout

    @staticmethod
    def disposition(payment: Payment | None, envelope: WorkEnvelope) -> Disposition | None:
        if payment is None or envelope.work_version > payment.work_version:
            return "reject"
        if (
            envelope.work_version < payment.work_version
            or payment.webhook_sent_at
            or payment.processing_attempts >= 3
            or payment.webhook_attempts >= 3
        ):
            return "ack"
        return None

    @staticmethod
    def fail(session: AsyncSession, payment: Payment, stage: str, error: Exception) -> None:
        if stage == "processing":
            payment.processing_attempts += 1
            attempts = payment.processing_attempts
        else:
            payment.webhook_attempts += 1
            attempts = payment.webhook_attempts
        payment.work_version += 1
        # Exception text may contain URLs, credentials or response bodies.
        payment.last_error = type(error).__name__
        event_id = uuid4()
        envelope = WorkEnvelope(
            event_id=event_id, payment_id=payment.id, work_version=payment.work_version
        )
        session.add(
            Outbox(
                id=event_id,
                payment_id=payment.id,
                work_version=payment.work_version,
                kind="dead" if attempts >= 3 else "work",
                payload=envelope.model_dump(mode="json"),
                available_at=datetime.now(UTC)
                + timedelta(seconds=0 if attempts >= 3 else attempts),
            )
        )
        logger.warning(
            "payment_stage_failed payment_id=%s event_id=%s stage=%s attempt=%s error=%s",
            payment.id,
            event_id,
            stage,
            attempts,
            type(error).__name__,
        )

    async def process(self, body: bytes) -> Disposition:
        try:
            envelope = WorkEnvelope.model_validate_json(body)
        except ValidationError:
            logger.warning("payment_work_rejected reason=malformed")
            return "reject"
        # Commit the gateway result independently, retaining the original work version.
        async with self.sessions() as session, session.begin():
            payment = await PaymentRepository(session).lock(envelope.payment_id)
            disposition = self.disposition(payment, envelope)
            if disposition is not None:
                return disposition
            assert payment is not None
            if payment.status == "pending":
                try:
                    async with asyncio.timeout(self.gateway_timeout):
                        result = await self.gateway.process(payment)
                        if result not in (PaymentStatus.SUCCEEDED, PaymentStatus.FAILED):
                            raise ValueError("gateway returned a nonterminal outcome")
                except SQLAlchemyError:
                    raise
                except Exception as exc:  # noqa: BLE001 - integration errors consume a stage attempt
                    self.fail(session, payment, "processing", exc)
                    return "ack"
                payment.status = result.value
                payment.processed_at = datetime.now(UTC)
                payment.last_error = None
        # A fresh session avoids stale ORM state if another delivery wins this lock.
        async with self.sessions() as session, session.begin():
            payment = await PaymentRepository(session).lock(envelope.payment_id)
            disposition = self.disposition(payment, envelope)
            if disposition is not None:
                return disposition
            assert payment is not None and payment.processed_at is not None
            notification = WebhookNotification(
                event_id=uuid5(NAMESPACE_URL, f"payment-notification:{payment.id}"),
                payment_id=payment.id,
                amount=payment.amount,
                currency=Currency(payment.currency),
                status=PaymentStatus(payment.status),
                processed_at=payment.processed_at,
            )
            try:
                await self.webhook.send(payment.webhook_url, notification)
            except SQLAlchemyError:
                raise
            except Exception as exc:  # noqa: BLE001 - integration errors consume a stage attempt
                self.fail(session, payment, "webhook", exc)
            else:
                payment.webhook_sent_at = datetime.now(UTC)
                payment.last_error = None
        logger.info(
            "payment_work_committed payment_id=%s event_id=%s work_version=%s",
            envelope.payment_id,
            envelope.event_id,
            envelope.work_version,
        )
        return "ack"
