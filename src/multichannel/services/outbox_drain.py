"""Background drain for outbound provider dispatch rows."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import joinedload

from multichannel.models.outbox_item import OutboxItem, OutboxItemState

logger = structlog.get_logger("multichannel.outbox_drain")

MAX_ATTEMPTS = 8
MAX_BACKOFF_SECONDS = 60 * 60


class OutboxDrain:
    def __init__(
        self,
        session_maker: async_sessionmaker[AsyncSession],
        interval_seconds: float = 1.0,
        batch_size: int = 50,
    ) -> None:
        self._session_maker = session_maker
        self._interval_seconds = interval_seconds
        self._batch_size = batch_size
        self._stop_event = asyncio.Event()

    def stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        while not self._stop_event.is_set():
            await self._drain_once()
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self._interval_seconds,
                )
            except TimeoutError:
                pass

    async def _drain_once(self) -> None:
        async with self._session_maker() as session:
            async with session.begin():
                rows = await self._claim_batch(session)
                for row in rows:
                    row.state = OutboxItemState.CLAIMED
                    try:
                        message = row.message
                        # TODO(sprint12): from multichannel.providers.dispatch import dispatch_message
                        # TODO(sprint12): await dispatch_message(message)
                        _ = message
                    except Exception as exc:
                        await self._mark_retry(row, exc)
                    else:
                        row.state = OutboxItemState.DONE
                        row.last_error = None

    async def _claim_batch(self, session: AsyncSession) -> list[OutboxItem]:
        now = datetime.now(tz=UTC)
        result = await session.scalars(
            select(OutboxItem)
            .options(joinedload(OutboxItem.message))
            .where(
                OutboxItem.state == OutboxItemState.PENDING,
                OutboxItem.next_attempt_at <= now,
            )
            .order_by(OutboxItem.next_attempt_at, OutboxItem.created_at)
            .limit(self._batch_size)
            .with_for_update(skip_locked=True)
        )
        return list(result)

    async def _mark_retry(self, row: OutboxItem, exc: Exception) -> None:
        row.attempts += 1
        row.last_error = str(exc)
        if row.attempts >= MAX_ATTEMPTS:
            row.state = OutboxItemState.DEAD
            logger.exception(
                "outbox_item_dead",
                outbox_item_id=str(row.id),
                attempts=row.attempts,
            )
            return

        backoff_seconds = min(2**row.attempts, MAX_BACKOFF_SECONDS)
        row.next_attempt_at = datetime.now(tz=UTC) + timedelta(seconds=backoff_seconds)
        row.state = OutboxItemState.PENDING
        logger.exception(
            "outbox_item_dispatch_failed",
            outbox_item_id=str(row.id),
            attempts=row.attempts,
            next_attempt_at=row.next_attempt_at.isoformat(),
        )
