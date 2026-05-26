"""Background drain for CloudEvent outbox rows."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from multichannel.models.event_outbox import EventOutbox, EventOutboxState
from multichannel.schemas.cloudevent import CloudEvent
from multichannel.services.redis_publisher import RedisStreamPublisher

logger = structlog.get_logger("multichannel.event_outbox_drain")


class EventOutboxDrain:
    def __init__(
        self,
        session_maker: async_sessionmaker[AsyncSession],
        publisher: RedisStreamPublisher,
        interval_seconds: float = 1.0,
        batch_size: int = 50,
    ) -> None:
        self._session_maker = session_maker
        self._publisher = publisher
        self._interval_seconds = interval_seconds
        self._batch_size = batch_size
        self._stop_event = asyncio.Event()

    def stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        await self._publisher.connect()
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
                    row.state = EventOutboxState.CLAIMED
                    try:
                        event = CloudEvent.model_validate(row.event_data)
                        await self._publisher.publish(event)
                    except Exception as exc:
                        row.state = EventOutboxState.PENDING
                        logger.exception(
                            "event_outbox_publish_failed",
                            event_outbox_id=str(row.id),
                            error=str(exc),
                        )
                    else:
                        row.state = EventOutboxState.DONE
                        row.published_at = datetime.now(tz=UTC)

    async def _claim_batch(self, session: AsyncSession) -> list[EventOutbox]:
        result = await session.scalars(
            select(EventOutbox)
            .where(EventOutbox.state == EventOutboxState.PENDING)
            .order_by(EventOutbox.created_at)
            .limit(self._batch_size)
            .with_for_update(skip_locked=True)
        )
        return list(result)
