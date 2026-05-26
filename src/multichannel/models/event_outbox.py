"""Durable event outbox rows for downstream fan-out."""

from __future__ import annotations

import enum
import uuid
from datetime import datetime

from sqlalchemy import JSON, DateTime, Enum, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from multichannel.models.base import Base, TimestampMixin
from multichannel.models.message import enum_values


class EventOutboxState(str, enum.Enum):
    PENDING = "pending"
    CLAIMED = "claimed"
    DONE = "done"
    DEAD = "dead"


class EventOutbox(Base, TimestampMixin):
    __tablename__ = "event_outbox"

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    event_type: Mapped[str] = mapped_column(String(128), nullable=False)
    event_data: Mapped[dict] = mapped_column(JSON, nullable=False)
    state: Mapped[EventOutboxState] = mapped_column(
        Enum(
            EventOutboxState,
            native_enum=False,
            length=16,
            values_callable=enum_values,
        ),
        nullable=False,
        default=EventOutboxState.PENDING,
        server_default=EventOutboxState.PENDING.value,
    )
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
