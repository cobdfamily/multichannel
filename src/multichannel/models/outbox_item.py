"""Provider dispatch outbox rows."""

from __future__ import annotations

import enum
import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, Enum, ForeignKey, Integer, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from multichannel.models.base import Base, TimestampMixin
from multichannel.models.message import enum_values

if TYPE_CHECKING:
    from multichannel.models.message import Message


class OutboxItemState(str, enum.Enum):
    PENDING = "pending"
    CLAIMED = "claimed"
    DONE = "done"
    DEAD = "dead"


class OutboxItem(Base, TimestampMixin):
    __tablename__ = "outbox_items"

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    message_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("messages.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default="0",
    )
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    state: Mapped[OutboxItemState] = mapped_column(
        Enum(
            OutboxItemState,
            native_enum=False,
            length=16,
            values_callable=enum_values,
        ),
        nullable=False,
        default=OutboxItemState.PENDING,
        server_default=OutboxItemState.PENDING.value,
    )

    message: Mapped["Message"] = relationship(back_populates="outbox_items")
