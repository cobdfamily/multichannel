"""Canonical message rows."""

from __future__ import annotations

import enum
import uuid
from datetime import datetime

from sqlalchemy import JSON, DateTime, Enum, String, Text, UniqueConstraint, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from multichannel.models.base import Base, TimestampMixin


def enum_values(enum_cls: type[enum.Enum]) -> list[str]:
    return [member.value for member in enum_cls]


class MessageDirection(str, enum.Enum):
    IN = "in"
    OUT = "out"


class MessageProvider(str, enum.Enum):
    POSTMARK = "postmark"
    SIGNALWIRE = "signalwire"
    FBMESSENGER = "fbmessenger"
    INSTAGRAM = "instagram"


class MessageStatus(str, enum.Enum):
    RECEIVED = "received"
    QUEUED = "queued"
    DISPATCHED = "dispatched"
    DELIVERED = "delivered"
    FAILED = "failed"
    BOUNCED = "bounced"


class Message(Base, TimestampMixin):
    __tablename__ = "messages"
    __table_args__ = (
        UniqueConstraint(
            "provider",
            "provider_message_id",
            name="uq_messages_provider_message_id",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    direction: Mapped[MessageDirection] = mapped_column(
        Enum(
            MessageDirection,
            native_enum=False,
            length=16,
            values_callable=enum_values,
        ),
        nullable=False,
    )
    provider: Mapped[MessageProvider] = mapped_column(
        Enum(
            MessageProvider,
            native_enum=False,
            length=32,
            values_callable=enum_values,
        ),
        nullable=False,
    )
    provider_message_id: Mapped[str] = mapped_column(String(255), nullable=False)
    provider_thread_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    conversation_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        nullable=False,
        index=True,
    )
    from_addr: Mapped[dict] = mapped_column(JSON, nullable=False)
    to_addrs: Mapped[list[dict]] = mapped_column(JSON, nullable=False)
    subject: Mapped[str | None] = mapped_column(String, nullable=True)
    text_body: Mapped[str | None] = mapped_column(Text, nullable=True)
    html_body: Mapped[str | None] = mapped_column(Text, nullable=True)
    attachments: Mapped[list[dict] | None] = mapped_column(JSON, nullable=True)
    status: Mapped[MessageStatus] = mapped_column(
        Enum(
            MessageStatus,
            native_enum=False,
            length=32,
            values_callable=enum_values,
        ),
        nullable=False,
    )
    status_detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    requested_purpose: Mapped[str | None] = mapped_column(String(255), nullable=True)
    requested_by_actor_id: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
    )
    received_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    delivered_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    outbox_items: Mapped[list["OutboxItem"]] = relationship(
        back_populates="message",
        cascade="all, delete-orphan",
    )
