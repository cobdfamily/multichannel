"""CloudEvents 1.0 schemas for canonical message events."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from multichannel.models.message import Message


class MessageData(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    direction: Literal["in", "out"]
    provider: Literal["postmark", "signalwire", "fbmessenger", "instagram"]
    from_: dict[str, Any] = Field(alias="from")
    to: list[dict[str, Any]]
    subject: str | None = None
    text: str | None = None
    html: str | None = None
    attachments: list[dict[str, Any]] = Field(default_factory=list)
    provider_message_id: str
    provider_thread_id: str | None = None


class CloudEvent(BaseModel):
    specversion: Literal["1.0"] = "1.0"
    id: str
    source: str
    type: str
    time: datetime
    data: MessageData | dict[str, Any]

    @classmethod
    def received(cls, message: Message) -> "CloudEvent":
        return cls._from_message("cobd.multichannel.message.received", message)

    @classmethod
    def dispatched(cls, message: Message) -> "CloudEvent":
        return cls._from_message("cobd.multichannel.message.dispatched", message)

    @classmethod
    def failed(cls, message: Message, error: str | None = None) -> "CloudEvent":
        event = cls._from_message("cobd.multichannel.message.failed", message)
        if error is not None:
            event.data = event.data.model_dump(by_alias=True) | {"error": error}
        return event

    @classmethod
    def _from_message(cls, event_type: str, message: Message) -> "CloudEvent":
        event_time = (
            message.delivered_at
            or message.sent_at
            or message.received_at
            or datetime.now(tz=UTC)
        )
        return cls(
            id=str(message.id or uuid.uuid4()),
            source=f"/multichannel/{message.provider.value}",
            type=event_type,
            time=event_time,
            data=MessageData(
                direction=message.direction.value,
                provider=message.provider.value,
                from_=message.from_addr,
                to=message.to_addrs,
                subject=message.subject,
                text=message.text_body,
                html=message.html_body,
                attachments=message.attachments or [],
                provider_message_id=message.provider_message_id,
                provider_thread_id=message.provider_thread_id,
            ),
        )
