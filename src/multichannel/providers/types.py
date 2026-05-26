"""Provider-normalized inbound message shapes."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ParsedInbound:
    provider: str
    provider_message_id: str
    provider_thread_id: str | None
    from_addr: dict[str, Any]
    to_addrs: list[dict[str, Any]]
    subject: str | None = None
    text_body: str | None = None
    html_body: str | None = None
    attachments: list[dict[str, Any]] = field(default_factory=list)
    raw_payload: dict[str, Any] = field(default_factory=dict)
