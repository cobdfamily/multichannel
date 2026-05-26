"""Instagram Messaging provider adapter."""

from __future__ import annotations

from typing import Any

from multichannel.providers.fbmessenger import send as _send
from multichannel.providers.fbmessenger import verify_hmac_sha256
from multichannel.providers.types import ParsedInbound


async def send(*args, **kwargs):  # noqa: ANN002, ANN003, ANN201
    return await _send(*args, **kwargs)


def parse_inbound(
    payload: dict[str, Any],
    entry_index: int,
    message_index: int,
) -> ParsedInbound:
    parsed = __import__(
        "multichannel.providers.fbmessenger",
        fromlist=["parse_inbound"],
    ).parse_inbound(payload, entry_index, message_index)
    return ParsedInbound(
        provider="instagram",
        provider_message_id=parsed.provider_message_id,
        provider_thread_id=parsed.provider_thread_id,
        from_addr=parsed.from_addr,
        to_addrs=parsed.to_addrs,
        subject=parsed.subject,
        text_body=parsed.text_body,
        html_body=parsed.html_body,
        attachments=parsed.attachments,
        raw_payload=parsed.raw_payload,
    )
