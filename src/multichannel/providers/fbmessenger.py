"""Meta Messenger provider adapter."""

from __future__ import annotations

import hashlib
import hmac
from typing import Any

import httpx

from multichannel.config import Settings
from multichannel.models import Message
from multichannel.providers.types import ParsedInbound


def verify_hmac_sha256(body: bytes, signature: str, secret: str) -> bool:
    if not signature or not secret or not signature.startswith("sha256="):
        return False
    expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    supplied = signature.split("=", 1)[1]
    return hmac.compare_digest(expected, supplied)


def parse_inbound(
    payload: dict[str, Any],
    entry_index: int,
    message_index: int,
) -> ParsedInbound:
    item = payload["entry"][entry_index]["messaging"][message_index]
    sender_id = str(item["sender"]["id"])
    recipient_id = str(item["recipient"]["id"])
    message = item.get("message") or {}
    provider_message_id = str(message.get("mid") or item.get("message_id") or "")
    return ParsedInbound(
        provider="fbmessenger",
        provider_message_id=provider_message_id,
        provider_thread_id=sender_id,
        from_addr={"id": sender_id},
        to_addrs=[{"id": recipient_id}],
        text_body=message.get("text"),
        attachments=message.get("attachments") or [],
        raw_payload=item,
    )


async def send(message: Message, settings: Settings) -> dict[str, str]:
    recipient_id = message.to_addrs[0].get("id") or message.to_addrs[0].get("psid")
    token = settings.META_PAGE_ACCESS_TOKEN
    url = f"https://graph.facebook.com/{settings.META_GRAPH_API_VERSION}/me/messages"
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(
            url,
            params={"access_token": token},
            json={
                "recipient": {"id": recipient_id},
                "message": {"text": message.text_body or ""},
            },
        )
        resp.raise_for_status()
        payload = resp.json()
    return {
        "provider_message_id": str(payload.get("message_id") or payload.get("mid") or ""),
        "status": "dispatched",
    }
