"""Meta Messenger and Instagram provider adapter."""

from __future__ import annotations

import hashlib
import hmac
from typing import Any

import httpx

from multichannel.config import Settings
from multichannel.models import Message


def verify_hmac_sha256(body: bytes, signature_header: str, secret: str) -> bool:
    if not signature_header or not secret or not signature_header.startswith("sha256="):
        return False
    expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    supplied = signature_header.split("=", 1)[1]
    return hmac.compare_digest(expected, supplied)


def _message_to_item(item: dict[str, Any], provider: str) -> dict[str, Any] | None:
    message = item.get("message") or {}
    mid = message.get("mid") or item.get("message_id")
    if not mid:
        return None
    sender_id = str((item.get("sender") or {}).get("id") or "")
    recipient_id = str((item.get("recipient") or {}).get("id") or "")
    return {
        "provider": provider,
        "provider_message_id": str(mid),
        "provider_thread_id": sender_id or str(mid),
        "from_addr": {"id": sender_id},
        "to_addrs": [{"id": recipient_id}],
        "subject": None,
        "text_body": message.get("text"),
        "html_body": None,
        "attachments": message.get("attachments") or [],
        "raw_payload": item,
    }


def _instagram_change_message(
    entry: dict[str, Any], change: dict[str, Any], message: dict[str, Any], provider: str
) -> dict[str, Any] | None:
    mid = message.get("mid") or message.get("id")
    if not mid:
        return None
    sender_id = str(message.get("from") or message.get("sender_id") or "")
    recipient_id = str(entry.get("id") or (change.get("value") or {}).get("recipient_id") or "")
    return {
        "provider": provider,
        "provider_message_id": str(mid),
        "provider_thread_id": sender_id or str(mid),
        "from_addr": {"id": sender_id},
        "to_addrs": [{"id": recipient_id}],
        "subject": None,
        "text_body": message.get("text"),
        "html_body": None,
        "attachments": message.get("attachments") or [],
        "raw_payload": message,
    }


def parse_inbound(payload: dict[str, Any], provider: str) -> list[dict[str, Any]]:
    parsed: list[dict[str, Any]] = []
    for entry in payload.get("entry") or []:
        for item in entry.get("messaging") or []:
            normalized = _message_to_item(item, provider)
            if normalized is not None:
                parsed.append(normalized)
        for change in entry.get("changes") or []:
            value = change.get("value") or {}
            for message in value.get("messages") or []:
                normalized = _instagram_change_message(entry, change, message, provider)
                if normalized is not None:
                    parsed.append(normalized)
    return parsed


async def send(message: Message, settings: Settings, provider: str) -> dict[str, str]:
    recipient_id = message.to_addrs[0].get("id") or message.to_addrs[0].get("psid")
    token = (
        settings.INSTAGRAM_ACCESS_TOKEN
        if provider == "instagram" and settings.INSTAGRAM_ACCESS_TOKEN
        else settings.META_PAGE_ACCESS_TOKEN
    )
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
