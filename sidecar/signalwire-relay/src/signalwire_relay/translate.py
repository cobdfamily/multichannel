from __future__ import annotations

import hashlib
import hmac
from datetime import UTC, datetime
from typing import Any


def compute_hmac(secret: str, body: bytes) -> str:
    return hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()


def translate_event(raw: dict[str, Any]) -> dict[str, Any]:
    event_type = str(_first(raw, "event_type", "event", "type") or "")
    data = _payload(raw)

    provider_message_id = str(
        _first(data, "id", "message_id", "messageId", "sid", "call_id", "callId")
        or _first(raw, "id", "message_id", "messageId", "sid", "call_id", "callId")
        or ""
    )
    provider_thread_id = str(
        _first(
            data,
            "conversation_id",
            "conversationId",
            "thread_id",
            "threadId",
            "from",
            "from_number",
            "fromNumber",
        )
        or _first(
            raw,
            "conversation_id",
            "conversationId",
            "thread_id",
            "threadId",
            "from",
            "from_number",
            "fromNumber",
        )
        or provider_message_id
    )

    from_phone = str(_first(data, "from", "from_number", "fromNumber", "caller_id") or "")
    to_value = _first(data, "to", "to_number", "toNumber", "called_number")
    text_body = _extract_text(data)
    if _is_call_event(event_type, data):
        text_body = None

    return {
        "provider_message_id": provider_message_id,
        "provider_thread_id": provider_thread_id,
        "from": {"phone": from_phone},
        "to": [{"phone": phone} for phone in _phones(to_value)],
        "subject": None,
        "text_body": text_body,
        "html_body": None,
        "attachments": _extract_attachments(data),
        "received_at": _timestamp(raw, data),
        "raw": raw,
    }


def _payload(raw: dict[str, Any]) -> dict[str, Any]:
    for key in ("message", "payload", "params", "data", "call"):
        value = raw.get(key)
        if isinstance(value, dict):
            return value
    return raw


def _first(source: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = source.get(key)
        if value not in (None, ""):
            return value
    return None


def _extract_text(data: dict[str, Any]) -> str | None:
    value = _first(data, "body", "text", "message", "content")
    if value in (None, ""):
        return None
    return str(value)


def _extract_attachments(data: dict[str, Any]) -> list[str]:
    media = _first(data, "media", "media_urls", "mediaUrls", "attachments")
    if media is None:
        return []
    if isinstance(media, str):
        return [media]
    if isinstance(media, dict):
        values = media.values()
    elif isinstance(media, list | tuple):
        values = media
    else:
        return []

    urls: list[str] = []
    for item in values:
        if isinstance(item, str):
            urls.append(item)
        elif isinstance(item, dict):
            url = _first(item, "url", "media_url", "mediaUrl")
            if url:
                urls.append(str(url))
    return urls


def _phones(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        phone = _first(value, "phone", "number", "address")
        return [str(phone)] if phone else []
    if isinstance(value, list | tuple):
        phones: list[str] = []
        for item in value:
            phones.extend(_phones(item))
        return phones
    return [str(value)]


def _timestamp(raw: dict[str, Any], data: dict[str, Any]) -> str:
    value = _first(
        data,
        "received_at",
        "receivedAt",
        "timestamp",
        "created_at",
        "createdAt",
        "date_created",
    ) or _first(raw, "received_at", "receivedAt", "timestamp", "created_at", "createdAt")
    if isinstance(value, str) and value:
        if value.endswith("Z"):
            return value
        return value.replace("+00:00", "Z")
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _is_call_event(event_type: str, data: dict[str, Any]) -> bool:
    lower_type = event_type.lower()
    if "call" in lower_type or "voice" in lower_type:
        return True
    return any(key in data for key in ("call_id", "callId", "call_state", "direction"))
