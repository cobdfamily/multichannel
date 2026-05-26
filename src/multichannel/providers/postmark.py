"""Postmark provider adapter."""

from __future__ import annotations

import base64
import hashlib
import hmac
from email.utils import getaddresses
from typing import Any

import httpx

from multichannel.config import Settings
from multichannel.models import Message
from multichannel.providers.types import ParsedInbound


def verify_hmac(body: bytes, signature_header: str, secret: str) -> bool:
    if not signature_header or not secret:
        return False
    digest = hmac.new(secret.encode(), body, hashlib.sha1).digest()
    expected = base64.b64encode(digest).decode()
    return hmac.compare_digest(expected, signature_header)


def _header(payload: dict[str, Any], name: str) -> str | None:
    for item in payload.get("Headers") or []:
        if str(item.get("Name", "")).lower() == name.lower():
            value = item.get("Value")
            return str(value) if value is not None else None
    return None


def _thread_id(value: str | None) -> str | None:
    if not value:
        return None
    parts = value.split()
    return parts[0].strip("<>") if parts else None


def _addresses(value: str | None, full: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    if full:
        rows = []
        for item in full:
            row = {"email": item.get("Email") or item.get("email")}
            name = item.get("Name") or item.get("name")
            if name:
                row["name"] = name
            rows.append(row)
        return rows
    rows = []
    for name, email in getaddresses([value or ""]):
        row = {"email": email}
        if name:
            row["name"] = name
        rows.append(row)
    return rows


def parse_inbound(payload: dict[str, Any]) -> ParsedInbound:
    in_reply_to = _header(payload, "In-Reply-To")
    references = _header(payload, "References")
    provider_message_id = str(payload.get("MessageID") or payload.get("MessageId") or "")
    thread_id = _thread_id(in_reply_to) or _thread_id(references) or provider_message_id
    to_addrs = _addresses(payload.get("To"), payload.get("ToFull"))
    return ParsedInbound(
        provider="postmark",
        provider_message_id=provider_message_id,
        provider_thread_id=thread_id,
        from_addr={"email": payload.get("From"), "name": payload.get("FromName")},
        to_addrs=to_addrs,
        subject=payload.get("Subject"),
        text_body=payload.get("TextBody"),
        html_body=payload.get("HtmlBody"),
        attachments=payload.get("Attachments") or [],
        raw_payload=payload,
    )


async def send(message: Message, settings: Settings) -> dict[str, str]:
    to_addr = message.to_addrs[0].get("email") or message.to_addrs[0].get("address")
    from_addr = message.from_addr.get("email") or settings.POSTMARK_FROM_EMAIL
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(
            "https://api.postmarkapp.com/email",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "X-Postmark-Server-Token": settings.POSTMARK_SERVER_TOKEN,
            },
            json={
                "From": from_addr,
                "To": to_addr,
                "Subject": message.subject or "",
                "TextBody": message.text_body or "",
                "HtmlBody": message.html_body,
                "Attachments": message.attachments or [],
            },
        )
        resp.raise_for_status()
        payload = resp.json()
    return {
        "provider_message_id": str(payload.get("MessageID") or payload.get("MessageId") or ""),
        "status": "dispatched",
    }
