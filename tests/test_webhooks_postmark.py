from __future__ import annotations

import base64
import hashlib
import hmac
import json

import pytest
from sqlalchemy import func, select

from multichannel.lib.conversation_id import derive_conversation_id
from multichannel.models import EventOutbox, Message, MessageProvider


def signed_body(payload: dict, secret: str) -> tuple[bytes, str]:
    body = json.dumps(payload, separators=(",", ":")).encode()
    digest = hmac.new(secret.encode(), body, hashlib.sha1).digest()
    return body, base64.b64encode(digest).decode()


def payload(message_id: str = "pm-1") -> dict:
    return {
        "MessageID": message_id,
        "FromName": "Alice",
        "From": "alice@example.org",
        "To": "team@example.org",
        "ToFull": [{"Email": "team@example.org", "Name": "Team"}],
        "Subject": "Question",
        "TextBody": "Hello",
        "HtmlBody": "<p>Hello</p>",
        "Attachments": [],
        "Headers": [{"Name": "References", "Value": "<root-thread@example.org> <other@example.org>"}],
    }


@pytest.mark.asyncio
async def test_postmark_hmac_ok(client, db_session, settings):
    body, sig = signed_body(payload(), settings.POSTMARK_WEBHOOK_SECRET)
    resp = await client.post(
        "/api/v1/webhook/postmark",
        content=body,
        headers={"X-Postmark-Signature": sig, "Content-Type": "application/json"},
    )
    assert resp.status_code == 200
    assert await db_session.scalar(select(Message)) is not None
    assert await db_session.scalar(select(EventOutbox)) is not None


@pytest.mark.asyncio
async def test_postmark_hmac_bad(client, db_session):
    resp = await client.post(
        "/api/v1/webhook/postmark",
        json=payload(),
        headers={"X-Postmark-Signature": "bad"},
    )
    assert resp.status_code == 401
    assert await db_session.scalar(select(Message)) is None


@pytest.mark.asyncio
async def test_postmark_dedupe(client, db_session, settings):
    body, sig = signed_body(payload("same-id"), settings.POSTMARK_WEBHOOK_SECRET)
    headers = {"X-Postmark-Signature": sig, "Content-Type": "application/json"}
    first = await client.post("/api/v1/webhook/postmark", content=body, headers=headers)
    second = await client.post("/api/v1/webhook/postmark", content=body, headers=headers)
    assert first.status_code == 200
    assert second.status_code == 200
    assert second.json()["status"] == "duplicate"
    count = await db_session.scalar(select(func.count()).select_from(Message))
    assert count == 1


@pytest.mark.asyncio
async def test_postmark_conversation_id_from_references(client, db_session, settings):
    body, sig = signed_body(payload("pm-refs"), settings.POSTMARK_WEBHOOK_SECRET)
    resp = await client.post(
        "/api/v1/webhook/postmark",
        content=body,
        headers={"X-Postmark-Signature": sig, "Content-Type": "application/json"},
    )
    assert resp.status_code == 200
    message = await db_session.scalar(select(Message).where(Message.provider == MessageProvider.POSTMARK))
    assert message.conversation_id == derive_conversation_id("postmark", "root-thread@example.org")
