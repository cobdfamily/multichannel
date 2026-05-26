from __future__ import annotations

import hashlib
import hmac
import json

import pytest
from sqlalchemy import select

from multichannel.models import Message, MessageProvider


def signed_body(payload: dict, secret: str) -> tuple[bytes, str]:
    body = json.dumps(payload, separators=(",", ":")).encode()
    return body, hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def payload() -> dict:
    return {
        "provider_message_id": "sw-1",
        "provider_thread_id": "+15551112222",
        "from": "+15551112222",
        "to": "+15553334444",
        "body": "SMS body",
    }


@pytest.mark.asyncio
async def test_signalwire_sidecar_hmac_ok(client, db_session, settings):
    body, sig = signed_body(payload(), settings.SIGNALWIRE_SIDECAR_HMAC)
    resp = await client.post(
        "/api/v1/webhook/signalwire",
        content=body,
        headers={"X-COBD-Sidecar-HMAC": sig, "Content-Type": "application/json"},
    )
    assert resp.status_code == 200
    message = await db_session.scalar(select(Message).where(Message.provider == MessageProvider.SIGNALWIRE))
    assert message is not None
    assert message.provider_message_id == "sw-1"


@pytest.mark.asyncio
async def test_signalwire_sidecar_hmac_bad(client, db_session):
    resp = await client.post(
        "/api/v1/webhook/signalwire",
        json=payload(),
        headers={"X-COBD-Sidecar-HMAC": "bad"},
    )
    assert resp.status_code == 401
    assert await db_session.scalar(select(Message)) is None
