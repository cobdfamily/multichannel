from __future__ import annotations

import hashlib
import hmac
import json

import pytest
from sqlalchemy import func, select

from multichannel.models import Message, MessageProvider


def signed_body(payload: dict, secret: str) -> tuple[bytes, str]:
    body = json.dumps(payload, separators=(",", ":")).encode()
    digest = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return body, f"sha256={digest}"


@pytest.mark.asyncio
async def test_fbmessenger_hmac_ok(client, db_session, settings):
    payload = {
        "entry": [
            {
                "messaging": [
                    {
                        "sender": {"id": "psid-1"},
                        "recipient": {"id": "page-1"},
                        "message": {"mid": "fb-1", "text": "One"},
                    },
                    {
                        "sender": {"id": "psid-2"},
                        "recipient": {"id": "page-1"},
                        "message": {"mid": "fb-2", "text": "Two"},
                    },
                ]
            }
        ]
    }
    body, sig = signed_body(payload, settings.META_APP_SECRET)
    resp = await client.post(
        "/api/v1/webhook/fbmessenger",
        content=body,
        headers={"X-Hub-Signature-256": sig, "Content-Type": "application/json"},
    )
    assert resp.status_code == 200
    count = await db_session.scalar(select(func.count()).select_from(Message))
    assert count == 2


@pytest.mark.asyncio
async def test_instagram_modern_shape(client, db_session, settings):
    payload = {
        "entry": [
            {
                "id": "ig-business",
                "changes": [
                    {
                        "value": {
                            "messages": [
                                {"id": "ig-1", "from": "ig-user", "text": "Hi"},
                            ]
                        }
                    }
                ],
            }
        ]
    }
    body, sig = signed_body(payload, settings.META_APP_SECRET)
    resp = await client.post(
        "/api/v1/webhook/instagram",
        content=body,
        headers={"X-Hub-Signature-256": sig, "Content-Type": "application/json"},
    )
    assert resp.status_code == 200
    message = await db_session.scalar(select(Message).where(Message.provider == MessageProvider.INSTAGRAM))
    assert message is not None
    assert message.provider_message_id == "ig-1"


@pytest.mark.asyncio
async def test_meta_hmac_bad(client, db_session):
    resp = await client.post(
        "/api/v1/webhook/fbmessenger",
        json={"entry": []},
        headers={"X-Hub-Signature-256": "sha256=bad"},
    )
    assert resp.status_code == 401
    assert await db_session.scalar(select(Message)) is None


@pytest.mark.asyncio
async def test_meta_get_verify_token(client):
    ok = await client.get(
        "/api/v1/webhook/fbmessenger",
        params={
            "hub.mode": "subscribe",
            "hub.verify_token": "verify-token",
            "hub.challenge": "challenge-value",
        },
    )
    assert ok.status_code == 200
    assert ok.text == "challenge-value"

    bad = await client.get(
        "/api/v1/webhook/instagram",
        params={
            "hub.mode": "subscribe",
            "hub.verify_token": "wrong",
            "hub.challenge": "challenge-value",
        },
    )
    assert bad.status_code == 403
