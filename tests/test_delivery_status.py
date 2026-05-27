"""Delivery-status webhook route tests."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from uuid import uuid4

from fastapi.testclient import TestClient

from multichannel.models import Message, MessageDirection, MessageProvider, MessageStatus


def _add_sent_message(store, provider: str, provider_message_id: str) -> Message:
    msg = Message(
        direction=MessageDirection.OUT,
        provider=MessageProvider(provider),
        provider_message_id=provider_message_id,
        provider_thread_id="t-1",
        conversation_id=uuid4(),
        from_addr={"address": "noreply@cobd.ca"},
        to_addrs=[{"address": "user@example.com"}],
        text_body="hi",
        status=MessageStatus.DISPATCHED,
        raw_payload={},
    )
    msg.id = uuid4()
    store.messages.append(msg)
    return msg


def _postmark_sig(body: bytes, secret: str) -> str:
    digest = hmac.new(secret.encode(), body, hashlib.sha1).digest()
    return base64.b64encode(digest).decode()


def _meta_sig(body: bytes, secret: str) -> str:
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def _sidecar_sig(body: bytes, secret: str) -> str:
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def test_postmark_delivery_updates_status(test_client: TestClient) -> None:
    store = test_client.app.state.multichannel.database.store
    msg = _add_sent_message(store, "postmark", "pm-1")
    body = json.dumps(
        {
            "RecordType": "Delivery",
            "MessageID": "pm-1",
            "DeliveredAt": "2026-01-01T00:00:00Z",
            "Description": "delivered",
        }
    ).encode()
    sig = _postmark_sig(body, "postmark-secret")
    r = test_client.post(
        "/api/v1/webhook/postmark/status",
        content=body,
        headers={
            "X-Postmark-Signature": sig,
            "Content-Type": "application/json",
        },
    )
    assert r.status_code == 200
    payload = r.json()
    assert payload["status"] == "updated"
    assert payload["new_status"] == "delivered"
    assert msg.status == MessageStatus.DELIVERED
    assert msg.delivered_at is not None


def test_postmark_bounce_updates_status(test_client: TestClient) -> None:
    store = test_client.app.state.multichannel.database.store
    msg = _add_sent_message(store, "postmark", "pm-2")
    body = json.dumps(
        {
            "RecordType": "Bounce",
            "MessageID": "pm-2",
            "Description": "hard bounce",
            "Type": "HardBounce",
        }
    ).encode()
    sig = _postmark_sig(body, "postmark-secret")
    r = test_client.post(
        "/api/v1/webhook/postmark/status",
        content=body,
        headers={
            "X-Postmark-Signature": sig,
            "Content-Type": "application/json",
        },
    )
    assert r.status_code == 200
    assert r.json()["new_status"] == "bounced"
    assert msg.status == MessageStatus.BOUNCED


def test_postmark_bad_signature_401(test_client: TestClient) -> None:
    body = json.dumps({"RecordType": "Delivery", "MessageID": "x"}).encode()
    r = test_client.post(
        "/api/v1/webhook/postmark/status",
        content=body,
        headers={
            "X-Postmark-Signature": "bad",
            "Content-Type": "application/json",
        },
    )
    assert r.status_code == 401


def test_postmark_unknown_message_returns_200_unknown(test_client: TestClient) -> None:
    body = json.dumps(
        {"RecordType": "Delivery", "MessageID": "never-sent"}
    ).encode()
    sig = _postmark_sig(body, "postmark-secret")
    r = test_client.post(
        "/api/v1/webhook/postmark/status",
        content=body,
        headers={
            "X-Postmark-Signature": sig,
            "Content-Type": "application/json",
        },
    )
    assert r.status_code == 200
    assert r.json()["status"] == "unknown_message"


def test_signalwire_status_updates_message(test_client: TestClient) -> None:
    store = test_client.app.state.multichannel.database.store
    msg = _add_sent_message(store, "signalwire", "sw-1")
    body = json.dumps(
        {"provider_message_id": "sw-1", "status": "delivered"}
    ).encode()
    sig = _sidecar_sig(body, "sidecar-secret")
    r = test_client.post(
        "/api/v1/webhook/signalwire/status",
        content=body,
        headers={
            "X-COBD-Sidecar-HMAC": sig,
            "Content-Type": "application/json",
        },
    )
    assert r.status_code == 200
    assert r.json()["new_status"] == "delivered"
    assert msg.status == MessageStatus.DELIVERED


def test_meta_delivered_event_updates_status(test_client: TestClient) -> None:
    store = test_client.app.state.multichannel.database.store
    msg = _add_sent_message(store, "fbmessenger", "fb-1")
    body = json.dumps(
        {
            "object": "page",
            "entry": [
                {
                    "id": "page-1",
                    "messaging": [
                        {
                            "delivery": {
                                "mids": ["fb-1"],
                                "watermark": 1700000000000,
                            }
                        }
                    ],
                }
            ],
        }
    ).encode()
    sig = _meta_sig(body, "meta-secret")
    r = test_client.post(
        "/api/v1/webhook/meta/status/fbmessenger",
        content=body,
        headers={
            "X-Hub-Signature-256": sig,
            "Content-Type": "application/json",
        },
    )
    assert r.status_code == 200
    assert r.json()["events_applied"] == 1
    assert msg.status == MessageStatus.DELIVERED


def test_meta_unknown_provider_404(test_client: TestClient) -> None:
    r = test_client.post(
        "/api/v1/webhook/meta/status/plaid",
        content=b"{}",
        headers={
            "X-Hub-Signature-256": "sha256=00",
            "Content-Type": "application/json",
        },
    )
    assert r.status_code == 404
