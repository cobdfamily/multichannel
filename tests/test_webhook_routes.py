from __future__ import annotations

import base64
import hashlib
import hmac
import json

import pytest

from tests.conftest import db_count, db_messages


def body(payload: dict) -> bytes:
    return json.dumps(payload, separators=(",", ":")).encode()


def postmark_payload(message_id: str = "pm-1") -> dict:
    return {
        "MessageID": message_id,
        "From": "sender@example.org",
        "To": "inbox@example.org",
        "TextBody": "Hello",
        "Headers": [{"Name": "In-Reply-To", "Value": "<thread-1@example.org>"}],
    }


def meta_payload(message_id: str = "mid-1", object_: str = "page") -> dict:
    return {
        "object": object_,
        "entry": [
            {
                "id": "page-1",
                "messaging": [
                    {
                        "sender": {"id": "thread-psid"},
                        "recipient": {"id": "page-1"},
                        "message": {"mid": message_id, "text": "Hello"},
                    }
                ],
            }
        ],
    }


def signalwire_payload(message_id: str = "sw-1") -> dict:
    return {
        "provider_message_id": message_id,
        "provider_thread_id": "+15551230000",
        "from": "+15551230000",
        "to": "+15557650000",
        "text": "Hello",
    }


def postmark_sig(raw: bytes, secret: str = "postmark-secret") -> str:
    return base64.b64encode(hmac.new(secret.encode(), raw, hashlib.sha1).digest()).decode()


def meta_sig(raw: bytes, secret: str = "meta-secret") -> str:
    return "sha256=" + hmac.new(secret.encode(), raw, hashlib.sha256).hexdigest()


def signalwire_sig(raw: bytes, secret: str = "sidecar-secret") -> str:
    return hmac.new(secret.encode(), raw, hashlib.sha256).hexdigest()


CASES = [
    ("postmark", "/api/v1/webhook/postmark", postmark_payload, "X-Postmark-Signature", postmark_sig),
    ("fbmessenger", "/api/v1/webhook/fbmessenger", meta_payload, "X-Hub-Signature-256", meta_sig),
    (
        "instagram",
        "/api/v1/webhook/instagram",
        lambda message_id: meta_payload(message_id, "instagram"),
        "X-Hub-Signature-256",
        meta_sig,
    ),
    ("signalwire", "/api/v1/webhook/signalwire", signalwire_payload, "X-COBD-Sidecar-HMAC", signalwire_sig),
]


@pytest.mark.parametrize(("provider", "path", "payload_fn", "header", "sign"), CASES)
def test_webhook_signature_dedupe_and_conversation(
    test_client,
    database,
    provider,
    path,
    payload_fn,
    header,
    sign,
) -> None:
    raw = body(payload_fn(f"{provider}-1"))
    bad = test_client.post(path, content=raw, headers={header: "bad", "content-type": "application/json"})
    assert bad.status_code == 401
    assert db_count(database) == 0

    first = test_client.post(
        path,
        content=raw,
        headers={header: sign(raw), "content-type": "application/json"},
    )
    assert first.status_code == 200
    assert first.json()["status"] == "received"
    assert db_count(database) == 1

    duplicate = test_client.post(
        path,
        content=raw,
        headers={header: sign(raw), "content-type": "application/json"},
    )
    assert duplicate.status_code == 200
    assert duplicate.json()["status"] == "duplicate"
    assert db_count(database) == 1

    second_raw = body(payload_fn(f"{provider}-2"))
    second = test_client.post(
        path,
        content=second_raw,
        headers={header: sign(second_raw), "content-type": "application/json"},
    )
    assert second.status_code == 200
    messages = db_messages(database)
    assert len(messages) == 2
    assert messages[0].provider.value == provider
    assert messages[0].conversation_id == messages[1].conversation_id


def test_meta_verify_token_challenge(test_client) -> None:
    for provider in ("fbmessenger", "instagram"):
        response = test_client.get(
            f"/api/v1/webhook/{provider}",
            params={"hub.verify_token": "verify-token", "hub.challenge": "challenge-123"},
        )
        assert response.status_code == 200
        assert response.text == "challenge-123"

        denied = test_client.get(
            f"/api/v1/webhook/{provider}",
            params={"hub.verify_token": "wrong", "hub.challenge": "challenge-123"},
        )
        assert denied.status_code == 403
