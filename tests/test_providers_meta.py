from __future__ import annotations

import hashlib
import hmac
import json

from multichannel.providers.fbmessenger import parse_inbound, verify_hmac_sha256


def fixture() -> dict:
    return {
        "object": "page",
        "entry": [
            {
                "id": "page-1",
                "messaging": [
                    {
                        "sender": {"id": "psid-1"},
                        "recipient": {"id": "page-1"},
                        "message": {"mid": "mid-1", "text": "Hello"},
                    }
                ],
            }
        ],
    }


def test_verify_hmac_sha256() -> None:
    body = json.dumps(fixture(), separators=(",", ":")).encode()
    signature = "sha256=" + hmac.new(b"secret", body, hashlib.sha256).hexdigest()

    assert verify_hmac_sha256(body, signature, "secret") is True
    assert verify_hmac_sha256(body, "sha256=bad", "secret") is False


def test_parse_inbound() -> None:
    parsed = parse_inbound(fixture(), 0, 0)

    assert parsed.provider == "fbmessenger"
    assert parsed.provider_message_id == "mid-1"
    assert parsed.provider_thread_id == "psid-1"
    assert parsed.from_addr == {"id": "psid-1"}
    assert parsed.to_addrs == [{"id": "page-1"}]
    assert parsed.text_body == "Hello"
