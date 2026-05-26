from __future__ import annotations

import hashlib
import hmac
import json

from multichannel.providers.meta import parse_inbound, verify_hmac_sha256


def fbmessenger_fixture() -> dict:
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


def instagram_fixture() -> dict:
    return {
        "object": "instagram",
        "entry": [
            {
                "id": "ig-account-1",
                "changes": [
                    {
                        "field": "messages",
                        "value": {
                            "messages": [
                                {
                                    "mid": "ig-mid-1",
                                    "from": "ig-user-9",
                                    "text": "Ola",
                                }
                            ]
                        },
                    }
                ],
            }
        ],
    }


def test_verify_hmac_sha256_round_trip() -> None:
    body = json.dumps(fbmessenger_fixture(), separators=(",", ":")).encode()
    signature = "sha256=" + hmac.new(b"secret", body, hashlib.sha256).hexdigest()
    assert verify_hmac_sha256(body, signature, "secret") is True


def test_verify_hmac_sha256_rejects_bad_sig() -> None:
    body = json.dumps(fbmessenger_fixture(), separators=(",", ":")).encode()
    assert verify_hmac_sha256(body, "sha256=bad", "secret") is False
    assert verify_hmac_sha256(body, "", "secret") is False
    assert verify_hmac_sha256(body, "md5=bad", "secret") is False


def test_parse_inbound_fbmessenger() -> None:
    parsed = parse_inbound(fbmessenger_fixture(), provider="fbmessenger")
    assert len(parsed) == 1
    m = parsed[0]
    assert m["provider"] == "fbmessenger"
    assert m["provider_message_id"] == "mid-1"
    assert m["provider_thread_id"] == "psid-1"
    assert m["from_addr"] == {"id": "psid-1"}
    assert m["to_addrs"] == [{"id": "page-1"}]
    assert m["text_body"] == "Hello"


def test_parse_inbound_instagram_modern_shape() -> None:
    parsed = parse_inbound(instagram_fixture(), provider="instagram")
    assert len(parsed) == 1
    m = parsed[0]
    assert m["provider"] == "instagram"
    assert m["provider_message_id"] == "ig-mid-1"
    assert m["provider_thread_id"] == "ig-user-9"
    assert m["text_body"] == "Ola"


def test_parse_inbound_empty_payload_returns_empty_list() -> None:
    assert parse_inbound({}, provider="fbmessenger") == []
    assert parse_inbound({"entry": []}, provider="fbmessenger") == []
