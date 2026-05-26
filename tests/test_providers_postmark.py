from __future__ import annotations

import base64
import hashlib
import hmac
import json

from multichannel.providers.postmark import parse_inbound, verify_hmac


def fixture() -> dict:
    return {
        "MessageID": "pm-1",
        "From": "sender@example.org",
        "FromName": "Sender",
        "To": "inbox@example.org",
        "Subject": "Re: Hello",
        "TextBody": "Plain text",
        "HtmlBody": "<p>Plain text</p>",
        "Headers": [
            {"Name": "References", "Value": "<root@example.org> <reply@example.org>"},
            {"Name": "In-Reply-To", "Value": "<reply@example.org>"},
        ],
    }


def test_verify_hmac() -> None:
    body = json.dumps(fixture(), separators=(",", ":")).encode()
    signature = base64.b64encode(hmac.new(b"secret", body, hashlib.sha1).digest()).decode()

    assert verify_hmac(body, signature, "secret") is True
    assert verify_hmac(body, "bad", "secret") is False


def test_parse_inbound() -> None:
    parsed = parse_inbound(fixture())

    assert parsed.provider == "postmark"
    assert parsed.provider_message_id == "pm-1"
    assert parsed.provider_thread_id == "reply@example.org"
    assert parsed.from_addr["email"] == "sender@example.org"
    assert parsed.to_addrs == [{"email": "inbox@example.org"}]
    assert parsed.text_body == "Plain text"
