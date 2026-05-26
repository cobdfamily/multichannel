import hashlib
import hmac

from signalwire_relay.translate import compute_hmac, translate_event


def test_translate_inbound_sms() -> None:
    raw = {
        "event_type": "messaging.message.received",
        "message": {
            "id": "msg-123",
            "conversation_id": "thread-abc",
            "from": "+15551230001",
            "to": "+15551230002",
            "body": "hello",
            "received_at": "2026-05-26T20:00:00Z",
        },
    }

    assert translate_event(raw) == {
        "provider_message_id": "msg-123",
        "provider_thread_id": "thread-abc",
        "from": {"phone": "+15551230001"},
        "to": [{"phone": "+15551230002"}],
        "subject": None,
        "text_body": "hello",
        "html_body": None,
        "attachments": [],
        "received_at": "2026-05-26T20:00:00Z",
        "raw": raw,
    }


def test_translate_inbound_mms() -> None:
    raw = {
        "event_type": "messaging.message.received",
        "message": {
            "message_id": "msg-456",
            "from_number": "+15551230003",
            "to_number": "+15551230004",
            "text": "see attached",
            "media": [
                {"url": "https://cdn.example.test/a.jpg"},
                {"mediaUrl": "https://cdn.example.test/b.png"},
            ],
            "timestamp": "2026-05-26T20:01:00Z",
        },
    }

    event = translate_event(raw)

    assert event["provider_message_id"] == "msg-456"
    assert event["provider_thread_id"] == "+15551230003"
    assert event["from"] == {"phone": "+15551230003"}
    assert event["to"] == [{"phone": "+15551230004"}]
    assert event["text_body"] == "see attached"
    assert event["attachments"] == [
        "https://cdn.example.test/a.jpg",
        "https://cdn.example.test/b.png",
    ]
    assert event["subject"] is None
    assert event["html_body"] is None
    assert event["received_at"] == "2026-05-26T20:01:00Z"
    assert event["raw"] == raw


def test_translate_voice_event() -> None:
    raw = {
        "event_type": "calling.call.received",
        "call": {
            "call_id": "call-789",
            "from": "+15551230005",
            "to": [{"phone": "+15551230006"}],
            "direction": "inbound",
            "timestamp": "2026-05-26T20:02:00Z",
        },
    }

    event = translate_event(raw)

    assert event["provider_message_id"] == "call-789"
    assert event["provider_thread_id"] == "+15551230005"
    assert event["from"] == {"phone": "+15551230005"}
    assert event["to"] == [{"phone": "+15551230006"}]
    assert event["text_body"] is None
    assert event["attachments"] == []
    assert event["raw"]["call"]["direction"] == "inbound"


def test_compute_hmac() -> None:
    secret = "shared-secret"
    body = b'{"provider_message_id":"msg-123"}'

    expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()

    assert compute_hmac(secret, body) == expected
