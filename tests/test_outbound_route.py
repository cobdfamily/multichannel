from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

PERSON_ID = "11111111-1111-1111-1111-111111111111"


def outbound_event() -> dict:
    return {
        "specversion": "1.0",
        "id": "evt-1",
        "source": "/tests",
        "type": "cobd.multichannel.message.send",
        "time": datetime(2026, 5, 26, tzinfo=UTC).isoformat(),
        "data": {
            "direction": "out",
            "provider": "postmark",
            "from": {"email": "team@example.org"},
            "to": [{"email": "person@example.org", "person_id": PERSON_ID}],
            "subject": "Hello",
            "text": "Body",
            "provider_message_id": "out-1",
            "provider_thread_id": "thread-1",
        },
    }


def test_outbound_happy_path(test_client, database, medici, notaio) -> None:
    response = test_client.post(
        "/api/v1/outbound",
        json=outbound_event(),
        headers={"X-Actor-Id": "user-1", "X-Actor-Type": "service", "X-Purpose": "care"},
    )

    assert response.status_code == 202
    assert response.json()["status"] == "queued"
    medici.check_consent.assert_awaited_once_with(
        person_id=UUID(PERSON_ID),
        purpose="care",
        channel="email",
    )
    notaio.record.assert_awaited_once()

    message_count = len(database.store.messages)
    outbox_count = len(database.store.outbox_items)
    event_count = len(database.store.events)
    message = database.store.messages[0]
    assert (message_count, outbox_count, event_count) == (1, 1, 1)
    assert str(message.id) == response.json()["message_id"]
    assert message.status.value == "queued"


def test_outbound_consent_denied(test_client, database, medici, notaio) -> None:
    medici.check_consent.return_value = False

    response = test_client.post(
        "/api/v1/outbound",
        json=outbound_event(),
        headers={"X-Actor-Id": "user-1", "X-Actor-Type": "service", "X-Purpose": "care"},
    )

    assert response.status_code == 409
    assert response.json()["detail"]["error"]["code"] == "consent-revoked"
    notaio.record.assert_awaited_once()

    assert len(database.store.messages) == 0
