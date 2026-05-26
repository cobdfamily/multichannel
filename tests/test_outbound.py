from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import select

from multichannel.models import Message, MessageStatus, OutboxItem


def outbound_event(person_id: str) -> dict:
    return {
        "specversion": "1.0",
        "id": "evt-out-1",
        "source": "/tests",
        "type": "cobd.multichannel.message.send",
        "time": datetime.now(tz=UTC).isoformat(),
        "data": {
            "direction": "out",
            "provider": "postmark",
            "from": {"email": "sender@example.org"},
            "to": [{"email": "user@example.org", "person_id": person_id}],
            "subject": "Hello",
            "text": "Body",
            "attachments": [],
            "provider_message_id": "local-out-1",
            "provider_thread_id": "thread-1",
        },
    }


@pytest.mark.asyncio
async def test_outbound_happy_path(client, db_session):
    person_id = str(uuid4())
    resp = await client.post(
        "/api/v1/outbound",
        json=outbound_event(person_id),
        headers={"X-Actor-Id": "actor-1", "X-Actor-Type": "user", "X-Purpose": "care"},
    )
    assert resp.status_code == 202
    assert resp.json()["status"] == "queued"

    message = await db_session.scalar(select(Message))
    assert message is not None
    assert message.status == MessageStatus.QUEUED
    outbox = await db_session.scalar(select(OutboxItem))
    assert outbox is not None
    assert outbox.message_id == message.id


@pytest.mark.asyncio
async def test_outbound_consent_denied(client, db_session, medici):
    medici.allowed = False
    resp = await client.post(
        "/api/v1/outbound",
        json=outbound_event(str(uuid4())),
        headers={"X-Actor-Id": "actor-1", "X-Actor-Type": "user", "X-Purpose": "care"},
    )
    assert resp.status_code == 409
    assert await db_session.scalar(select(Message)) is None


@pytest.mark.asyncio
async def test_outbound_missing_actor_id(client):
    resp = await client.post(
        "/api/v1/outbound",
        json=outbound_event(str(uuid4())),
        headers={"X-Actor-Type": "user", "X-Purpose": "care"},
    )
    assert resp.status_code == 401
