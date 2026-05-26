import json

import httpx
import pytest

from multichannel.config import Settings
from multichannel.services.notaio_client import AuditEvent, NotaioClient


def settings() -> Settings:
    return Settings(
        DATABASE_URL="postgresql+asyncpg://user:pass@localhost/test",
        NOTAIO_URL="https://notaio.example",
        NOTAIO_CLIENT_SECRET="notaio-secret",
    )


@pytest.mark.asyncio
async def test_record_posts_audit_event(httpx_mock) -> None:
    httpx_mock.add_response(
        method="POST",
        url="https://notaio.example/api/v1/events",
        status_code=201,
        json={"id": "receipt-1"},
    )
    client = NotaioClient(settings())
    event = AuditEvent(
        actor_user_id="user-1",
        action="message.received",
        outcome="success",
        subject="message-1",
        metadata={"provider": "postmark"},
    )

    await client.record(event)
    await client.close()

    request = httpx_mock.get_request()
    assert request is not None
    assert request.headers["Authorization"] == "Bearer notaio-secret"
    assert json.loads(request.content) == {
        "actor_user_id": "user-1",
        "action": "message.received",
        "outcome": "success",
        "subject": "message-1",
        "metadata": {"provider": "postmark"},
    }
    assert str(request.url) == "https://notaio.example/api/v1/events"


@pytest.mark.asyncio
async def test_record_raises_on_non_2xx(httpx_mock) -> None:
    httpx_mock.add_response(
        method="POST",
        url="https://notaio.example/api/v1/events",
        status_code=503,
        json={"error": "down"},
    )
    client = NotaioClient(settings())

    with pytest.raises(httpx.HTTPStatusError):
        await client.record(
            AuditEvent(
                actor_user_id="user-1",
                action="message.dispatched",
                outcome="error",
                subject="message-1",
                metadata={},
            )
        )
    await client.close()
