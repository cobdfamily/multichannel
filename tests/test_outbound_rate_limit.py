"""Route-level rate-limit integration tests.

Verifies the /outbound route returns 429 + Retry-After when
buckets are exhausted, and stays at 202 with the limiter
disabled.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import fakeredis.aioredis
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from multichannel.api import router
from multichannel.config import Settings
from multichannel.runtime import AppState, session_dep
from multichannel.services.rate_limit import RateLimiter

# Re-use the fake session machinery from conftest by importing.
from tests.conftest import (
    DummyRedis,
    FakeDatabase,
    MemoryStore,
    MediciFake,
)


def _outbound_event(provider: str = "postmark") -> dict[str, Any]:
    person_id = str(uuid4())
    return {
        "specversion": "1.0",
        "id": str(uuid4()),
        "source": f"/test/{provider}",
        "type": "cobd.multichannel.message.send",
        "time": "2026-01-01T00:00:00Z",
        "data": {
            "direction": "out",
            "provider": provider,
            "provider_message_id": str(uuid4()),
            "from_": {"address": "noreply@cobd.ca"},
            "to": [{"person_id": person_id, "address": "user@example.com"}],
            "subject": "hi",
            "text": "test",
        },
    }


def _make_client(settings: Settings, rate_limiter: RateLimiter | None) -> TestClient:
    store = MemoryStore()
    database = FakeDatabase(store)
    notaio = AsyncMock()
    notaio.record = AsyncMock()
    notaio.close = AsyncMock()
    medici = MediciFake()
    app = FastAPI()
    app.state.multichannel = AppState(
        settings=settings,
        database=database,  # type: ignore[arg-type]
        notaio=notaio,
        medici=medici,
        redis=DummyRedis(settings.REDIS_URL),
        rate_limiter=rate_limiter,
    )

    async def fake_session_dep() -> AsyncIterator:
        async with database.session_maker() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    app.dependency_overrides[session_dep] = fake_session_dep
    app.include_router(router)
    return TestClient(app)


@pytest.fixture()
async def fakeredis_limiter() -> RateLimiter:
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    return RateLimiter(client)


def test_429_returned_when_actor_bucket_exhausted(
    fakeredis_limiter: RateLimiter,
) -> None:
    settings = Settings(
        RATE_LIMIT_ENABLED=True,
        RATE_LIMIT_ACTOR_PER_MIN=3,
        RATE_LIMIT_POSTMARK_PER_MIN=300,
        POSTMARK_WEBHOOK_SECRET="x",
        META_APP_SECRET="x",
        META_VERIFY_TOKEN="x",
        SIGNALWIRE_SIDECAR_HMAC="x",
    )
    client = _make_client(settings, fakeredis_limiter)
    headers = {
        "X-Actor-Id": "actor-r1",
        "X-Actor-Type": "user",
        "X-Purpose": "transactional",
    }
    for i in range(3):
        r = client.post("/api/v1/outbound", json=_outbound_event(), headers=headers)
        assert r.status_code == 202, f"call {i} expected 202, got {r.status_code}: {r.text}"
    r = client.post("/api/v1/outbound", json=_outbound_event(), headers=headers)
    assert r.status_code == 429
    assert r.headers.get("Retry-After") is not None
    body = r.json()
    assert body["error"] == "rate_limited"
    assert body["scope"] == "actor"
    assert body["retry_after_seconds"] > 0


def test_429_returned_when_provider_bucket_exhausted(
    fakeredis_limiter: RateLimiter,
) -> None:
    settings = Settings(
        RATE_LIMIT_ENABLED=True,
        RATE_LIMIT_ACTOR_PER_MIN=300,
        RATE_LIMIT_POSTMARK_PER_MIN=2,
        POSTMARK_WEBHOOK_SECRET="x",
        META_APP_SECRET="x",
        META_VERIFY_TOKEN="x",
        SIGNALWIRE_SIDECAR_HMAC="x",
    )
    client = _make_client(settings, fakeredis_limiter)
    # Different actors so the actor bucket doesn't trip.
    for i in range(2):
        r = client.post(
            "/api/v1/outbound",
            json=_outbound_event(),
            headers={
                "X-Actor-Id": f"actor-p{i}",
                "X-Actor-Type": "user",
                "X-Purpose": "transactional",
            },
        )
        assert r.status_code == 202
    r = client.post(
        "/api/v1/outbound",
        json=_outbound_event(),
        headers={
            "X-Actor-Id": "actor-p99",
            "X-Actor-Type": "user",
            "X-Purpose": "transactional",
        },
    )
    assert r.status_code == 429
    assert r.json()["scope"] == "provider"


def test_disabled_limiter_never_429s() -> None:
    settings = Settings(
        RATE_LIMIT_ENABLED=False,
        RATE_LIMIT_ACTOR_PER_MIN=1,
        POSTMARK_WEBHOOK_SECRET="x",
        META_APP_SECRET="x",
        META_VERIFY_TOKEN="x",
        SIGNALWIRE_SIDECAR_HMAC="x",
    )
    client = _make_client(settings, rate_limiter=None)
    headers = {
        "X-Actor-Id": "actor-d1",
        "X-Actor-Type": "user",
        "X-Purpose": "transactional",
    }
    for _ in range(5):
        r = client.post("/api/v1/outbound", json=_outbound_event(), headers=headers)
        assert r.status_code == 202
