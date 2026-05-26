"""Test fixtures.

Docker-backed testcontainers are not usable in this sandbox, and adding
aiosqlite would require network access. Route tests therefore use a small
in-memory async session fake that preserves the contracts the API layer needs.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.sql.elements import BinaryExpression

from multichannel.api import router
from multichannel.config import Settings
from multichannel.models import EventOutbox, Message, OutboxItem
from multichannel.runtime import AppState, session_dep
from multichannel.services.redis_publisher import RedisStreamPublisher


class DummyRedis(RedisStreamPublisher):
    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None


@dataclass
class MemoryStore:
    messages: list[Message] = field(default_factory=list)
    outbox_items: list[OutboxItem] = field(default_factory=list)
    events: list[EventOutbox] = field(default_factory=list)


class FakeScalarResult:
    def __init__(self, rows: list) -> None:
        self._rows = rows

    def all(self) -> list:
        return self._rows


class FakeSession:
    def __init__(self, store: MemoryStore) -> None:
        self.store = store

    def add(self, row) -> None:  # noqa: ANN001
        if getattr(row, "id", None) is None:
            import uuid

            row.id = uuid.uuid4()
        if isinstance(row, Message):
            self.store.messages.append(row)
        elif isinstance(row, OutboxItem):
            self.store.outbox_items.append(row)
        elif isinstance(row, EventOutbox):
            self.store.events.append(row)

    async def flush(self) -> None:
        return None

    async def commit(self) -> None:
        return None

    async def rollback(self) -> None:
        return None

    async def scalar(self, statement) -> object | None:  # noqa: ANN001
        statement_text = str(statement)
        if "count(" in statement_text:
            if "event_outbox" in statement_text:
                return len(self.store.events)
            if "outbox_items" in statement_text:
                return len(self.store.outbox_items)
            return len(self.store.messages)

        provider = None
        provider_message_id = None
        for criterion in getattr(statement, "_where_criteria", ()):
            if isinstance(criterion, BinaryExpression):
                name = getattr(criterion.left, "name", None)
                value = getattr(criterion.right, "value", None)
                if name == "provider":
                    provider = getattr(value, "value", value)
                if name == "provider_message_id":
                    provider_message_id = value
        for row in self.store.messages:
            row_provider = getattr(row.provider, "value", row.provider)
            if row_provider == provider and row.provider_message_id == provider_message_id:
                return row.id
        if provider_message_id is not None:
            return None
        entity = None
        descriptions = getattr(statement, "column_descriptions", None) or []
        if descriptions:
            entity = descriptions[0].get("entity")
        rows: list = []
        if entity is Message:
            rows = list(self.store.messages)
        elif entity is EventOutbox:
            rows = list(self.store.events)
        elif entity is OutboxItem:
            rows = list(self.store.outbox_items)
        if provider is not None:
            rows = [
                row
                for row in rows
                if getattr(getattr(row, "provider", None), "value", getattr(row, "provider", None))
                == provider
            ]
        return rows[0] if rows else None

    async def get(self, model, row_id):  # noqa: ANN001, ANN201
        if model is Message:
            for row in self.store.messages:
                if row.id == row_id:
                    return row
        return None

    async def scalars(self, statement) -> FakeScalarResult:  # noqa: ANN001
        return FakeScalarResult(list(self.store.messages))


class FakeDatabase:
    def __init__(self, store: MemoryStore) -> None:
        self.store = store

    @asynccontextmanager
    async def session_maker(self) -> AsyncIterator[FakeSession]:
        session = FakeSession(self.store)
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise

    async def close(self) -> None:
        return None


@pytest.fixture()
def settings() -> Settings:
    return Settings(
        DATABASE_URL="sqlite+memory://",
        POSTMARK_WEBHOOK_SECRET="postmark-secret",
        META_APP_SECRET="meta-secret",
        META_VERIFY_TOKEN="verify-token",
        SIGNALWIRE_SIDECAR_HMAC="sidecar-secret",
    )


@pytest.fixture()
def database() -> FakeDatabase:
    return FakeDatabase(MemoryStore())


@pytest.fixture()
def notaio() -> AsyncMock:
    client = AsyncMock()
    client.record = AsyncMock()
    client.close = AsyncMock()
    return client


class MediciFake:
    def __init__(self) -> None:
        self.check_consent = AsyncMock(return_value=True)
        self.close = AsyncMock()

    @property
    def allowed(self) -> bool:
        return bool(self.check_consent.return_value)

    @allowed.setter
    def allowed(self, value: bool) -> None:
        self.check_consent.return_value = value


@pytest.fixture()
def medici() -> MediciFake:
    return MediciFake()


@pytest.fixture()
def test_client(settings: Settings, database: FakeDatabase, notaio: AsyncMock, medici: AsyncMock) -> Iterator[TestClient]:
    app = FastAPI()
    app.state.multichannel = AppState(
        settings=settings,
        database=database,  # type: ignore[arg-type]
        notaio=notaio,
        medici=medici,
        redis=DummyRedis(settings.REDIS_URL),
    )

    async def fake_session_dep() -> AsyncIterator[FakeSession]:
        async with database.session_maker() as session:
            yield session

    app.dependency_overrides[session_dep] = fake_session_dep
    app.include_router(router)
    with TestClient(app) as client:
        yield client


@pytest.fixture()
def app(settings: Settings, database: FakeDatabase, notaio: AsyncMock, medici: MediciFake) -> FastAPI:
    app = FastAPI()
    app.state.multichannel = AppState(
        settings=settings,
        database=database,  # type: ignore[arg-type]
        notaio=notaio,
        medici=medici,  # type: ignore[arg-type]
        redis=DummyRedis(settings.REDIS_URL),
    )

    async def fake_session_dep() -> AsyncIterator[FakeSession]:
        async with database.session_maker() as session:
            yield session

    app.dependency_overrides[session_dep] = fake_session_dep
    app.include_router(router)
    return app


@pytest.fixture()
async def client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest.fixture()
async def db_session(database: FakeDatabase) -> AsyncIterator[FakeSession]:
    async with database.session_maker() as session:
        yield session


def db_count(database: FakeDatabase) -> int:
    return len(database.store.messages)


def db_messages(database: FakeDatabase) -> list[Message]:
    return list(database.store.messages)
