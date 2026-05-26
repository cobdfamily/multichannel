"""Runtime container + FastAPI dependency providers."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

from fastapi import Header, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession

from multichannel.config import Settings
from multichannel.db import Database
from multichannel.services.medici_client import MediciClient
from multichannel.services.notaio_client import NotaioClient
from multichannel.services.redis_publisher import RedisStreamPublisher


@dataclass
class Actor:
    actor_id: str
    actor_type: str


@dataclass
class AppState:
    settings: Settings
    database: Database
    notaio: NotaioClient
    medici: MediciClient
    redis: RedisStreamPublisher
    outbox_task: asyncio.Task | None = field(default=None)
    event_outbox_task: asyncio.Task | None = field(default=None)


def state(request: Request) -> AppState:
    return request.app.state.multichannel  # type: ignore[no-any-return]


async def session_dep(request: Request) -> AsyncIterator[AsyncSession]:
    db = state(request).database
    async with db.session_maker() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def actor_dep(
    x_actor_id: str | None = Header(default=None, alias="X-Actor-Id"),
    x_actor_type: str | None = Header(default=None, alias="X-Actor-Type"),
) -> Actor:
    if not x_actor_id or not x_actor_type:
        raise HTTPException(
            status_code=401,
            detail={
                "error": {
                    "code": "actor_required",
                    "message": "X-Actor-Id and X-Actor-Type headers are required.",
                }
            },
        )
    return Actor(actor_id=x_actor_id, actor_type=x_actor_type)


async def purpose_dep(
    x_purpose: str | None = Header(default=None, alias="X-Purpose"),
    required: bool = False,
) -> str | None:
    if required and not x_purpose:
        raise HTTPException(
            status_code=400,
            detail={
                "error": {
                    "code": "purpose_required",
                    "message": "X-Purpose header is required.",
                }
            },
        )
    return x_purpose


async def required_purpose_dep(
    x_purpose: str | None = Header(default=None, alias="X-Purpose"),
) -> str:
    purpose = await purpose_dep(x_purpose=x_purpose, required=True)
    return purpose or ""


def settings_dep(request: Request) -> Settings:
    return state(request).settings


def notaio_dep(request: Request) -> NotaioClient:
    return state(request).notaio


def medici_dep(request: Request) -> MediciClient:
    return state(request).medici


def redis_dep(request: Request) -> RedisStreamPublisher:
    return state(request).redis
