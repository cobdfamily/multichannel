"""Helpers for outbound idempotency keys."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from hashlib import sha256

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from multichannel.models import IdempotencyKey

EXPIRY_HOURS = 24


def compute_fingerprint(body: bytes) -> str:
    return sha256(body).hexdigest()


async def claim_key(
    session: AsyncSession,
    key: str,
    actor_id: str,
    fingerprint: str,
) -> tuple[IdempotencyKey | None, bool]:
    existing = await session.scalar(
        select(IdempotencyKey).where(
            IdempotencyKey.actor_id == actor_id,
            IdempotencyKey.key == key,
        )
    )
    if existing is not None:
        return existing, False

    now = datetime.now(tz=UTC)
    row = IdempotencyKey(
        key=key,
        actor_id=actor_id,
        request_fingerprint=fingerprint,
        created_at=now,
        expires_at=now + timedelta(hours=EXPIRY_HOURS),
    )
    session.add(row)
    try:
        await session.flush()
    except IntegrityError:
        await session.rollback()
        existing = await session.scalar(
            select(IdempotencyKey).where(
                IdempotencyKey.actor_id == actor_id,
                IdempotencyKey.key == key,
            )
        )
        return existing, False
    return row, True
