"""Unit tests for the token-bucket RateLimiter against fakeredis."""

from __future__ import annotations

import asyncio

import fakeredis.aioredis
import pytest

from multichannel.services.rate_limit import RateLimiter


@pytest.fixture
async def limiter() -> RateLimiter:
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    return RateLimiter(client)


async def test_first_n_calls_allowed(limiter: RateLimiter) -> None:
    capacity = 5
    refill = capacity / 60.0  # 5/min
    for _ in range(capacity):
        allowed, retry = await limiter.check("test:1", capacity, refill)
        assert allowed is True
        assert retry == 0.0


async def test_n_plus_one_denied(limiter: RateLimiter) -> None:
    capacity = 3
    refill = capacity / 60.0
    for _ in range(capacity):
        await limiter.check("test:2", capacity, refill)
    allowed, retry = await limiter.check("test:2", capacity, refill)
    assert allowed is False
    assert retry > 0


async def test_refill_lets_next_call_through(limiter: RateLimiter) -> None:
    capacity = 1
    refill = 10.0  # 1 token / 0.1s
    allowed, _ = await limiter.check("test:3", capacity, refill)
    assert allowed is True
    allowed, _ = await limiter.check("test:3", capacity, refill)
    assert allowed is False
    await asyncio.sleep(0.15)
    allowed, _ = await limiter.check("test:3", capacity, refill)
    assert allowed is True


async def test_distinct_keys_are_isolated(limiter: RateLimiter) -> None:
    capacity = 1
    refill = capacity / 60.0
    assert (await limiter.check("test:a", capacity, refill))[0] is True
    assert (await limiter.check("test:a", capacity, refill))[0] is False
    # Other key still has its full bucket.
    assert (await limiter.check("test:b", capacity, refill))[0] is True
