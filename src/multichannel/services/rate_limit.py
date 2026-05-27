"""Token-bucket rate limiter backed by Redis.

Two buckets per /outbound request:
* per-actor (X-Actor-Id)        — caps a single caller's burst
* per-provider                  — caps total send rate per
                                  provider so a provider account
                                  doesn't get suspended for
                                  exceeding its own limits

The bucket math is the classic token-bucket: a key holds
`(tokens_remaining, last_refill_ts)` in a Redis hash. On each
check() we first refill `(now - last_refill_ts) * refill_rate`
tokens up to `capacity`, then try to take 1 token. If 1 is
available, the call is allowed and tokens decrement by 1. If
not, the call is denied and we return how long the caller
should wait for the next token to refill.

Atomicity matters: two concurrent requests must not both see
"1 token left" and both succeed. We use a small Lua script
executed server-side (Redis EVAL) so the read-modify-write
runs as one operation.
"""

from __future__ import annotations

import time

from redis.asyncio import Redis

# Lua: KEYS[1]=hash key; ARGV[1]=capacity; ARGV[2]=refill_per_sec;
# ARGV[3]=now (float seconds). Returns {allowed (0/1), retry_after_seconds}.
_BUCKET_SCRIPT = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local bucket = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(bucket[1])
local ts = tonumber(bucket[2])

if tokens == nil then
    tokens = capacity
    ts = now
end

local elapsed = math.max(0, now - ts)
tokens = math.min(capacity, tokens + elapsed * refill)

local allowed = 0
local retry_after = 0
if tokens >= 1 then
    tokens = tokens - 1
    allowed = 1
else
    -- how long until we have 1 token?
    retry_after = (1 - tokens) / refill
end

redis.call('HSET', key, 'tokens', tokens, 'ts', now)
-- Expire the key after 2x the time to refill a full bucket.
-- Idle buckets get GC'd; active ones get refreshed.
redis.call('EXPIRE', key, math.ceil(2 * capacity / refill))

return {allowed, tostring(retry_after)}
"""


class RateLimiter:
    """Token-bucket limiter on top of Redis."""

    def __init__(self, redis: Redis) -> None:
        self._redis = redis
        self._script_sha: str | None = None

    async def _ensure_script(self) -> str:
        if self._script_sha is None:
            self._script_sha = await self._redis.script_load(_BUCKET_SCRIPT)
        return self._script_sha

    async def check(
        self,
        key: str,
        capacity: int,
        refill_per_second: float,
    ) -> tuple[bool, float]:
        """Try to take one token from the bucket.

        Returns (allowed, retry_after_seconds). When allowed,
        retry_after is 0.
        """
        full_key = f"mc:ratelimit:{key}"
        now = time.time()
        sha = await self._ensure_script()
        try:
            result = await self._redis.evalsha(
                sha, 1, full_key, str(capacity),
                str(refill_per_second), str(now),
            )
        except Exception:  # script flushed; reload + retry once
            self._script_sha = None
            sha = await self._ensure_script()
            result = await self._redis.evalsha(
                sha, 1, full_key, str(capacity),
                str(refill_per_second), str(now),
            )
        allowed_raw, retry_raw = result
        return (int(allowed_raw) == 1, float(retry_raw))
