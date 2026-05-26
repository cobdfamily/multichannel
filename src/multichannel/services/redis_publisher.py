"""Redis Streams publisher for CloudEvents."""

from __future__ import annotations

import redis.asyncio as redis

from multichannel.schemas.cloudevent import CloudEvent

EVENT_STREAM_KEY = "multichannel:events"


class RedisStreamPublisher:
    def __init__(self, redis_url: str) -> None:
        self._redis_url = redis_url
        self._client: redis.Redis | None = None

    async def connect(self) -> None:
        if self._client is None:
            self._client = redis.from_url(self._redis_url, decode_responses=True)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def publish(self, event: CloudEvent) -> str:
        if self._client is None:
            await self.connect()
        assert self._client is not None
        return await self._client.xadd(
            EVENT_STREAM_KEY,
            {
                "event": event.model_dump_json(by_alias=True),
                "type": event.type,
            },
        )
