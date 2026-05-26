from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest

from multichannel.services.redis_publisher import EVENT_STREAM_KEY, RedisStreamPublisher
from multichannel.schemas.cloudevent import CloudEvent


@pytest.mark.asyncio
async def test_publish_writes_event_and_type_to_stream() -> None:
    redis_client = AsyncMock()
    redis_client.xadd.return_value = "123-0"
    event = CloudEvent(
        id="event-1",
        source="/multichannel/postmark",
        type="cobd.multichannel.message.received",
        time=datetime(2026, 5, 26, tzinfo=UTC),
        data={"message_id": "message-1"},
    )

    with patch(
        "multichannel.services.redis_publisher.redis.from_url",
        return_value=redis_client,
    ) as from_url:
        publisher = RedisStreamPublisher("redis://localhost:6379/0")
        entry_id = await publisher.publish(event)

    assert entry_id == "123-0"
    from_url.assert_called_once_with("redis://localhost:6379/0", decode_responses=True)
    redis_client.xadd.assert_awaited_once()
    stream, fields = redis_client.xadd.await_args.args
    assert stream == EVENT_STREAM_KEY
    assert fields["type"] == "cobd.multichannel.message.received"
    assert '"id":"event-1"' in fields["event"]
