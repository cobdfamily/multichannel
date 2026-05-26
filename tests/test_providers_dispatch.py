from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import pytest

from multichannel.config import Settings
from multichannel.models import Message, MessageDirection, MessageProvider, MessageStatus
from multichannel.providers.dispatch import DispatchError, dispatch_message


def settings() -> Settings:
    return Settings(
        POSTMARK_SERVER_TOKEN="postmark-token",
        POSTMARK_FROM_EMAIL="sender@example.org",
        SIGNALWIRE_PROJECT_ID="project",
        SIGNALWIRE_AUTH_TOKEN="token",
        SIGNALWIRE_SPACE_URL="https://space.signalwire.com",
        SIGNALWIRE_FROM_NUMBER="+15550000000",
        META_PAGE_ACCESS_TOKEN="page-token",
        INSTAGRAM_ACCESS_TOKEN="ig-token",
        META_GRAPH_API_VERSION="v18.0",
    )


def message(provider: MessageProvider) -> Message:
    return Message(
        id=uuid4(),
        direction=MessageDirection.OUT,
        provider=provider,
        provider_message_id=f"local-{provider.value}",
        provider_thread_id="thread",
        conversation_id=uuid4(),
        from_addr={"email": "sender@example.org", "phone": "+15550000000"},
        to_addrs=[{"email": "user@example.org", "phone": "+15551112222", "id": "psid"}],
        subject="Subject",
        text_body="Body",
        html_body=None,
        attachments=[],
        status=MessageStatus.QUEUED,
        raw_payload={},
    )


@pytest.mark.asyncio
async def test_unknown_provider_dispatch_error():
    with pytest.raises(DispatchError):
        await dispatch_message(SimpleNamespace(provider="unknown"), settings())  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_postmark_send_httpx_mock(httpx_mock):
    httpx_mock.add_response(
        method="POST",
        url="https://api.postmarkapp.com/email",
        json={"MessageID": "pm-dispatched"},
    )
    result = await dispatch_message(message(MessageProvider.POSTMARK), settings())
    assert result == {"provider_message_id": "pm-dispatched", "status": "dispatched"}


@pytest.mark.asyncio
async def test_signalwire_send_httpx_mock(httpx_mock):
    httpx_mock.add_response(
        method="POST",
        url="https://space.signalwire.com/api/laml/2010-04-01/Accounts/project/Messages.json",
        json={"sid": "sw-dispatched"},
    )
    result = await dispatch_message(message(MessageProvider.SIGNALWIRE), settings())
    assert result == {"provider_message_id": "sw-dispatched", "status": "dispatched"}


@pytest.mark.asyncio
async def test_fbmessenger_send_httpx_mock(httpx_mock):
    httpx_mock.add_response(
        method="POST",
        url="https://graph.facebook.com/v18.0/me/messages?access_token=page-token",
        json={"message_id": "fb-dispatched"},
    )
    result = await dispatch_message(message(MessageProvider.FBMESSENGER), settings())
    assert result == {"provider_message_id": "fb-dispatched", "status": "dispatched"}


@pytest.mark.asyncio
async def test_instagram_send_httpx_mock(httpx_mock):
    httpx_mock.add_response(
        method="POST",
        url="https://graph.facebook.com/v18.0/me/messages?access_token=ig-token",
        json={"message_id": "ig-dispatched"},
    )
    result = await dispatch_message(message(MessageProvider.INSTAGRAM), settings())
    assert result == {"provider_message_id": "ig-dispatched", "status": "dispatched"}
