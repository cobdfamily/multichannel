from uuid import UUID

import httpx
import pytest

from multichannel.config import Settings
from multichannel.services.medici_client import MediciClient, provider_to_channel

PERSON_ID = UUID("11111111-1111-1111-1111-111111111111")


def settings() -> Settings:
    return Settings(
        DATABASE_URL="postgresql+asyncpg://user:pass@localhost/test",
        MEDICI_URL="https://medici.example",
        MEDICI_CLIENT_SECRET="medici-secret",
    )


@pytest.mark.asyncio
async def test_check_consent_granted_posts_correct_request(httpx_mock) -> None:
    httpx_mock.add_response(
        method="GET",
        url=f"https://medici.example/api/v1/persons/{PERSON_ID}/consent?purpose=care&channel=email",
        status_code=200,
        json={"status": "granted"},
    )
    client = MediciClient(settings())

    assert await client.check_consent(PERSON_ID, "care", "email") is True
    await client.close()

    request = httpx_mock.get_request()
    assert request is not None
    assert request.headers["Authorization"] == "Bearer medici-secret"


@pytest.mark.asyncio
async def test_check_consent_false_for_missing_or_revoked(httpx_mock) -> None:
    httpx_mock.add_response(
        method="GET",
        url=f"https://medici.example/api/v1/persons/{PERSON_ID}/consent?purpose=care&channel=sms",
        status_code=404,
    )
    httpx_mock.add_response(
        method="GET",
        url=f"https://medici.example/api/v1/persons/{PERSON_ID}/consent?purpose=care&channel=email",
        status_code=200,
        json={"status": "revoked"},
    )
    client = MediciClient(settings())

    assert await client.check_consent(PERSON_ID, "care", "sms") is False
    assert await client.check_consent(PERSON_ID, "care", "email") is False
    await client.close()


@pytest.mark.asyncio
async def test_check_consent_raises_on_5xx(httpx_mock) -> None:
    httpx_mock.add_response(
        method="GET",
        url=f"https://medici.example/api/v1/persons/{PERSON_ID}/consent?purpose=care&channel=email",
        status_code=503,
    )
    client = MediciClient(settings())

    with pytest.raises(httpx.HTTPStatusError):
        await client.check_consent(PERSON_ID, "care", "email")
    await client.close()


def test_provider_to_channel() -> None:
    assert provider_to_channel("postmark") == "email"
    assert provider_to_channel("signalwire") == "sms"
    assert provider_to_channel("fbmessenger") == "messenger"
    assert provider_to_channel("instagram") == "instagram"
