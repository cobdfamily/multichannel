"""medici consent client."""

from __future__ import annotations

from uuid import UUID

import httpx
import structlog

from multichannel.config import Settings, get_settings

logger = structlog.get_logger("multichannel.medici")

PROVIDER_CHANNELS = {
    "postmark": "email",
    "signalwire": "sms",
    "fbmessenger": "messenger",
    "instagram": "instagram",
}


def provider_to_channel(provider: str) -> str:
    return PROVIDER_CHANNELS[provider]


class MediciClient:
    """Async client for outbound consent checks."""

    def __init__(
        self,
        settings: Settings | None = None,
        timeout: float = 5.0,
    ) -> None:
        settings = settings or get_settings()
        self._base_url = settings.MEDICI_URL.rstrip("/")
        self._client = httpx.AsyncClient(
            base_url=f"{self._base_url}/api/v1",
            timeout=timeout,
            headers={
                "Authorization": f"Bearer {settings.MEDICI_CLIENT_SECRET}",
            },
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def check_consent(self, person_id: UUID, purpose: str, channel: str) -> bool:
        resp = await self._client.get(
            f"/persons/{person_id}/consent",
            params={"purpose": purpose, "channel": channel},
        )
        if resp.status_code == 404:
            logger.info(
                "medici_consent_missing",
                person_id=str(person_id),
                purpose=purpose,
                channel=channel,
            )
            return False
        if resp.status_code >= 500:
            logger.error(
                "medici_consent_check_failed",
                person_id=str(person_id),
                purpose=purpose,
                channel=channel,
                status_code=resp.status_code,
            )
            resp.raise_for_status()
        if resp.status_code >= 400:
            return False

        payload = resp.json()
        status = payload.get("status") or payload.get("consent_status")
        if status in {"granted", "active", "allowed"}:
            return True
        if status in {"revoked", "denied", "missing", "none"}:
            return False
        granted = payload.get("granted")
        return bool(granted)
