"""notaio HTTP client.

multichannel writes audit events for message lifecycle and consent-denial
decisions through this client.

Failure mode: notaio outage MUST surface as a service-level error, NOT a
silent skip. If the audit chain cannot be written, the operation must fail.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx
import structlog

from multichannel.config import Settings, get_settings

logger = structlog.get_logger("multichannel.notaio")


@dataclass(frozen=True)
class AuditEvent:
    """The event multichannel sends to notaio."""

    actor_user_id: str
    action: str
    outcome: str  # "success" | "denied" | "error"
    subject: str
    metadata: dict[str, Any]

    def to_payload(self) -> dict[str, Any]:
        return {
            "actor_user_id": self.actor_user_id,
            "action": self.action,
            "outcome": self.outcome,
            "subject": self.subject,
            "metadata": self.metadata,
        }


class NotaioClient:
    """Async client for the notaio REST surface."""

    def __init__(
        self,
        settings: Settings | None = None,
        timeout: float = 5.0,
    ) -> None:
        settings = settings or get_settings()
        self._base_url = settings.NOTAIO_URL.rstrip("/")
        self._client = httpx.AsyncClient(
            base_url=f"{self._base_url}/api/v1",
            timeout=timeout,
            headers={
                "Authorization": f"Bearer {settings.NOTAIO_CLIENT_SECRET}",
            },
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def record(self, event: AuditEvent) -> None:
        """POST /events.

        Raises httpx.HTTPStatusError on non-2xx so the caller can fail the
        in-flight operation. Callers MUST NOT swallow this error silently.
        """
        resp = await self._client.post(
            "/events",
            json=event.to_payload(),
        )
        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError:
            logger.exception(
                "notaio_record_failed",
                action=event.action,
                outcome=event.outcome,
                subject=event.subject,
                status_code=resp.status_code,
            )
            raise
