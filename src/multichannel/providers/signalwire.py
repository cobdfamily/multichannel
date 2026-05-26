"""SignalWire provider adapter."""

from __future__ import annotations

import httpx

from multichannel.config import Settings
from multichannel.models import Message


async def send(message: Message, settings: Settings) -> dict[str, str]:
    to_addr = message.to_addrs[0].get("phone") or message.to_addrs[0].get("address")
    from_addr = message.from_addr.get("phone") or settings.SIGNALWIRE_FROM_NUMBER
    base_url = settings.SIGNALWIRE_SPACE_URL.rstrip("/")
    url = (
        f"{base_url}/api/laml/2010-04-01/Accounts/"
        f"{settings.SIGNALWIRE_PROJECT_ID}/Messages.json"
    )
    data: dict[str, str] = {
        "From": from_addr,
        "To": to_addr,
        "Body": message.text_body or "",
    }
    media_urls = [a["url"] for a in (message.attachments or []) if a.get("url")]
    if media_urls:
        data["MediaUrl"] = media_urls[0]
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(
            url,
            auth=(settings.SIGNALWIRE_PROJECT_ID, settings.SIGNALWIRE_AUTH_TOKEN),
            data=data,
        )
        resp.raise_for_status()
        payload = resp.json()
    return {
        "provider_message_id": str(payload.get("sid") or payload.get("Sid") or ""),
        "status": "dispatched",
    }
