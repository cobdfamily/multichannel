"""Provider dispatch routing."""

from __future__ import annotations

from multichannel.config import Settings, get_settings
from multichannel.models import Message


class DispatchError(Exception):
    """Raised when a provider adapter cannot dispatch a message."""


async def dispatch_message(
    message: Message,
    settings: Settings | None = None,
) -> dict[str, str]:
    settings = settings or get_settings()
    provider = getattr(message.provider, "value", str(message.provider))
    try:
        if provider == "postmark":
            from multichannel.providers import postmark

            return await postmark.send(message, settings)
        if provider == "signalwire":
            from multichannel.providers import signalwire

            return await signalwire.send(message, settings)
        if provider in {"fbmessenger", "instagram"}:
            from multichannel.providers import meta

            return await meta.send(message, settings, provider=provider)
    except Exception as exc:  # noqa: BLE001
        raise DispatchError(str(exc)) from exc
    raise DispatchError(f"unsupported provider: {provider}")
