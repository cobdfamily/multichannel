"""Delivery-status webhook routes.

Providers POST status updates (delivered, bounced, opened/read) for
messages we previously sent. Each provider's payload shape differs;
this module normalises them into status changes on the original
Message row + a `cobd.multichannel.delivery.status` CloudEvent.

Idempotent: if we don't recognise the provider_message_id (e.g. the
message was sent by another tenant, or the webhook arrives before
the Message row is committed), we return 200 with `{"status":
"unknown_message"}`. Providers don't retry on 2xx; that keeps the
queue clear of "unknown" payloads we can't act on.
"""

from __future__ import annotations

import hashlib
import hmac
from datetime import UTC, datetime
from typing import Annotated, Any

import structlog
from fastapi import APIRouter, Depends, Header, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from multichannel.config import Settings
from multichannel.models import EventOutbox, Message, MessageStatus
from multichannel.providers import meta as meta_provider
from multichannel.runtime import notaio_dep, session_dep, settings_dep
from multichannel.schemas.cloudevent import CloudEvent
from multichannel.services.notaio_client import AuditEvent, NotaioClient

router = APIRouter(prefix="/webhook", tags=["delivery-status"])
logger = structlog.get_logger("multichannel.delivery_status")

STATUS_EVENT_TYPE = "cobd.multichannel.delivery.status"


# --- helpers ---------------------------------------------------------------


async def _find_message(
    session: AsyncSession, provider: str, provider_message_id: str,
) -> Message | None:
    stmt = select(Message).where(
        Message.provider == provider,
        Message.provider_message_id == provider_message_id,
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


def _emit_status_event(
    session: AsyncSession,
    message: Message,
    new_status: MessageStatus,
    provider_status: str,
    detail: dict[str, Any] | None = None,
) -> None:
    """Persist an EventOutbox row carrying a status CloudEvent."""
    ev = CloudEvent(
        id=str(message.id),
        source=f"/multichannel/{message.provider}",
        type=STATUS_EVENT_TYPE,
        time=datetime.now(tz=UTC),
        data={
            "message_id": str(message.id),
            "provider": str(message.provider),
            "provider_message_id": message.provider_message_id,
            "conversation_id": str(message.conversation_id) if message.conversation_id else None,
            "status": new_status.value,
            "provider_status": provider_status,
            "detail": detail or {},
        },
    )
    session.add(
        EventOutbox(
            event_type=ev.type,
            event_data=ev.model_dump(mode="json", by_alias=True),
        )
    )


async def _apply_status(
    session: AsyncSession,
    notaio: NotaioClient,
    provider: str,
    provider_message_id: str,
    new_status: MessageStatus,
    provider_status: str,
    detail: dict[str, Any] | None = None,
    delivered_at: datetime | None = None,
) -> dict[str, str]:
    message = await _find_message(session, provider, provider_message_id)
    if message is None:
        logger.info(
            "delivery_status.unknown_message",
            provider=provider,
            provider_message_id=provider_message_id,
        )
        return {"status": "unknown_message"}
    message.status = new_status
    if delivered_at is not None:
        message.delivered_at = delivered_at
    message.status_detail = (detail or {}).get("description")
    _emit_status_event(session, message, new_status, provider_status, detail)
    await notaio.record(
        AuditEvent(
            actor_user_id="service:multichannel",
            action=f"delivery_status.{new_status.value}",
            outcome="success",
            subject=str(message.id),
            metadata={
                "provider": provider,
                "provider_status": provider_status,
                "detail": detail or {},
            },
        )
    )
    return {"status": "updated", "message_id": str(message.id), "new_status": new_status.value}


# --- Postmark --------------------------------------------------------------

# Postmark's outbound webhook posts one of several RecordTypes.
_POSTMARK_RECORD_TO_STATUS: dict[str, MessageStatus] = {
    "Delivery":       MessageStatus.DELIVERED,
    "Bounce":         MessageStatus.BOUNCED,
    "SpamComplaint":  MessageStatus.FAILED,
    "Open":           MessageStatus.DELIVERED,  # already delivered; we just confirm
    "Click":          MessageStatus.DELIVERED,
}


def _verify_postmark_signature(body: bytes, header_value: str, secret: str) -> bool:
    """Postmark uses HMAC-SHA1 base64."""
    if not header_value or not secret:
        return False
    import base64
    digest = hmac.new(secret.encode(), body, hashlib.sha1).digest()
    expected = base64.b64encode(digest).decode()
    return hmac.compare_digest(expected, header_value)


@router.post("/postmark/status")
async def postmark_status(
    request: Request,
    session: Annotated[AsyncSession, Depends(session_dep)],
    notaio: Annotated[NotaioClient, Depends(notaio_dep)],
    settings: Annotated[Settings, Depends(settings_dep)],
    x_postmark_signature: Annotated[str | None, Header(alias="X-Postmark-Signature")] = None,
) -> dict[str, str]:
    body = await request.body()
    if not _verify_postmark_signature(body, x_postmark_signature or "", settings.POSTMARK_WEBHOOK_SECRET):
        raise HTTPException(status_code=401, detail={"error": "bad_signature"})
    payload = await request.json()
    record_type = payload.get("RecordType")
    new_status = _POSTMARK_RECORD_TO_STATUS.get(record_type)
    if new_status is None:
        return {"status": "ignored", "record_type": str(record_type)}
    provider_message_id = payload.get("MessageID") or ""
    delivered_at = None
    if new_status == MessageStatus.DELIVERED and payload.get("DeliveredAt"):
        delivered_at = datetime.fromisoformat(payload["DeliveredAt"].replace("Z", "+00:00"))
    return await _apply_status(
        session,
        notaio,
        provider="postmark",
        provider_message_id=str(provider_message_id),
        new_status=new_status,
        provider_status=str(record_type),
        detail={
            "description":
                payload.get("Description") or payload.get("Details"),
            "type": payload.get("Type"),
        },
        delivered_at=delivered_at,
    )


# --- SignalWire (via sidecar) ----------------------------------------------


def _verify_sidecar_hmac(body: bytes, header_value: str, secret: str) -> bool:
    if not header_value or not secret:
        return False
    expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, header_value)


_SIGNALWIRE_STATUS_TO_OURS: dict[str, MessageStatus] = {
    "delivered":  MessageStatus.DELIVERED,
    "sent":       MessageStatus.DISPATCHED,
    "failed":     MessageStatus.FAILED,
    "undelivered": MessageStatus.BOUNCED,
}


@router.post("/signalwire/status")
async def signalwire_status(
    request: Request,
    session: Annotated[AsyncSession, Depends(session_dep)],
    notaio: Annotated[NotaioClient, Depends(notaio_dep)],
    settings: Annotated[Settings, Depends(settings_dep)],
    x_cobd_sidecar_hmac: Annotated[
        str | None, Header(alias="X-COBD-Sidecar-HMAC")
    ] = None,
) -> dict[str, str]:
    body = await request.body()
    if not _verify_sidecar_hmac(
        body, x_cobd_sidecar_hmac or "", settings.SIGNALWIRE_SIDECAR_HMAC,
    ):
        raise HTTPException(status_code=401, detail={"error": "bad_signature"})
    payload = await request.json()
    provider_message_id = payload.get("provider_message_id") or ""
    sw_status = (payload.get("status") or "").lower()
    new_status = _SIGNALWIRE_STATUS_TO_OURS.get(sw_status)
    if new_status is None:
        return {"status": "ignored", "provider_status": sw_status}
    delivered_at = None
    if new_status == MessageStatus.DELIVERED:
        delivered_at = datetime.now(tz=UTC)
    return await _apply_status(
        session,
        notaio,
        provider="signalwire",
        provider_message_id=str(provider_message_id),
        new_status=new_status,
        provider_status=sw_status,
        detail={"description": payload.get("error_message")},
        delivered_at=delivered_at,
    )


# --- Meta (FB Messenger + Instagram) ---------------------------------------
# Meta sends delivery + read events on the SAME webhook URL as inbound
# messages. The entry.messaging[i] item has a `delivery` or `read` key
# instead of `message`. We expose dedicated routes here for ops clarity
# (Meta lets you configure multiple webhook subscriptions), but the
# implementation is shared.

def _meta_extract_status_events(
    payload: dict[str, Any], provider: str,
) -> list[dict[str, Any]]:
    """Walk Meta's entry/messaging array. Return per-message updates."""
    out: list[dict[str, Any]] = []
    for entry in payload.get("entry") or []:
        for msg in entry.get("messaging") or []:
            # FB Messenger sends delivery + read at the messaging level.
            # delivery: {mids: [...], watermark: ...}
            # read:     {watermark: ...}
            if "delivery" in msg:
                for mid in msg["delivery"].get("mids", []):
                    out.append(
                        {
                            "provider_message_id": str(mid),
                            "kind": "delivered",
                            "watermark": msg["delivery"].get("watermark"),
                        }
                    )
            if "read" in msg:
                # FB doesn't tell us which specific mid was read; the
                # watermark indicates "everything up to this time".
                # We record the watermark on the most recent message
                # to that thread; caller can refine if needed.
                out.append(
                    {
                        "provider_message_id": None,
                        "thread_id": str((msg.get("sender") or {}).get("id") or ""),
                        "kind": "read",
                        "watermark": msg["read"].get("watermark"),
                    }
                )
    return out


@router.post("/meta/status/{provider}")
async def meta_status(
    provider: str,
    request: Request,
    session: Annotated[AsyncSession, Depends(session_dep)],
    notaio: Annotated[NotaioClient, Depends(notaio_dep)],
    settings: Annotated[Settings, Depends(settings_dep)],
    x_hub_signature_256: Annotated[
        str | None, Header(alias="X-Hub-Signature-256")
    ] = None,
) -> dict[str, str | int]:
    if provider not in ("fbmessenger", "instagram"):
        raise HTTPException(status_code=404, detail={"error": "unknown_provider"})
    body = await request.body()
    if not meta_provider.verify_hmac_sha256(
        body, x_hub_signature_256 or "", settings.META_APP_SECRET,
    ):
        raise HTTPException(status_code=401, detail={"error": "bad_signature"})
    payload = await request.json()
    events = _meta_extract_status_events(payload, provider)
    applied = 0
    for ev in events:
        mid = ev.get("provider_message_id")
        kind = ev["kind"]
        if kind == "delivered" and mid:
            await _apply_status(
                session,
                notaio,
                provider=provider,
                provider_message_id=mid,
                new_status=MessageStatus.DELIVERED,
                provider_status=kind,
                detail={"watermark": ev.get("watermark")},
                delivered_at=datetime.now(tz=UTC),
            )
            applied += 1
        elif kind == "read":
            # Bulk "all messages in thread <= watermark are read".
            # No per-message id; we mark the most recent dispatched
            # message in that thread.
            thread = ev.get("thread_id")
            if not thread:
                continue
            stmt = (
                select(Message)
                .where(
                    Message.provider == provider,
                    Message.provider_thread_id == thread,
                    Message.direction == "out",
                )
                .order_by(Message.created_at.desc())
                .limit(1)
            )
            result = await session.execute(stmt)
            message = result.scalar_one_or_none()
            if message is None:
                continue
            message.status_detail = f"read_watermark={ev.get('watermark')}"
            _emit_status_event(
                session,
                message,
                MessageStatus.DELIVERED,
                "read",
                {"watermark": ev.get("watermark")},
            )
            applied += 1
    return {"status": "ok", "events_applied": applied}
