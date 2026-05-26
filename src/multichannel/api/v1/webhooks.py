"""Provider webhook endpoints."""

from __future__ import annotations

import hashlib
import hmac
from datetime import UTC, datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, Response, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from multichannel.lib.conversation_id import derive_conversation_id
from multichannel.models import EventOutbox, Message, MessageDirection, MessageProvider, MessageStatus
from multichannel.providers import meta, postmark
from multichannel.providers.types import ParsedInbound
from multichannel.runtime import notaio_dep, session_dep, settings_dep
from multichannel.schemas.cloudevent import CloudEvent
from multichannel.services.notaio_client import AuditEvent, NotaioClient
from multichannel.config import Settings

router = APIRouter(prefix="/webhook", tags=["webhooks"])


def _json_event(event: CloudEvent) -> dict[str, Any]:
    return event.model_dump(mode="json", by_alias=True)


async def _duplicate(session: AsyncSession, provider: str, provider_message_id: str) -> bool:
    found = await session.scalar(
        select(Message.id).where(
            Message.provider == MessageProvider(provider),
            Message.provider_message_id == provider_message_id,
        )
    )
    return found is not None


async def _insert_inbound(
    parsed: dict[str, Any] | ParsedInbound,
    session: AsyncSession,
    notaio: NotaioClient,
) -> Message | None:
    if isinstance(parsed, ParsedInbound):
        parsed = {
            "provider": parsed.provider,
            "provider_message_id": parsed.provider_message_id,
            "provider_thread_id": parsed.provider_thread_id,
            "from_addr": parsed.from_addr,
            "to_addrs": parsed.to_addrs,
            "subject": parsed.subject,
            "text_body": parsed.text_body,
            "html_body": parsed.html_body,
            "attachments": parsed.attachments,
            "raw_payload": parsed.raw_payload,
        }
    provider = parsed["provider"]
    provider_message_id = parsed["provider_message_id"]
    if await _duplicate(session, provider, provider_message_id):
        return None
    message = Message(
        direction=MessageDirection.IN,
        provider=MessageProvider(provider),
        provider_message_id=provider_message_id,
        provider_thread_id=parsed.get("provider_thread_id"),
        conversation_id=derive_conversation_id(provider, parsed.get("provider_thread_id")),
        from_addr=parsed.get("from_addr") or {},
        to_addrs=parsed.get("to_addrs") or [],
        subject=parsed.get("subject"),
        text_body=parsed.get("text_body"),
        html_body=parsed.get("html_body"),
        attachments=parsed.get("attachments") or [],
        status=MessageStatus.RECEIVED,
        raw_payload=parsed.get("raw_payload") or {},
        received_at=datetime.now(tz=UTC),
    )
    session.add(message)
    await session.flush()
    event = CloudEvent.received(message)
    session.add(EventOutbox(event_type=event.type, event_data=_json_event(event)))
    await notaio.record(
        AuditEvent(
            actor_user_id="provider-webhook",
            action="message.receive",
            outcome="success",
            subject=str(message.id),
            metadata={"provider": provider, "provider_message_id": provider_message_id},
        )
    )
    return message


@router.post("/postmark")
async def postmark_webhook(
    request: Request,
    session: Annotated[AsyncSession, Depends(session_dep)],
    notaio: Annotated[NotaioClient, Depends(notaio_dep)],
    settings: Annotated[Settings, Depends(settings_dep)],
    x_postmark_signature: Annotated[str | None, Header(alias="X-Postmark-Signature")] = None,
) -> dict[str, Any]:
    body = await request.body()
    if not postmark.verify_hmac(body, x_postmark_signature or "", settings.POSTMARK_WEBHOOK_SECRET):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail={"error": {"code": "bad_signature"}})
    parsed = postmark.parse_inbound(await request.json())
    message = await _insert_inbound(parsed, session, notaio)
    if message is None:
        return {"status": "duplicate"}
    return {"status": "received", "message_id": str(message.id)}


@router.post("/fbmessenger")
async def fbmessenger_webhook(
    request: Request,
    session: Annotated[AsyncSession, Depends(session_dep)],
    notaio: Annotated[NotaioClient, Depends(notaio_dep)],
    settings: Annotated[Settings, Depends(settings_dep)],
    x_hub_signature_256: Annotated[str | None, Header(alias="X-Hub-Signature-256")] = None,
) -> dict[str, Any]:
    body = await request.body()
    if not meta.verify_hmac_sha256(body, x_hub_signature_256 or "", settings.META_APP_SECRET):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail={"error": {"code": "bad_signature"}})
    count = 0
    duplicates = 0
    for parsed in meta.parse_inbound(await request.json(), provider="fbmessenger"):
        if await _insert_inbound(parsed, session, notaio) is not None:
            count += 1
        else:
            duplicates += 1
    if count == 0 and duplicates:
        return {"status": "duplicate"}
    return {"status": "received", "count": count}


@router.post("/instagram")
async def instagram_webhook(
    request: Request,
    session: Annotated[AsyncSession, Depends(session_dep)],
    notaio: Annotated[NotaioClient, Depends(notaio_dep)],
    settings: Annotated[Settings, Depends(settings_dep)],
    x_hub_signature_256: Annotated[str | None, Header(alias="X-Hub-Signature-256")] = None,
) -> dict[str, Any]:
    body = await request.body()
    if not meta.verify_hmac_sha256(body, x_hub_signature_256 or "", settings.META_APP_SECRET):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail={"error": {"code": "bad_signature"}})
    count = 0
    duplicates = 0
    for parsed in meta.parse_inbound(await request.json(), provider="instagram"):
        if await _insert_inbound(parsed, session, notaio) is not None:
            count += 1
        else:
            duplicates += 1
    if count == 0 and duplicates:
        return {"status": "duplicate"}
    return {"status": "received", "count": count}


def _verify_sidecar_hmac(body: bytes, signature: str | None, secret: str) -> bool:
    if not signature or not secret:
        return False
    expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


@router.post("/signalwire")
async def signalwire_webhook(
    request: Request,
    session: Annotated[AsyncSession, Depends(session_dep)],
    notaio: Annotated[NotaioClient, Depends(notaio_dep)],
    settings: Annotated[Settings, Depends(settings_dep)],
    x_cobd_sidecar_hmac: Annotated[str | None, Header(alias="X-COBD-Sidecar-HMAC")] = None,
) -> dict[str, Any]:
    body = await request.body()
    if not _verify_sidecar_hmac(body, x_cobd_sidecar_hmac, settings.SIGNALWIRE_SIDECAR_HMAC):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail={"error": {"code": "bad_signature"}})
    payload = await request.json()
    parsed = {
        "provider": "signalwire",
        "provider_message_id": str(payload.get("provider_message_id") or payload.get("message_id") or payload.get("sid")),
        "provider_thread_id": payload.get("provider_thread_id") or payload.get("from") or payload.get("from_addr", {}).get("phone"),
        "from_addr": payload.get("from_addr") or {"phone": payload.get("from")},
        "to_addrs": payload.get("to_addrs") or [{"phone": payload.get("to")}],
        "subject": None,
        "text_body": payload.get("text_body") or payload.get("body") or payload.get("text"),
        "html_body": None,
        "attachments": payload.get("attachments") or [],
        "raw_payload": payload,
    }
    message = await _insert_inbound(parsed, session, notaio)
    if message is None:
        return {"status": "duplicate"}
    return {"status": "received", "message_id": str(message.id)}


async def _meta_verify(
    settings: Settings,
    hub_mode: str | None,
    hub_verify_token: str | None,
    hub_challenge: str | None,
) -> Response:
    if hub_verify_token == settings.META_VERIFY_TOKEN:
        return Response(content=hub_challenge or "", media_type="text/plain")
    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail={"error": {"code": "bad_verify_token"}})


@router.get("/fbmessenger")
async def fbmessenger_verify(
    settings: Annotated[Settings, Depends(settings_dep)],
    hub_mode: Annotated[str | None, Query(alias="hub.mode")] = None,
    hub_verify_token: Annotated[str | None, Query(alias="hub.verify_token")] = None,
    hub_challenge: Annotated[str | None, Query(alias="hub.challenge")] = None,
) -> Response:
    return await _meta_verify(settings, hub_mode, hub_verify_token, hub_challenge)


@router.get("/instagram")
async def instagram_verify(
    settings: Annotated[Settings, Depends(settings_dep)],
    hub_mode: Annotated[str | None, Query(alias="hub.mode")] = None,
    hub_verify_token: Annotated[str | None, Query(alias="hub.verify_token")] = None,
    hub_challenge: Annotated[str | None, Query(alias="hub.challenge")] = None,
) -> Response:
    return await _meta_verify(settings, hub_mode, hub_verify_token, hub_challenge)
