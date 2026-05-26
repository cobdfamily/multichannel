"""Outbound message enqueue endpoint.

Callers may send ``Idempotency-Key: <uuid>`` to safely retry POST /outbound.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from multichannel.lib.conversation_id import derive_conversation_id
from multichannel.lib.idempotency import claim_key, compute_fingerprint
from multichannel.models import (
    EventOutbox,
    IdempotencyKey,
    Message,
    MessageDirection,
    MessageProvider,
    MessageStatus,
)
from multichannel.models.outbox_item import OutboxItem
from multichannel.runtime import Actor, actor_dep, medici_dep, notaio_dep, session_dep
from multichannel.schemas.cloudevent import CloudEvent, MessageData
from multichannel.services.medici_client import MediciClient, provider_to_channel
from multichannel.services.notaio_client import AuditEvent, NotaioClient

router = APIRouter(prefix="/outbound", tags=["outbound"])
logger = structlog.get_logger("multichannel.outbound")

SEND_TYPE = "cobd.multichannel.message.send"


def _json_event(event: CloudEvent) -> dict:
    return event.model_dump(mode="json", by_alias=True)


def _recipient_person_id(data: MessageData) -> UUID:
    for recipient in data.to:
        value = recipient.get("person_id") or recipient.get("blind_hub_id") or recipient.get("id")
        if value:
            return UUID(str(value))
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail={"error": {"code": "recipient_person_id_required"}},
    )


async def reap_expired(session: AsyncSession) -> None:
    """Delete expired idempotency rows."""

    # TODO: Wire this into a periodic maintenance worker.
    await session.execute(delete(IdempotencyKey).where(IdempotencyKey.expires_at <= datetime.now(tz=UTC)))


@router.post("", status_code=status.HTTP_202_ACCEPTED)
async def enqueue_outbound(
    request: Request,
    event: CloudEvent,
    session: Annotated[AsyncSession, Depends(session_dep)],
    actor: Annotated[Actor, Depends(actor_dep)],
    notaio: Annotated[NotaioClient, Depends(notaio_dep)],
    medici: Annotated[MediciClient, Depends(medici_dep)],
    x_purpose: Annotated[str | None, Header(alias="X-Purpose")] = None,
    x_skip_consent: Annotated[str | None, Header(alias="X-Skip-Consent")] = None,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> dict[str, str]:
    if event.type != SEND_TYPE:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"error": {"code": "bad_type"}})
    data = event.data if isinstance(event.data, MessageData) else MessageData.model_validate(event.data)
    if data.direction != "out":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": {"code": "bad_direction"}},
        )
    if not x_purpose:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": {"code": "purpose_required"}},
        )

    key_claim: IdempotencyKey | None = None
    if idempotency_key:
        fingerprint = compute_fingerprint(await request.body())
        existing, was_new = await claim_key(session, idempotency_key, actor.actor_id, fingerprint)
        now = datetime.now(tz=UTC)
        if existing is not None and not was_new:
            if existing.expires_at <= now:
                await session.delete(existing)
                await session.flush()
                existing, was_new = await claim_key(session, idempotency_key, actor.actor_id, fingerprint)
            elif existing.request_fingerprint != fingerprint:
                logger.warning(
                    "outbound.idempotency_conflict",
                    actor_id=actor.actor_id,
                    idempotency_key=idempotency_key,
                )
                return JSONResponse(
                    status_code=422,
                    content={
                        "error": "idempotency_conflict",
                        "message": "Idempotency-Key reused with different body",
                    },
                )
            else:
                logger.info(
                    "outbound.idempotency_replay",
                    actor_id=actor.actor_id,
                    idempotency_key=idempotency_key,
                    message_id=str(existing.message_id),
                )
                return JSONResponse(
                    status_code=status.HTTP_200_OK,
                    content={"message_id": str(existing.message_id), "status": "idempotent-replay"},
                )
        if existing is not None and was_new:
            key_claim = existing
    else:
        logger.warning("outbound.no_idempotency_key", actor_id=actor.actor_id)

    skip_consent = (
        (x_skip_consent or "").lower() == "true"
        and actor.actor_type == "service"
        and x_purpose == "transactional-system"
    )
    person_id = _recipient_person_id(data)
    channel = provider_to_channel(data.provider)
    if skip_consent:
        await notaio.record(
            AuditEvent(
                actor_user_id=actor.actor_id,
                action="message.consent.skip",
                outcome="success",
                subject=str(person_id),
                metadata={"provider": data.provider, "purpose": x_purpose, "reason": "service_override"},
            )
        )
    elif not await medici.check_consent(
        person_id=person_id,
        purpose=x_purpose,
        channel=channel,
    ):
        await notaio.record(
            AuditEvent(
                actor_user_id=actor.actor_id,
                action="message.send",
                outcome="denied",
                subject=str(person_id),
                metadata={"provider": data.provider, "purpose": x_purpose, "channel": channel},
            )
        )
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail={"error": {"code": "consent-revoked"}})

    message = Message(
        direction=MessageDirection.OUT,
        provider=MessageProvider(data.provider),
        provider_message_id=data.provider_message_id or event.id,
        provider_thread_id=data.provider_thread_id,
        conversation_id=derive_conversation_id(data.provider, data.provider_thread_id),
        from_addr=data.from_,
        to_addrs=data.to,
        subject=data.subject,
        text_body=data.text,
        html_body=data.html,
        attachments=data.attachments,
        status=MessageStatus.QUEUED,
        raw_payload=_json_event(event),
        requested_purpose=x_purpose,
        requested_by_actor_id=actor.actor_id,
    )
    session.add(message)
    await session.flush()
    if key_claim is not None:
        key_claim.message_id = message.id
        await session.flush()
    session.add(
        OutboxItem(
            message_id=message.id,
            next_attempt_at=datetime.now(tz=UTC),
        )
    )
    queued_event = CloudEvent(
        id=str(message.id),
        source=f"/multichannel/{data.provider}",
        type="cobd.multichannel.message.queued",
        time=datetime.now(tz=UTC),
        data=data,
    )
    session.add(
        EventOutbox(
            event_type=queued_event.type,
            event_data=_json_event(queued_event),
        )
    )
    await notaio.record(
        AuditEvent(
            actor_user_id=actor.actor_id,
            action="message.send",
            outcome="success",
            subject=str(message.id),
            metadata={"provider": data.provider, "purpose": x_purpose},
        )
    )
    return {"message_id": str(message.id), "status": "queued"}
