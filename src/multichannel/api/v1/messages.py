"""Message read endpoints."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from multichannel.models import Message
from multichannel.runtime import session_dep
from multichannel.schemas.cloudevent import CloudEvent

router = APIRouter(prefix="/messages", tags=["messages"])


@router.get("/{message_id}")
async def get_message(
    message_id: UUID,
    session: Annotated[AsyncSession, Depends(session_dep)],
) -> dict:
    message = await session.get(Message, message_id)
    if message is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail={"error": {"code": "not_found"}})
    event = (
        CloudEvent.received(message)
        if message.direction.value == "in"
        else CloudEvent(
            id=str(message.id),
            source=f"/multichannel/{message.provider.value}",
            type="cobd.multichannel.message.queued",
            time=message.created_at,
            data=CloudEvent.received(message).data,
        )
    )
    return event.model_dump(mode="json", by_alias=True)


@router.get("")
async def list_messages(
    session: Annotated[AsyncSession, Depends(session_dep)],
    conversation_id: Annotated[UUID, Query()],
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> dict[str, list[dict]]:
    rows = (
        await session.scalars(
            select(Message)
            .where(Message.conversation_id == conversation_id)
            .order_by(Message.created_at.desc())
            .limit(limit)
            .offset(offset)
        )
    ).all()
    return {
        "items": [
            CloudEvent.received(row).model_dump(mode="json", by_alias=True)
            if row.direction.value == "in"
            else CloudEvent(
                id=str(row.id),
                source=f"/multichannel/{row.provider.value}",
                type="cobd.multichannel.message.queued",
                time=row.created_at,
                data=CloudEvent.received(row).data,
            ).model_dump(mode="json", by_alias=True)
            for row in rows
        ]
    }
