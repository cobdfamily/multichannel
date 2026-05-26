"""Conversation ID derivation."""

from __future__ import annotations

from uuid import UUID, uuid4, uuid5

CONVERSATION_NS = UUID("a9a55da4-d3e0-4d2d-8d49-1e0e0c6b1aa1")


def derive_conversation_id(provider: str, thread_id: str | None) -> UUID:
    if thread_id is None:
        return uuid4()
    return uuid5(CONVERSATION_NS, f"{provider}:{thread_id}")
