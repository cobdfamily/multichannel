"""Baseline multichannel data layer.

Revision ID: 0001_baseline
Revises:
Create Date: 2026-05-26
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_baseline"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def timestamp_columns() -> list[sa.Column]:
    return [
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    ]


def upgrade() -> None:
    op.create_table(
        "messages",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "direction",
            sa.Enum("in", "out", native_enum=False, length=16),
            nullable=False,
        ),
        sa.Column(
            "provider",
            sa.Enum(
                "postmark",
                "signalwire",
                "fbmessenger",
                "instagram",
                native_enum=False,
                length=32,
            ),
            nullable=False,
        ),
        sa.Column("provider_message_id", sa.String(255), nullable=False),
        sa.Column("provider_thread_id", sa.String(255), nullable=True),
        sa.Column("conversation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("from_addr", postgresql.JSONB(), nullable=False),
        sa.Column("to_addrs", postgresql.JSONB(), nullable=False),
        sa.Column("subject", sa.String(), nullable=True),
        sa.Column("text_body", sa.Text(), nullable=True),
        sa.Column("html_body", sa.Text(), nullable=True),
        sa.Column("attachments", postgresql.JSONB(), nullable=True),
        sa.Column(
            "status",
            sa.Enum(
                "received",
                "queued",
                "dispatched",
                "delivered",
                "failed",
                "bounced",
                native_enum=False,
                length=32,
            ),
            nullable=False,
        ),
        sa.Column("status_detail", sa.Text(), nullable=True),
        sa.Column("raw_payload", postgresql.JSONB(), nullable=False),
        sa.Column("requested_purpose", sa.String(255), nullable=True),
        sa.Column("requested_by_actor_id", sa.String(255), nullable=True),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        *timestamp_columns(),
        sa.UniqueConstraint(
            "provider",
            "provider_message_id",
            name="uq_messages_provider_message_id",
        ),
    )
    op.create_index("ix_messages_conversation_id", "messages", ["conversation_id"])

    op.create_table(
        "outbox_items",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "message_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("messages.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "state",
            sa.Enum(
                "pending",
                "claimed",
                "done",
                "dead",
                native_enum=False,
                length=16,
            ),
            nullable=False,
            server_default="pending",
        ),
        *timestamp_columns(),
    )
    op.create_index("ix_outbox_items_message_id", "outbox_items", ["message_id"])

    op.create_table(
        "event_outbox",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("event_type", sa.String(128), nullable=False),
        sa.Column("event_data", postgresql.JSONB(), nullable=False),
        sa.Column(
            "state",
            sa.Enum(
                "pending",
                "claimed",
                "done",
                "dead",
                native_enum=False,
                length=16,
            ),
            nullable=False,
            server_default="pending",
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        *timestamp_columns(),
    )


def downgrade() -> None:
    op.drop_table("event_outbox")
    op.drop_index("ix_outbox_items_message_id", table_name="outbox_items")
    op.drop_table("outbox_items")
    op.drop_index("ix_messages_conversation_id", table_name="messages")
    op.drop_table("messages")
