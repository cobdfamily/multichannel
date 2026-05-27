"""multichannel runtime configuration."""

from __future__ import annotations

import logging
from functools import lru_cache
from typing import Literal

import structlog
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="MC_",
        case_sensitive=True,
        extra="ignore",
    )

    SERVICE_NAME: str = "multichannel"
    SERVICE_VERSION: str = "0.1.0"
    ENVIRONMENT: Literal["development", "staging", "production"] = "development"

    DATABASE_URL: str = "postgresql+asyncpg://multichannel:multichannel@localhost/multichannel"
    DATABASE_POOL_MAX: int = 20
    REDIS_URL: str = "redis://localhost:6379/0"
    NOTAIO_URL: str = "http://localhost:8003"
    NOTAIO_CLIENT_SECRET: str = ""
    MEDICI_URL: str = "http://localhost:8002"
    MEDICI_CLIENT_SECRET: str = ""
    POSTMARK_SERVER_TOKEN: str = ""
    POSTMARK_FROM_EMAIL: str = ""
    SIGNALWIRE_PROJECT_ID: str = ""
    SIGNALWIRE_AUTH_TOKEN: str = ""
    SIGNALWIRE_SPACE_URL: str = ""
    SIGNALWIRE_FROM_NUMBER: str = ""
    META_GRAPH_API_VERSION: str = "v20.0"
    META_PAGE_ACCESS_TOKEN: str = ""
    INSTAGRAM_ACCESS_TOKEN: str = ""
    META_VERIFY_TOKEN: str = ""
    POSTMARK_WEBHOOK_SECRET: str = ""
    META_APP_SECRET: str = ""
    SIGNALWIRE_SIDECAR_HMAC: str = ""
    OUTBOX_DRAIN_ENABLED: bool = False
    EVENT_OUTBOX_DRAIN_ENABLED: bool = False

    # Token-bucket rate limits. Two buckets per /outbound:
    # one per actor (X-Actor-Id), one per provider. Either
    # exceeded -> 429 + Retry-After.
    RATE_LIMIT_ENABLED: bool = True
    RATE_LIMIT_ACTOR_PER_MIN: int = 60
    RATE_LIMIT_POSTMARK_PER_MIN: int = 300
    RATE_LIMIT_SIGNALWIRE_PER_MIN: int = 60
    RATE_LIMIT_FBMESSENGER_PER_MIN: int = 60
    RATE_LIMIT_INSTAGRAM_PER_MIN: int = 60

    LOG_LEVEL: Literal["debug", "info", "warning", "error"] = "info"
    LOG_FORMAT: Literal["json", "pretty"] = "json"

    @property
    def is_production(self) -> bool:
        return self.ENVIRONMENT == "production"


def configure_logging(settings: Settings) -> None:
    timestamper = structlog.processors.TimeStamper(fmt="iso", utc=True)
    processors: list = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        timestamper,
    ]
    processors.append(
        structlog.processors.JSONRenderer()
        if settings.LOG_FORMAT == "json"
        else structlog.dev.ConsoleRenderer()
    )
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, settings.LOG_LEVEL.upper())
        ),
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
