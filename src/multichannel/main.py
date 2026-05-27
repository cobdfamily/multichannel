"""multichannel FastAPI app entrypoint."""

from __future__ import annotations

import asyncio
import contextlib
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Response, status
from sqlalchemy import text

from multichannel import __version__
from multichannel.api import router as api_router
from multichannel.config import configure_logging, get_settings
from multichannel.db import Database
from multichannel.runtime import AppState
from multichannel.services.event_outbox_drain import EventOutboxDrain
from multichannel.services.medici_client import MediciClient
from multichannel.services.notaio_client import NotaioClient
from multichannel.services.outbox_drain import OutboxDrain
from multichannel.services.rate_limit import RateLimiter
from multichannel.services.redis_publisher import RedisStreamPublisher


@asynccontextmanager
async def lifespan(app: FastAPI):  # noqa: ANN201
    settings = get_settings()
    configure_logging(settings)
    log = structlog.get_logger("multichannel.startup")
    log.info("starting", version=__version__, env=settings.ENVIRONMENT)

    database = Database(settings)
    notaio = NotaioClient(settings)
    medici = MediciClient(settings)
    redis = RedisStreamPublisher(settings.REDIS_URL)
    # Open the publisher's connection up front so the limiter
    # shares the same client. RateLimiter only used when enabled.
    rate_limiter: RateLimiter | None = None
    if settings.RATE_LIMIT_ENABLED:
        await redis.connect()
        client = getattr(redis, "_client", None)
        if client is not None:
            rate_limiter = RateLimiter(client)
    app.state.multichannel = AppState(
        settings=settings,
        database=database,
        notaio=notaio,
        medici=medici,
        redis=redis,
        rate_limiter=rate_limiter,
    )

    outbox_task: asyncio.Task | None = None
    event_outbox_task: asyncio.Task | None = None
    if settings.OUTBOX_DRAIN_ENABLED:
        outbox_drain = OutboxDrain(database.session_maker)
        outbox_task = asyncio.create_task(outbox_drain.run(), name="multichannel.outbox.drain")
        log.info("outbox.drain.started")
    else:
        log.info("outbox.drain.disabled")
    if settings.EVENT_OUTBOX_DRAIN_ENABLED:
        event_drain = EventOutboxDrain(database.session_maker, redis)
        event_outbox_task = asyncio.create_task(
            event_drain.run(), name="multichannel.event_outbox.drain"
        )
        log.info("event_outbox.drain.started")
    else:
        log.info("event_outbox.drain.disabled")
    app.state.multichannel.outbox_task = outbox_task
    app.state.multichannel.event_outbox_task = event_outbox_task

    try:
        yield
    finally:
        log.info("stopping")
        for task in (outbox_task, event_outbox_task):
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
        await redis.close()
        await medici.close()
        await notaio.close()
        await database.close()


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title="multichannel",
        version=__version__,
        lifespan=lifespan,
        docs_url="/docs" if not settings.is_production else None,
        redoc_url=None,
    )

    @app.get("/health", include_in_schema=False)
    async def health(response: Response) -> dict[str, object]:
        state = app.state.multichannel
        try:
            async with state.database.engine.connect() as conn:
                await conn.execute(text("select 1"))
            await state.redis.connect()
            client = getattr(state.redis, "_client", None)
            if client is not None:
                await client.ping()
        except Exception:
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
            return {"status": "unhealthy"}

        def task_state(task: asyncio.Task | None) -> str:
            if task is None:
                return "disabled"
            if task.done():
                return "dead" if task.exception() else "finished"
            return "running"

        return {
            "status": "ok",
            "outbox_drain": task_state(getattr(state, "outbox_task", None)),
            "event_outbox_drain":
                task_state(getattr(state, "event_outbox_task", None)),
        }

    app.include_router(api_router)
    return app


app = create_app()


def run() -> None:
    import uvicorn

    uvicorn.run("multichannel.main:app", host="0.0.0.0", port=8004)  # noqa: S104
