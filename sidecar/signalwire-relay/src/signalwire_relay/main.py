from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import signal
import time
from collections.abc import Awaitable, Callable
from typing import Any

import httpx
import structlog
from aiohttp import web
from pydantic_settings import BaseSettings, SettingsConfigDict
from tenacity import (
    AsyncRetrying,
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    stop_never,
    wait_exponential,
)

from signalwire_relay.translate import compute_hmac, translate_event

EventHandler = Callable[[dict[str, Any]], None]
ClientFactory = Callable[["Settings"], Any]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SWR_")

    MULTICHANNEL_URL: str = "http://multichannel:8004"
    SIDECAR_HMAC: str
    SIGNALWIRE_PROJECT_ID: str
    SIGNALWIRE_API_TOKEN: str
    SIGNALWIRE_SPACE_URL: str
    LOG_LEVEL: str = "INFO"


class RelayState:
    def __init__(self) -> None:
        self.last_alive_at = 0.0

    def mark_alive(self) -> None:
        self.last_alive_at = time.monotonic()

    @property
    def healthy(self) -> bool:
        return self.last_alive_at > 0 and time.monotonic() - self.last_alive_at <= 30


def configure_logging(level: str) -> None:
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO))
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.JSONRenderer(),
        ],
    )


async def run_async(
    *,
    settings: Settings | None = None,
    client: Any | None = None,
    client_factory: ClientFactory | None = None,
) -> None:
    settings = settings or Settings()
    configure_logging(settings.LOG_LEVEL)
    log = structlog.get_logger("signalwire_relay")
    state = RelayState()
    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop_event.set)

    health_runner = await start_health_server(state)
    log.info("health_server_started", port=8005)

    async with httpx.AsyncClient(timeout=10) as http:
        try:
            await relay_reconnect_loop(
                settings=settings,
                http=http,
                state=state,
                stop_event=stop_event,
                client=client,
                client_factory=client_factory or default_client_factory,
            )
        finally:
            await health_runner.cleanup()
            log.info("shutdown_complete")


def run() -> None:
    asyncio.run(run_async())


async def relay_reconnect_loop(
    *,
    settings: Settings,
    http: httpx.AsyncClient,
    state: RelayState,
    stop_event: asyncio.Event,
    client: Any | None,
    client_factory: ClientFactory,
) -> None:
    log = structlog.get_logger("signalwire_relay")

    async for attempt in AsyncRetrying(
        retry=retry_if_exception_type(Exception),
        wait=wait_exponential(multiplier=1, min=1, max=60),
        stop=stop_never,
        before_sleep=_log_reconnect_sleep,
        reraise=True,
    ):
        if stop_event.is_set():
            return
        with attempt:
            relay_client = client or client_factory(settings)
            try:
                await run_relay_session(
                    relay_client,
                    settings=settings,
                    http=http,
                    state=state,
                    stop_event=stop_event,
                )
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if stop_event.is_set():
                    return
                log.warning("relay_session_dropped", error=str(exc))
                raise
            finally:
                await maybe_await(_close_client(relay_client))


async def run_relay_session(
    relay_client: Any,
    *,
    settings: Settings,
    http: httpx.AsyncClient,
    state: RelayState,
    stop_event: asyncio.Event,
) -> None:
    log = structlog.get_logger("signalwire_relay")

    async def deliver(raw: dict[str, Any]) -> None:
        state.mark_alive()
        envelope = translate_event(raw)
        await post_event(settings=settings, http=http, envelope=envelope)

    def handler(raw: dict[str, Any]) -> None:
        asyncio.create_task(_deliver_logged(deliver, raw))

    await maybe_await(_connect_client(relay_client))
    state.mark_alive()
    subscribe_to_events(relay_client, handler)
    log.info("relay_session_started")

    await _wait_for_client_or_stop(relay_client, stop_event)


async def _deliver_logged(
    deliver: Callable[[dict[str, Any]], Awaitable[None]],
    raw: dict[str, Any],
) -> None:
    log = structlog.get_logger("signalwire_relay")
    try:
        await deliver(raw)
    except Exception as exc:
        log.warning("event_delivery_failed", error=str(exc))


@retry(
    retry=retry_if_exception_type(httpx.HTTPStatusError),
    wait=wait_exponential(multiplier=1, min=1, max=30),
    stop=stop_after_attempt(5),
    reraise=True,
)
async def post_event(
    *,
    settings: Settings,
    http: httpx.AsyncClient,
    envelope: dict[str, Any],
) -> None:
    log = structlog.get_logger("signalwire_relay")
    body = json.dumps(envelope, separators=(",", ":"), sort_keys=True).encode("utf-8")
    response = await http.post(
        f"{settings.MULTICHANNEL_URL.rstrip('/')}/webhook/signalwire",
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-COBD-Sidecar-HMAC": compute_hmac(settings.SIDECAR_HMAC, body),
        },
    )
    if response.status_code >= 500:
        response.raise_for_status()
    if response.status_code >= 400:
        log.warning(
            "event_post_rejected",
            status_code=response.status_code,
            provider_message_id=envelope.get("provider_message_id"),
        )
        return
    log.info(
        "event_posted",
        status_code=response.status_code,
        provider_message_id=envelope.get("provider_message_id"),
    )


async def start_health_server(state: RelayState) -> web.AppRunner:
    async def health(_: web.Request) -> web.Response:
        status = 200 if state.healthy else 503
        return web.json_response({"ok": state.healthy}, status=status)

    app = web.Application()
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8005)
    await site.start()
    return runner


def default_client_factory(settings: Settings) -> Any:
    from signalwire.realtime_api import SignalWire

    try:
        return SignalWire(
            project_id=settings.SIGNALWIRE_PROJECT_ID,
            token=settings.SIGNALWIRE_API_TOKEN,
            space_url=settings.SIGNALWIRE_SPACE_URL,
        )
    except TypeError:
        return SignalWire(
            settings.SIGNALWIRE_PROJECT_ID,
            settings.SIGNALWIRE_API_TOKEN,
            settings.SIGNALWIRE_SPACE_URL,
        )


def subscribe_to_events(client: Any, handler: EventHandler) -> None:
    events = (
        "messaging.message.received",
        "calling.call.received",
        "calling.call.state",
        "voice.call.received",
    )
    if hasattr(client, "on"):
        for event in events:
            client.on(event, handler)
        return
    if hasattr(client, "subscribe"):
        for event in events:
            client.subscribe(event, handler)
        return
    if hasattr(client, "add_event_listener"):
        for event in events:
            client.add_event_listener(event, handler)
        return
    raise RuntimeError("SignalWire client does not expose a recognized event subscription API")


async def _wait_for_client_or_stop(client: Any, stop_event: asyncio.Event) -> None:
    for method_name in ("wait_closed", "wait", "serve_forever", "run_forever"):
        method = getattr(client, method_name, None)
        if method is not None:
            task = asyncio.create_task(maybe_await(method()))
            stop_task = asyncio.create_task(stop_event.wait())
            done, pending = await asyncio.wait(
                {task, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for pending_task in pending:
                pending_task.cancel()
            if stop_task in done:
                return
            await task
            return

    while not stop_event.is_set():
        await asyncio.sleep(1)


async def maybe_await(value: Any) -> Any:
    if isinstance(value, Awaitable):
        return await value
    return value


def _connect_client(client: Any) -> Any:
    for method_name in ("connect", "open", "start"):
        method = getattr(client, method_name, None)
        if method is not None:
            return method()
    return None


def _close_client(client: Any) -> Any:
    for method_name in ("close", "disconnect", "stop"):
        method = getattr(client, method_name, None)
        if method is not None:
            return method()
    return None


def _log_reconnect_sleep(retry_state: RetryCallState) -> None:
    wait_time = retry_state.next_action.sleep if retry_state.next_action else None
    structlog.get_logger("signalwire_relay").warning(
        "relay_reconnect_scheduled",
        attempt=retry_state.attempt_number,
        wait_seconds=wait_time,
    )


if __name__ == "__main__":
    run()
