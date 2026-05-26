# SignalWire Relay Sidecar

This directory contains a standalone Python process for the COBD multichannel
service. It owns the long-lived SignalWire Relay WebSocket connection and sends
normalized inbound events to the main service over HTTP.

The sidecar deliberately does not import the parent multichannel application.
Its only integration contract is:

- `POST {SWR_MULTICHANNEL_URL}/webhook/signalwire`
- JSON body matching the envelope produced by `signalwire_relay.translate`
- `X-COBD-Sidecar-HMAC` header containing `hex(hmac_sha256(secret, body))`

## Runtime

Install dependencies and run tests:

```sh
uv sync
uv run pytest -v
```

Run locally:

```sh
SWR_SIDECAR_HMAC=change-me \
SWR_SIGNALWIRE_PROJECT_ID=... \
SWR_SIGNALWIRE_API_TOKEN=... \
SWR_SIGNALWIRE_SPACE_URL=example.signalwire.com \
uv run signalwire_relay
```

The process starts an aiohttp health server on port `8005`.

`GET /health` returns:

- `200` when the Relay socket has been alive within the last 30 seconds
- `503` when the socket has not connected or has gone stale

## Configuration

Settings are read with `pydantic-settings` using the `SWR_` prefix:

- `SWR_MULTICHANNEL_URL`, default `http://multichannel:8004`
- `SWR_SIDECAR_HMAC`, required shared secret
- `SWR_SIGNALWIRE_PROJECT_ID`, required SignalWire project ID
- `SWR_SIGNALWIRE_API_TOKEN`, required SignalWire API token
- `SWR_SIGNALWIRE_SPACE_URL`, required SignalWire space URL
- `SWR_LOG_LEVEL`, default `INFO`

## Event Flow

SignalWire inbound messaging events are translated to the common multichannel
envelope. SMS uses `text_body`; MMS also includes media URLs in `attachments`.
Voice and call events are forwarded with `text_body` set to `null` and their
provider metadata preserved in `raw`.

HTTP delivery retries on 5xx responses with exponential backoff. The Relay
connection runs in a reconnect loop with capped exponential backoff and cleanly
closes on `SIGINT` or `SIGTERM`.
