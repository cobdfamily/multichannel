# multichannel

Thin bi-directional messaging normalizer.
Fronts CiviCRM, Chatwoot, and anything
else that wants to receive messages
without learning four provider-specific
webhook shapes.

```
   ┌──────────────────────────────────────────────────────────┐
   │                        consumers                         │
   │   (CiviCRM bridge, Chatwoot bridge, florin notifier...)  │
   └──────────────────────────────────────────────────────────┘
                              ▲
                              │  CloudEvent envelopes
                              │  (Redis Streams)
   ┌──────────────────────────┴───────────────────────────────┐
   │                  multichannel (FastAPI)                  │
   │                                                          │
   │  POST /outbound          atrium-fronted (X-Actor, etc.)  │
   │  POST /webhook/postmark        verify HMAC               │
   │  POST /webhook/fbmessenger     verify X-Hub-Signature    │
   │  POST /webhook/instagram       (FB Graph -- same)        │
   │  POST /webhook/signalwire      shared HMAC from sidecar  │
   │  GET  /messages/{id}                                     │
   │                                                          │
   │  Postgres: inbox, outbox, dedup, threading, audit        │
   │  Redis:    streams (events), token-bucket rate limits    │
   └──────────────────────────┬───────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
    Postmark               Meta Graph         signalwire-relay
    (HTTP REST)         (FB Messenger,        (sidecar holding
                         Instagram DMs)        the Relay socket)
                                                       │
                                                       ▼
                                              SignalWire Realtime
```

## What it does

* Inbound: provider POSTs a webhook
  (Postmark, FB, IG) or the sidecar
  forwards a Relay event. multichannel
  verifies the signature, dedupes by
  `(provider, provider_message_id)`,
  derives a `conversation_id` from the
  provider thread headers, stores the
  envelope in Postgres, and emits a
  `cobd.multichannel.message.received`
  CloudEvent to Redis Streams.
* Outbound: consumers (florin, medici,
  a CiviCRM workflow, etc.) call
  `POST /outbound` with a CloudEvent
  envelope. multichannel checks
  consent via medici, looks up which
  provider handles `to:`, dispatches
  (Apprise for the providers it
  supports, native SDK otherwise), and
  records the result.

## What it does NOT do

* No agent UI / inbox. Chatwoot does
  that downstream by consuming events
  from Redis Streams.
* No conversation threading beyond
  provider thread IDs and a derived
  `conversation_id` UUID. Cross-channel
  identity resolution (Alice's SMS +
  email being the same person) is the
  CRM's job, not ours.
* No template substitution. Callers
  send fully-rendered text/html. If
  Postmark templates are useful, the
  caller invokes them directly.

## Layout

```
src/multichannel/
  main.py              FastAPI app entrypoint
  config.py            Settings (pydantic-settings)
  db.py                async SQLAlchemy engine + session
  runtime.py           shared DI: actor/session/notaio/medici deps
  models/              SQLAlchemy ORM
  api/v1/              FastAPI routers
  providers/           per-provider in + out adapters
  services/            notaio, medici, redis-streams clients
alembic/               migrations
sidecar/
  signalwire-relay/    standalone process holding the WS socket
tests/                 pytest
docker-compose.yaml    local dev: app + sidecar + pg + redis
```

## Auth

Same pattern as medici: atrium sits in
front, validates the JWT, forwards
verified identity via headers:

* `X-Actor-Id`    — caller user/service id
* `X-Actor-Type`  — `service` or `user`
* `X-Purpose`     — the declared purpose (transactional/marketing/admin)

multichannel reads these headers; it
does not parse tokens. Atrium also
returns 401/403 upstream of us — by
the time a request hits multichannel,
auth is already a yes.

## Consent

For outbound, every dispatch hits
medici's `GET /api/v1/persons/{id}/
consent?purpose=...&channel=...`
before sending. A `false` response
fails the request with `409
consent-revoked`. Notaio audits both
the consent check and the dispatch
attempt.

## Audit

Every received/sent/failed event lands
in notaio with `actor_user_id`,
`action`, `outcome`, plus event-id and
provider. Same pattern medici uses.

## Local dev

```sh
docker compose up -d   # postgres + redis + multichannel + sidecar
uv run poe dev         # if you want the app running on your host
uv run poe test
```

## See also

* `sidecar/signalwire-relay/` — the
  Relay-WebSocket holder
* `Other/multichannel/` — the
  abandoned 2026-04 TypeScript skeleton
  (DEPRECATED; do not import)
