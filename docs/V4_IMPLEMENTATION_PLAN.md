# Socket.IO Go v4 Implementation Plan

This plan turns `V4_API_DESIGN.md` into an executable migration sequence. The goal is to reach v4 without destabilizing the proven protocol/interop core.

## Phase 0 — Freeze contracts

Deliverables:

- finalize `Server`, `Namespace`, `Socket`, `BroadcastOperator` public surfaces;
- finalize `Subscription`, `RawEmitter`, `RawRegistrar`, `AckEmitter`;
- finalize v4 Adapter interface;
- finalize `Option`/`Config` model;
- finalize queue/backpressure defaults.

Acceptance:

- `design/v4preview` compiles in CI;
- no unresolved public API naming conflicts;
- migration guide maps every removed v3 public API.

## Phase 1 — Lifecycle and concurrency core

Implement in the v3 codebase first where non-breaking, then carry forward:

- Socket lifetime context;
- exact listener subscription tokens;
- immutable Socket emission flags;
- atomic Namespace creation;
- pre-connect Socket tracking and cleanup;
- bounded ordered per-Socket queue;
- Go-native `Serve`, `ListenAndServe`, `Shutdown`.

Acceptance:

- `go test -race` on core modules;
- no leaked queue goroutines after aborted middleware handshakes;
- concurrent `Of("/same")` returns one namespace instance;
- concurrent `Volatile/Timeout/Compress` emissions do not share transient state;
- listener closure identity tests pass.

## Phase 2 — Root v4 package

Create module path:

```text
github.com/aqcool/socket.io/v4
```

Port:

- Server/Namespace/Socket lifecycle;
- in-memory Adapter;
- parser integration;
- typed Event API into root package;
- error model and sentinel errors;
- HTTP handler integration.

Do not port provider adapters yet.

Acceptance:

- chat/basic examples compile on v4;
- upstream Node client interoperability passes for default adapter;
- v4 API contains no `Prototype/Proto/Construct` public methods;
- primary async query APIs return `(T, error)` and take context.

## Phase 3 — Adapter migration

Port providers in this order:

1. Redis
2. Valkey
3. Postgres
4. MongoDB
5. NATS
6. Kafka
7. AMQP
8. Unix/local cluster adapter

Each provider must implement the same capability declaration and error semantics.

Acceptance per adapter:

- official interoperability matrix passes;
- cluster broadcast/ACK tests pass;
- recovery tests pass if capability is declared;
- context cancellation stops pending provider operations;
- Close is idempotent and drains worker goroutines.

## Phase 4 — Client modules

Port Engine.IO and Socket.IO Go clients after server API freeze so shared contracts do not churn.

Acceptance:

- official Node server interoperability;
- reconnect/backoff tests;
- binary/ACK tests;
- context-aware Dial/Close APIs;
- backpressure limits documented.

## Phase 5 — Observability and optional features

Port:

- instrumentation/Admin UI;
- OpenTelemetry hooks;
- Prometheus hooks;
- reliability layer;
- sticky routing;
- presence/auth/guard integrations.

Rules:

- core module must not gain heavy observability dependencies;
- context is the trace propagation carrier;
- optional modules must not change wire behavior.

## Phase 6 — RC hardening

Required gates before `v4.0.0-rc.1`:

- all module unit tests pass;
- core and adapters pass `go test -race`;
- upstream Node interoperability matrix passes;
- fuzz targets run for packet/parser boundaries;
- 3-hour cluster soak passes;
- network partition/recovery scenario passes;
- no known goroutine growth after connect/disconnect churn;
- migration guide verified against representative v3 examples.

## Phase 7 — v4.0.0

Before final release:

- remove temporary compatibility shims that were explicitly marked RC-only;
- freeze exported names;
- publish API docs/examples;
- tag provider adapters with compatible v4 releases;
- retain v3 security/critical bug support policy for a documented transition period.

## Non-goals during v4 migration

Do not add new transports or new broker adapters until the v4 core is stable. Feature growth should not compete with API/lifecycle stabilization.

Do not rewrite Engine.IO upgrade sequencing without a failing race/interop/fuzz case proving the need.

## Suggested PR sequence

```text
PR 1  v4 contracts + docs
PR 2  Socket context + subscriptions
PR 3  immutable SocketOperator
PR 4  Namespace atomic/pre-connect lifecycle
PR 5  bounded queue/backpressure
PR 6  root typed Event API + codec
PR 7  Adapter v4 + memory adapter
PR 8  Redis/Valkey
PR 9  Postgres/Mongo
PR 10 remaining providers + RC cleanup
```

Every PR should include one or more of:

- Node behavior comparison test;
- concurrency/race regression test;
- migration compile test;
- resource-lifecycle regression test.
