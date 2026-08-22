# v4 API Preview

This directory is a **contract-only, compile-checked preview** of the proposed v4 public API.

It is not a Socket.IO implementation and must not be imported by production applications. Its purpose is to make the signatures in `docs/V4_API_DESIGN.md` concrete enough for the Go compiler and reviewers to validate before the real v4 implementation starts.

The package deliberately uses only the standard library and small interfaces. No v3 implementation types are imported, which helps expose accidental coupling to v3 internals.

Key decisions represented here:

- consistent `error` returns for fallible operations;
- `context.Context` for I/O and operation lifetime;
- exact `Subscription` listener identity;
- typed `Event[Request, Response]` in the root API shape;
- immutable operator contracts;
- callback-free Adapter query methods;
- no public prototype/constructor emulation;
- bounded queue configuration;
- ordinary `http.Handler` / `net.Listener` integration.
