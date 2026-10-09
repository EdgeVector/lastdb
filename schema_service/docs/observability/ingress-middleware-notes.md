# Phase 2 / B3 — schema_service ingress middleware notes

Working notes from the Phase 2 B3 implementation (Actix middleware that
extracts the W3C `traceparent` header on each request and attaches the
resulting `opentelemetry::Context` as the parent of the request span).

Twin of fold_db_node B1 (PR #708). The pattern, file layout, and tests
are intentionally identical so the two binaries stay in lockstep.

## 2026-04-27 — Same dep gap as fold_db_node B1

The B3 task description claimed:

> the `observability` crate is already a workspace dep (added during
> Phase 1 T1c).

This is **not** what shipped. T1c (PR #60, [e8fa075][pr-60]) only
redirected the local-dev `.cargo/config.toml` patch at the new
`crates/core` workspace member after fold_db became a workspace — it
did not add `observability` to `Cargo.toml`. The fold_db rev pin
(`a0434b25…`, see workspace `Cargo.toml`) is still **pre-workspace**,
so the `observability` crate is not reachable through the current
`fold_db` dep at all.

[pr-60]: https://github.com/EdgeVector/schema_service/pull/60

This is the same gap fold_db_node B1 hit, resolved the same way.

## What B3 had to add

- `observability` as a direct git dep on `fold_db.git` (rev pinned to
  `8c6af2a0…`, the same post-workspace mainline commit fold_db_node
  B1 used). Lives in `[workspace.dependencies]`; consumed by
  `crates/server_http`.
- `tracing-actix-web` (root span source — produces the span the
  middleware mutates).
- `tracing` / `tracing-opentelemetry` / `opentelemetry` as explicit
  workspace deps (versions match
  `fold_db/crates/observability/Cargo.toml`: tracing 0.1,
  tracing-opentelemetry 0.28, opentelemetry 0.27). Versions must
  match exactly so cargo unifies them in the dep graph; otherwise we
  link two copies of opentelemetry types and `set_parent` calls do
  not typecheck.
- `http = "1"` because
  `observability::propagation::extract_parent_context` takes
  `&http::HeaderMap` from http 1.x. actix-http internally uses http
  0.2; the two `HeaderMap` containers are not interchangeable, so
  the middleware rebuilds an http 1.x `HeaderMap` on ingress.
- `futures-util` for `LocalBoxFuture` in the `Service` impl.
- `opentelemetry_sdk` + `tracing-subscriber` as dev-deps so the
  middleware test can install a propagator + OpenTelemetry layer
  ad-hoc and round-trip the header → span trace_id end-to-end.
- `.cargo/config.toml` extended with
  `observability = { path = "../fold_db/crates/observability" }` so
  sibling-checkout dev still collapses every spec onto one path.

## Why two revs of the same git URL is OK

Adding `observability` at a different rev than `fold_db` looks like it
should hit the dual-`fold_db` trap CLAUDE.md warns about. It doesn't:

- The dual-`fold_db` trap fires when **the `fold_db` package** is
  compiled twice and types from each copy show up at the same call
  site (re-exported through `schema_service_core` while imported
  directly elsewhere).
- `observability` does not depend on `fold_db` (see
  `fold_db/crates/observability/Cargo.toml`). Pinning it at a
  different rev brings in only the `observability` package; the
  workspace's other member (`fold_db`) at that rev is not compiled.

## Why we did not bump the `fold_db` rev

CLAUDE.md (and the workspace `Cargo.toml` comment on the pin) is
explicit that `fold_db_node` and `schema_service` must bump `fold_db`
revs in lockstep — otherwise the dual-`fold_db` errors fire. A
lockstep bump is a cross-repo workflow that is out of scope for B3
(which is purely middleware on the ingress path). Adding
`observability` as an independent dep sidesteps the lockstep
requirement entirely, exactly as fold_db_node B1 did.

When the next coordinated `fold_db` rev bump lands across both
consumers (post-workspace), the independent `observability` dep can be
dropped in favor of `fold_db`'s transitive re-export — or kept,
depending on whether we want `observability` to evolve independently
of `fold_db`'s release cadence.

## Propagator install is still missing

`observability::propagation::extract_parent_context` relies on a global
text-map propagator (`TraceContextPropagator`) being installed.
`observability::init::init_node` installs it, but `schema_service` does
not yet call any of the `init_*` helpers — that is a separate Phase 1
follow-up. Until it lands, the middleware will run with **no
propagator installed**, and every extracted context will be empty
(invalid span context). That is a no-op, not a regression: spans
simply will not be parented across the HTTP boundary.

The middleware test (`extracts_traceparent_into_root_span_parent`)
installs the propagator ad-hoc to validate the header → span
round-trip in isolation.

## Out-of-scope follow-ups

- **B4** — egress site classification for any outbound HTTP
  schema_service makes (`schema_service_client` and classifier call sites).
- **B5** — Lambda-binary path. The `crates/server_lambda` binary uses
  a different runtime (Lambda Web Adapter / `lambda_http`), not
  Actix; the W3C extraction there is covered by exemem-infra B5.
