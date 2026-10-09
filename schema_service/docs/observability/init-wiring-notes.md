# Init Wiring Notes (schema_service dev binary)

Phase 2 / observability — cohort B6 follow-up to PR #65 (W3C ingress
middleware). Twin of `fold_db_node`'s `init_node` wiring; the Lambda
binary path is **out of scope** here and is handled by cohort B5
(`T2-lambda-ingress` in `exemem-infra`).

Branch: `phase2-init-wiring` (cut from `main` @ `9a1d92d7`).

## Goal

Make the `W3CParentContext::set_parent` call PR #65 added actually flow
trace ids end-to-end. Until the binary installs the global
`tracing::Subscriber` + `TraceContextPropagator` that ship inside
`observability::init_node`, the middleware's parent extraction is a
no-op (see the "Propagator dependency" section in
`crates/server_http/src/middleware/otel.rs` for the in-tree warning).

## Implementation

`crates/server_http/src/bin/schema_service.rs` swap:

```rust
// before
fold_db::logging::LoggingSystem::init_default().await.ok();

// after
let _obs_guard = observability::init_node(
    "schema_service",
    env!("CARGO_PKG_VERSION"),
)?;
```

The `_obs_guard` binding is intentional. `ObsGuard` is `#[must_use]`
and contains the `tracing-appender` `WorkerGuard`; dropping it stops
the background flush thread mid-drain and any queued lines on the
channel are lost. A leading underscore silences the unused-variable
lint without dropping the binding (`_` would drop immediately, killing
the worker before the first request).

## Why `init_node` rather than `init_lambda` / `init_cli`

`init_node` is the only variant that:

1. Reads `OBS_FILE_PATH` and falls back to
   `~/.folddb/observability.jsonl` — the path the local dashboard
   already polls.
2. Wires the RING layer for `/api/logs` queries.
3. Installs a `tracing-opentelemetry` layer with a no-op
   `TracerProvider`, which is what mints the per-span `OtelData`
   that `set_parent` updates and that the egress propagator reads.

The schema_service dev binary is a long-running Actix server with a
local dashboard counterpart on the same box, so it matches the
"node" shape exactly. `init_lambda` (stdout, no RING) is reserved
for the cohort B5 Lambda sweep.

## Smoke test

`crates/server_http/tests/observability_smoke.rs` boots an Actix
`App` wrapped in `W3CParentContext + TracingLogger`, sends a request
with the W3C example traceparent, drops the guard, then asserts:

1. The handler's `tracing::info!` event landed as a JSON line in
   `$OBS_FILE_PATH` — proves init_node honored the env override and
   the FMT layer is connected to the global subscriber.
2. `Span::current().context().span().span_context().trace_id()`
   inside the handler equals the inbound trace_id — proves the
   propagator + `set_parent` compose end-to-end.

### Single-test-per-file constraint

`init_node` installs a process-global subscriber via `OnceCell` and
`set_global_default` — both single-shot. `OBS_FILE_PATH` is also
read once per init call. A second `#[test]` in the same integration
binary would either collide on init or overwrite the file path
mid-run. Tests in the lib's own test binary (the `mod tests` blocks
under `src/`) are in a separate process and are unaffected.

### Why the trace_id assertion goes through `Span::current().context()`,
### not the RING handle

Direct read of `OtelData.builder.trace_id` (what the RING layer
does) returns the *fresh* trace_id `tracing-opentelemetry`'s
`on_new_span` minted before `W3CParentContext::set_parent` ran. The
parent context updated by `set_parent` is on
`OtelData.parent_cx`, not `builder.trace_id`. The derived id is
correct because `tracer.sampled_context(builder)` (which
`OpenTelemetrySpanExt::context()` calls) reconciles the two; the
direct field read does not.

This is a real but **out-of-scope** mismatch in the upstream
`observability` crate's RING layer — fixing it requires teaching
RING to derive trace_id through `tracer.sampled_context` (or to
prefer `parent_cx.span().span_context().trace_id()` when present).
Filed as a follow-up; the schema_service-side wiring is correct.

The egress side (`observability::propagation::inject_w3c`) already
reads through `Span::current().context()`, so outbound calls
correctly inherit the inbound trace_id today. RING's drift only
affects the in-process `/api/logs` query view, not the wire trace.

## Verification

From the worktree at `/Users/example/.cline/worktrees/7e053/schema_service`:

- `cargo build --workspace` — clean.
- `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- `cargo test -p schema_service_server_http --test observability_smoke`
  — 1 passed.
- `cargo test --workspace` — the new smoke test passes; any unrelated
  workspace failures should be triaged against the current baseline.

## Follow-ups

1. **RING trace_id derivation** — upstream `observability` crate.
   RING should use `tracer.sampled_context(builder).span().span_context().trace_id()`
   so it agrees with `Span::current().context()` when ingress
   middleware re-parents the root span post-`on_new_span`. Without
   this, `/api/logs` will show a different trace_id than the wire
   trace for every request that came in with a traceparent.

2. **`fold_db::logging` deprecation** — the bin no longer calls
   `LoggingSystem::init_default`, but `log_feature!` macros in
   `crates/core/src/classify.rs` still emit
   through fold_db's `log` crate. The `LogTracer` install inside
   `init_node` bridges those to the global subscriber, so they keep
   working — but a sweep to replace them with native `tracing::info!`
   would let us drop the fold_db `logging` module from the dep
   surface entirely. Coordinate with the fold_db_node twin.
