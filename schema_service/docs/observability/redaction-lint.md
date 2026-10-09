# Redaction lint (Phase 5 / P5-T1-schema)

Static guard that fails CI if a `tracing` macro emits a sensitive field as a
raw value instead of routing it through the redaction macros.

This repo is a downstream consumer of `fold_db`'s `observability` crate, which
owns the format-time deny-list inside the `RedactingFormat` JSON formatter.
The CI lint here is the second of the same two layers of defence the canonical
fold_db doc describes — it catches the mistake at PR time so the deny-list
never has to fire in production.

The canonical rationale, full guarded-fields list, override grammar, and
deny-list-sync rules live in `fold_db/docs/observability/redaction-lint.md`.
This file documents only the schema_service-specific deltas.

## Local adjustments

- **Scope.** `crates/*/src/` — schema_service has the same crates layout as
  fold_db (`client`, `core`, `server_http`, `server_lambda`, `server_shared`,
  `worker`), so the rg root matches the canonical script
  with no path changes. Tests under `crates/*/tests/` are out of scope at
  the directory level, mirroring `lints/lint-tracing-egress.sh`.
- **Override count.** Zero — the redaction lint exits 0 from day one because
  there are currently no sensitive fields appearing on the right-hand side
  of a `tracing::*!` macro in `crates/*/src/`.

## Override grammar

Identical to fold_db. On the violating line itself or the line directly above
it (the two-line window survives `rustfmt` lifting a trailing comment onto
its own line), add a comment containing the literal:

```
// lint:redaction-ok <reason>
```

Always include a short reason after the marker so the next reviewer can tell
at a glance whether the override is still load-bearing. Use overrides
sparingly — typically only for unit tests that have to feed the raw value to
the FMT layer to verify the deny-list redacts it.

## Running locally

```sh
# From the fold monorepo root:
bash scripts/lints/lint-redaction.sh --scope=schema_service/crates
```

Exits `0` when every match is wrapped or overridden, `1` otherwise. The CI
job `Redaction Lint` in `.github/workflows/ci.yml` runs the canonical
script across `fold_db/crates` and `schema_service/crates` on every PR
and `push` to `main`. The job is parallel to `Spawn Instrument Lint` and
`Clippy + Tests`, runs on ubuntu-latest with apt-installed ripgrep, and
has no Rust toolchain dependency so it gives feedback in seconds.

## Out of scope (handled in fold_db, not duplicated here)

- The format-time deny-list inside the `RedactingFormat` JSON formatter —
  schema_service does not own that layer; it consumes it via the
  `observability` crate from fold_db. Extending the guarded-fields list
  requires editing both this script's `PATTERN` and the fold_db deny-list
  together, the same way fold_db's own doc describes.
- `crates/*/tests/` integration tests — the lint scope deliberately mirrors
  `lints/lint-tracing-egress.sh` for consistency.
- Sibling repos (`fold_db_node`, `exemem-infra`) — each ships its own
  per-repo port of the lint as a follow-up.
