# `tokio::spawn` instrument lint (Phase 5 / P5-T2-schema)

Static guard that fails CI if a `tokio::spawn(async ... { ... })` site
in `crates/*/src/` does not chain `.instrument(...)` / `.in_current_span()`
on the spawned future, and is not explicitly marked as an intentional
bare spawn.

This is the schema_service port of the canonical fold_db lint shipped
in [fold_db PR 654](https://github.com/EdgeVector/fold_db/pull/654).
The detection logic, override grammar, and exit-code convention are
identical — see the canonical doc for the full rationale, the override
categories, and the runtime test that pairs with this static check:

> [`fold_db/docs/observability/spawn-instrument-lint.md`](../../../fold_db/docs/observability/spawn-instrument-lint.md)

## Running locally

```sh
# From the fold monorepo root:
bash scripts/lints/lint-spawn-instrument.sh --scope=schema_service/crates
```

Exit code is `0` when every spawn site is instrumented or marked, `1`
otherwise. The CI job `Spawn Instrument Lint`
(`.github/workflows/ci.yml`) runs the canonical script with
`--scope=schema_service/crates` in parallel with the rust job on every
PR and `push` to `main`. It only needs `ripgrep` — no cargo dependency,
so it finishes in seconds.

## Override syntax (recap)

Use `// lint:spawn-bare-ok <reason>` for spawns that genuinely have no
parent context to propagate (boot-time perpetual workers, `#[cfg(test)]`
scaffolding). The marker may live on the spawn line, the preceding
line, or anywhere inside the spawn call's body. Always include a short
reason after the marker.

## schema_service-specific adjustments

- **Scope path.** Identical to fold_db: `crates/*/src/`. schema_service
  uses the same Cargo workspace layout (one `crates/<name>/src/` tree
  per crate, integration tests under `crates/<name>/tests/`), so no
  scope edit was needed.
- **CI shape.** schema_service runs the lint as its own parallel job
  (`Spawn Instrument Lint`) in `.github/workflows/ci.yml`, distinct from
  the heavier `rust` job. The lint has no cargo dependency, so a
  parallel job gives faster feedback than gating the rust pipeline on
  it. fold_db runs it as a step inside `rust-tests` for historical
  reasons; the substance is the same.
- **Override count.** One pre-approved override at lint-port time, in
  `crates/core/src/classify.rs::call_ollama_timeout` — a `#[cfg(test)]`
  scaffolding spawn that holds an idle TCP listener to drive a timeout
  test. No request-path spawns required overrides.
