# schema_service

Canonical schema service of the `EdgeVector/lastdb` workspace
(`crates/{core,client,server_shared,server_http,server_lambda,worker,schema_service_s3}`).
Cloud-only: prod is the AWS Lambda at `schema.folddb.com` (deployed via
EdgeVector/schema-infra); the `server_http` actix binary is dev-only.

## Canonical commands

**No tests** (Tom, 2026-10-09). The repo has no test suite.
Do not write, add, run, restore, or require tests.
Remove test and test coverage requirements when you find them.
Use build, format, and lint checks.
Situation: `no-tests-all-repos-20261009`.

```bash
cargo build --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all                             # ALWAYS before committing
```

CI lints (canonical scripts in the monorepo root; run against just this crate):

```bash
bash ../scripts/lints/lint-redaction.sh        --scope=schema_service/crates
bash ../scripts/lints/lint-spawn-instrument.sh --scope=schema_service/crates
bash ../scripts/lints/lint-tracing-egress.sh   --scope=schema_service/crates --strict
```

PR / merge: `gh pr create -R EdgeVector/lastdb --base main --head <branch>`.
Use `gh pr merge <PR> -R EdgeVector/lastdb --auto --squash` after the required
`ci-required` check succeeds.
The old fold repo is frozen. Do not open PRs there.

## Ask the brain for anything project-specific

This file holds commands only. For architecture, the crate map, the embedder
injection design, and the canonicalization gate, ask the brain
(`brain ask "<q>"` / `brain get <slug>`):

- `concepts-fold-schema-service` — crate-by-crate layout, cloud-only deploy posture, `core` has no fastembed/ONNX (injected `Arc<dyn Embedder>`), dual-signal canonicalization gate, in-repo observability notes.
- `concepts-fold-build-test-ci-safety` — fmt gate, lints, workspace path-dep model, merge mechanics.
- `concepts-observability-conventions` — tracing/redaction/spawn/egress rules.

See also: `README.md`, `openapi.yaml` (the `/v1/*` contract).
