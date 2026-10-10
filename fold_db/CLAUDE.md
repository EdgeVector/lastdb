# fold_db

Core library of the `EdgeVector/lastdb` workspace (`crates/{core,observability}`).
Local, schema-based database engine (LastStore-backed) with optional encrypted
Exemem cloud sync. **Library only — no UI, no React app, no desktop shell.**

Product surface is LastDB Mini in sibling crate `lastdb_node` (`lastdb` +
`lastdbd` over the owner Unix socket). Desktop/Tauri/`fold_db_node` + the old
embedded React UI were deleted in the Mini-only cutover (2026-07-12); restore
point branch: `archive/desktop-dmg-pre-removal`. Do not resurrect them here.

## Canonical commands

**No tests** (Tom, 2026-10-09). The repo has no test suite.
Do not write, add, run, restore, or require tests.
Remove test and test coverage requirements when you find them.
Use build, format, and lint checks.
Situation: `no-tests-all-repos-20261009`.

Run from the **workspace root** (`EdgeVector/lastdb`), not this subdirectory.

```bash
cargo build -p fold_db
cargo clippy -p fold_db --all-targets -- -D warnings
cargo fmt -p fold_db
```

For the product daemon/CLI:

```bash
cargo build -p lastdb_node
```

PR / merge: use **GitHub** (`gh -R EdgeVector/lastdb`).
The required check is `ci-required`. The old fold repo is frozen. Do not open PRs there.
LastGit and the Forgejo copy are retired.

## Ask the brain for anything project-specific

This file holds commands and cutover boundaries only. For architecture, error
idioms, feature flags, the security model, and gotchas, ask the brain
(`brain ask "<q>"` / `brain get <slug>`):

- `concepts-lastdb-agent-access-model` — Dynamo-style access patterns (not SQL)
- `concepts-fold-error-handling-idioms` — `From<SourceError>` + `?`, error types
- `concepts-fold-build-test-ci-safety` — fmt gate, builds, lints
- `concepts-fold-schema-service` — the schema service fold_db consumes as a client
- `concepts-observability-conventions` — tracing/redaction/spawn/egress rules
- `completed-programs` — closed product surfaces (Desktop/Tauri/web UI — do not resurrect)

See also: workspace-root `CLAUDE.md`, `README.md`, `../schema_service/openapi.yaml`.

Do not write, restore, or run tests. Use product build, format, and lint checks.
