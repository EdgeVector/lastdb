<!-- last-stack:generated-evidence:start (managed by last-stack setup; edits inside are overwritten) -->
## Keep raw output outside Git (Tom, 2026-10-23)

- Do not commit raw logs, samples, request/reply dumps, generated receipts, or compressed copies.
- Use the CI summary and artifacts as the validation record. Link the run in the PR.
- Do not create a separate report or `artifact.json` for routine CI. This rule replaces older proof-file requirements.
- Keep useful source scripts and durable design conclusions in Git. Optional evidence files have a cap of 10 files and 100,000 bytes per PR.
- CI keeps raw output in the runner's temporary directory. Other output uses a unique directory outside Git. Use `LAST_STACK_EVIDENCE_ROOT`, or `${XDG_STATE_HOME:-$HOME/.local/state}/last-stack/artifacts/<repo>/`.
- CI records the source revision, download URL, checksum, and expiration. Archive selected results when long retention is necessary.
- Keep secrets out of all output. Verify retained output before cleanup.
- Keep the required `generated-evidence` check and output ignores. Do not evade the check. Product-data exceptions require an exact path and reason in the accepted base policy.

Policy: https://github.com/EdgeVector/.github/tree/main/generated-evidence.
<!-- last-stack:generated-evidence:end -->

# AGENTS.md — EdgeVector/lastdb monorepo

**Read `CLAUDE.md` in this directory first.** It is the single instruction file
for this repo — build commands, the local agent loop, and the standing
rules — and every agent (Claude, Codex, or otherwise) should follow it.

The one thing worth repeating here, because getting it wrong invalidates a
whole design before a line is written:

> **LastDB is Dynamo-style NoSQL, NOT a table/SQL database.** No scan, no
> WHERE-style field filter, no JOIN. Point get by exact key is O(1); a range
> under one hash is O(log M); a full scan is not an operation. A schema is a
> catalog, not a table — each *field* has its own molecule, and a "row" is
> assembled at read time from the field molecules sharing a key.

Before designing or planning anything against LastDB, read:

- `docs/lastdb-canonical-model.md` — CANONICAL (schema → field → molecule →
  atom → file, proteins, the no-scan contract). If another doc or a code
  comment disagrees with it, it wins; fix the other.
- `docs/lastdb-agent-access-model.md` — day-to-day do / don't rules.
- `docs/lastdb-access-complexity-requirements.md` — the complexity law.

Brain equivalents (authoritative, kept in sync): `concepts-lastdb-canonical-model`,
`concepts-lastdb-agent-access-model`, `requirement-lastdb-access-complexity`.

## Code size limits (CI fails a PR that breaks them)

Source file: 400 lines. Function: 100 lines. CI checks
only the files and functions your PR touches. A new file or function over the
limit fails. A file or function already over the limit may grow by at most 10
lines per PR, so put new code in a new module or helper. Aim lower than the
limit. Full rule and the local check commands: `CLAUDE.md`, section
"Code size limits".

## No tests (Tom, 2026-10-09)

Do not write, restore, or run tests. Remove test and test coverage requirements
when you find them in CI, linters, agent instructions, or review rules.
Use format, syntax, type, lint, and product build checks.
This rule replaces all earlier test and test coverage requirements.
Situation: `no-tests-all-repos-20261009`.

### Scoped exception: canonical schema properties (Tom, 2026-10-28)

Tom explicitly requested automated schema-service stress, property, and concurrency
tests and approved the scoped CI PR and execution on 2026-10-28. Within this scope,
the 2026-10-09 test prohibition does not apply:

- `schema_service/crates/core/tests/` and the explicit Cargo target `canonical_properties`.
- `.github/workflows/schema-canonical-properties.yml` for that target only.

Keep every other test target retired. Use CI for Rust builds and execution.
The scope uses isolated memory stores and grants no primary, shared service load,
deployment, or production authorization.

### Scoped extension: canonical operational checks (Tom, 2026-10-28)

Tom requested the follow-ups for real-node molecule/protein links, cloud races,
the actual semantic model, recovery, and migration limits. This explicitly
authorizes only `scripts/schema_checks/`, the `s3_canonical_properties` target
in `schema_service_s3`, the `canonical_semantics` example in `server_shared`,
and `.github/workflows/schema-operational-checks.yml` in addition to the scope above.
Use CI for all builds and execution. Use fresh generated homes and a dedicated
model cache. No primary copy, archive access, production request, or deployment.
Tom separately approved the dedicated accepted-main AWS role and disposable
`lastdb-schema-ci-152335099025-*` check buckets on 2026-10-28. Keep all other tests retired.

### Scoped extension: atom history product proof (Tom, 2026-10-29)

Tom approved the fresh-home atom history proof as a required check on 2026-10-29.
The proof runs the public CLI and daemon on a fresh generated home.
This authorizes only:

- `scripts/operations/atom_history.py`
- `.github/workflows/atom-history.yml`
- the `atom_history` job in `.github/workflows/ci-required.yml`

Use CI for the build and the proof. No primary copy. Keep every other test target retired.

## Cursor Cloud specific instructions

The image already has Rust (`rust-toolchain.toml`, 1.96.1) and Node 22.
`install` adds `libssl-dev` (the `openssl` crate), Ruby, and the pinned
tree-sitter packages from `CLAUDE.md`, then builds `lastdb` / `lastdbd` and
`lastdb_app_sdk`. `start` runs `lastdbd` in tmux session `lastdbd_dev` against
`$HOME/.lastdb-dev` (leave `~/.lastdb` alone).

- Health: `target/debug/lastdb --data-dir ~/.lastdb-dev status` (exit 0 when
  `<home>/data/folddb.sock` answers).
- Prove a write with a keyed read: `lastdb get <schema> --key-hash <id>`, or
  `POST /api/query` with `filter.HashKey`. An unfiltered query is rejected.
- Do not pass `--all-features`.
- Checks for this repo: `cargo fmt --all -- --check`,
  `cargo build --locked -p lastdb_node --bin lastdb --bin lastdbd`, and
  `npm run typecheck`, `npm run lint`, and `npm run build` in `lastdb_app_sdk`.
