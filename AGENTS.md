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
  atom → file, named keys, and the no-scan contract). If another doc or a code
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

### Scoped extension: atom history product proof (Tom, 2026-10-29)

Tom approved the fresh-home atom history proof as a required check on 2026-10-29.
The proof runs the public CLI and daemon on a fresh generated home.
This authorizes only:

- `scripts/operations/atom_history.py`
- `.github/workflows/atom-history.yml`
- the `atom_history` job in `.github/workflows/ci-required.yml`

Use CI for the build and the proof. No primary copy. Keep every other test target retired.

### Scoped extension: record home product proofs (Tom, 2026-10-30/31)

Tom approved the record model plan (#911, item 9: "Proof in CI. A fresh format 7 home
on a synthetic seed: product operations, latency comparisons and `benchmark-complete`").
The workspace rule of 2026-10-29/30 also asks for CI product operations before merge.
The proofs run the public CLI and daemon on fresh generated homes.
This authorizes only:

- `scripts/operations/record_home.py`
- `scripts/operations/record_peer_sync.py`
- `.github/workflows/record-home.yml`
- the `record_home` job in `.github/workflows/ci-required.yml`

The same limits apply: CI only, generated data, no primary copy. Keep every other test target retired.

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
