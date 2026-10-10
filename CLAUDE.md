# CLAUDE.md — EdgeVector/lastdb monorepo

> **READ FIRST BEFORE DESIGNING OR PLANNING — LastDB is Dynamo-style NoSQL,
> NOT a table/SQL database.** There is no scan, no WHERE-style field filter,
> no JOIN. Everything is reached by a named key.
>
> | Operation | Shape | Complexity |
> |---|---|---|
> | Point get | exact hash (or hash+range) key | **O(1)** |
> | Range under one hash | prefix/between on the range key, hash fixed | **O(log M)** |
> | Multi-get | K exact keys | **O(K)** |
> | Full scan | — | **not supported** |
>
> Storage ladder: **schema → field → molecule → atom → file**. A schema is a
> catalog, not a table; each *field* has its own molecule (an index from key
> coordinates to the current atom); a "row" is assembled at read time from the
> field molecules sharing the same key. Secondary access patterns are protein
> member molecules for shared fields (dual-write only a thin projection),
> never a filter over everything.
>
> **Read before you design:**
> - `docs/lastdb-canonical-model.md` — CANONICAL. If any other doc or code
>   comment disagrees with it, it wins; fix the other.
> - `docs/lastdb-agent-access-model.md` — day-to-day usage rules (do / don't).
> - `docs/lastdb-access-complexity-requirements.md` — the complexity law.
>
> Brain equivalents (authoritative, kept in sync):
> `brain get concepts-lastdb-canonical-model` ·
> `brain get concepts-lastdb-agent-access-model` ·
> `brain get requirement-lastdb-access-complexity` ·
> https://thelastdb.com/docs/agent-access-model

One cargo workspace: `fold_db` (core), `schema_service` (cloud schema
registry), `lastdb_node` (the `lastdb`/`lastdbd` Mini daemon — the shipped
product), plus shared `lastdb_host` / `lastdb_uds` /
`lastdb_identity` / `folddb_profile` / `app_identity_crypto` /
`observability`. (The `fold_db_node` desktop node, Tauri
app, and `file_to_markdown` were deleted in the Mini-only cutover 2026-07-12;
restore point: branch `archive/desktop-dmg-pre-removal`.)

## Canonical commands

**The tests are deleted** (Tom, 2026-10-09: they flaked and gave no signal).
The required gate (`ci-required`) runs fmt, clippy, lints, and builds only, and
the repo has no test suite to run. CI and the lints do not require tests or
test coverage.

```bash
cargo build --workspace
cargo clippy --workspace --lib --bins -- -D warnings
cargo fmt --all                              # ALWAYS run before committing
scripts/install-hooks.sh                     # fmt + no-URL pre-commit hook
```

## Local agent loop (package-scoped — default while iterating)

Prefer **narrow** builds against the crate you touched. Full workspace is
for pre-PR / CI confidence, not every edit.

```bash
# Build only the package + its deps (examples)
cargo build -p lastdb_node
cargo build -p schema_service_core
cargo build -p fold_db

scripts/ci/run-db-perf-guard.sh            # Criterion setup/measure/compare; same durable target + wait/refuse + host lock
scripts/ci/with-fold-host-cargo-lock.sh -- cargo …  # agents/routines: serialize heavy fold cargo behind the probe

# Full workspace — use before opening/pushing a PR, or when shared crates move
cargo clippy --workspace --lib --bins -- -D warnings
```

How to pick `-p`: use the card's `Surfaces:` line and the crate that owns the
edited files (`lastdb_node`, `fold_db`, `schema_service_core`, …). Workspace
members are listed in the root `Cargo.toml`.

**Linker note (macOS):** do **not** install `mold` for local Mach-O builds —
Homebrew mold no longer supports Mach-O. Rely on sccache (`~/.cargo/config.toml`)
for compile cache; use package-scoped builds for link feedback speed.
`mold` remains useful on **Linux** CI hosts only.

CI lints:

```bash
bash scripts/lints/lint-redaction.sh        --scope=fold_db/crates --scope=schema_service/crates --scope=lastdb_node --scope=lastdb_host --scope=lastdb_uds --scope=lastdb_identity
bash scripts/lints/lint-spawn-instrument.sh --scope=schema_service/crates --scope=lastdb_node --scope=lastdb_host
bash scripts/lints/lint-tracing-egress.sh   --scope=fold_db/crates --scope=schema_service/crates --scope=lastdb_node --scope=lastdb_host --scope=lastdb_uds --scope=lastdb_identity --strict
bash scripts/lints/lint-byte-slice.sh       --scope=fold_db/crates --scope=schema_service/crates --scope=lastdb_node --scope=lastdb_host --scope=lastdb_uds --scope=lastdb_identity
bash scripts/lints/lint-no-hardcoded-urls.sh
```

Run the Mini daemon locally: `cargo run -p lastdb_node --bin lastdbd`.

PR / merge: this repo is canonical on GitHub since 2026-09-29 (brain
`decision-2026-09-29-fold-venue-back-to-github`). Open PRs with
`gh pr create -R EdgeVector/lastdb --head <branch>` and merge with
`gh pr merge <n> -R EdgeVector/lastdb --auto --squash`. The required check is
`ci-required`. The Forgejo copy is archived: do not push to it and do not use
`last-stack-forge-api` for fold.

> NEVER `--all-features` in a sandbox/cloud run (network model downloads).
> See the build/test record below.

## Code size limits (CI-enforced on the lines you change)

Write Rust so that small, focused files and functions are the default. CI job
`diff_file_size` (`.github/workflows/ci-required.yml`) fails a PR that breaks
these limits. It checks only the files and functions the PR touches. Old code
is not checked until you touch it.

| What | Limit |
|---|---|
| Source file | 400 lines |
| Function | 100 lines |

- A **new** file or function over the limit fails.
- A file or function that **crosses** the limit (was under, now over) fails.
- A file or function that is **already over** may grow by at most 10 lines per
  PR, and may always shrink. Prefer to split it: move the new code into a new
  module or helper function.
- Aim lower than the limit. A function of about 30 lines and a file of about
  200 lines read well.
- Override only for generated code or a data table: put
  `lint:file-size-ok <reason>` (file) or `lint:fn-size-ok <reason>` (function)
  in the code. Do not use an override to avoid a split.
- Check before you push: `python3 scripts/lints/lint-diff-file-size.py --base origin/main`
  and `python3 scripts/lints/lint-diff-fn-size.py --base origin/main` (the second
  needs `pip install tree-sitter==0.23.2 tree-sitter-rust==0.23.2`).
- Brain: `preference-fold-code-size-limits`.

## Ask the brain for anything project-specific

CLAUDE.md holds commands only. For architecture, security model, build/test
caveats, current gates/approvals, and gotchas, ask the brain
(`brain ask "<q>"` / `brain get <slug>`):

- `concepts-fold-build-test-ci-safety` — fmt gate, offline-safe test sweep, lints (incl. byte-slice ban), workspace path-dep model, downstream submodule pointer policy.
- `concepts-fold-dev-run-and-app-isolation` — dev-node flags, app-isolation opt-out, socket-only-default vs `--with-tcp`, keyless dev, log volume knobs, Lambda/CDK.
- `concepts-fold-error-handling-idioms` — `From<SourceError>` + bare `?` and `.context()` via `fold_db::error_context::ResultExt`; error types; coding standards.
- `concepts-fold-endpoint-registry` — `folddb_profile/environments.json` single source of truth, `build.rs` codegen, no-hardcoded-urls lint.
- `concepts-fold-trust-boundary-loopback` — loopback owner-context invariant + target trust model + door-2 release gate.
- `concepts-fold-db-node-internals` — fold_db core architecture, storage, schema system, feature flags, AI provider config.
- `concepts-fold-schema-service` — schema_service crates, cloud-only deploy, dual-signal canonicalization gate.
- `concepts-observability-conventions` — tracing/redaction/spawn/egress rules.
- `concepts-edgevector-repo-layout`, `projects-monorepo-consolidation` — layout + migration history.
- `concepts-claude-shell-gotchas-edgevector` — zsh glob / sandboxed PATH.
- `preference-lastdb-atom-size-hard-limit-64kib` — atom content size hard limit (64 KiB default, `LASTDB_MAX_ATOM_CONTENT_BYTES` env override, docs path).

Per-crate command files: `fold_db/CLAUDE.md`, `schema_service/CLAUDE.md`.
