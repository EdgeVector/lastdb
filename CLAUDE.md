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

# CLAUDE.md — EdgeVector/lastdb monorepo

## Builds and benchmarks run in CI (Tom, 2026-10-23)

This rule supersedes the local build commands and coordination rules below.

- Do not run benchmarks on Tom's machine.
- Do not run local builds, `cargo check`, or `cargo clippy` for LastDB.
- Make the code changes, then open or update the PR. Use GitHub Actions for builds and benchmarks.
- Do not acquire or wait for the host build or measurement lock.
- Stop and clean up a local benchmark that you own. Do not stop another agent's process.
- Tell your subagents about this rule. Review the PR results before merge.
- Lightweight source, syntax, and format checks can run locally. Tests remain retired.
- Product PRs require `benchmark-complete`: valid measurements and verified artifact uploads.
- Documentation-only PRs skip measurements. Main pushes and nightly runs keep their benchmark recipes.
- Slowdowns are advisory. Build failures, incomplete measurements, and failed uploads block the completion check.
- While CI waits, do not substitute a local build or benchmark.

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

## CI checks

**The tests are deleted** (Tom, 2026-10-09: they flaked and gave no signal).
The required gate (`ci-required`) runs fmt, clippy, lints, and builds only, and
the repo has no test suite to run. CI and the lints do not require tests or
test coverage.

Open or update a PR after the source changes. Read the required checks and
the `LastDB benchmarks` summary in GitHub. Product changes require completed
benchmarks and verified artifact uploads. Link the CI run in the PR; do not
commit a duplicate report or receipt. Fix failures through source commits.
Do not substitute a local build or benchmark while CI waits.

The benchmark workflow compares the exact base and candidate commits on one
GitHub machine. Read `scripts/benchmarks/README.md` for the standard and nightly
recipes, artifact contents, and measurement limits. Slowdowns remain advisory.

Format checks can run locally with `cargo fmt --all -- --check`.
Use `cargo fmt --all` to format source changes before a commit.

The commands below are CI audit commands. Do not run build or clippy commands
on Tom's machine.

The manual `.github/workflows/ci.yml` audit runs these commands. The normal
required PR gate uses `.github/workflows/ci-required.yml`; it does not run the
spawn or tracing-egress audits. Preserve and report inherited audit failures
without a gate override or unrelated source changes.

```bash
bash scripts/lints/lint-redaction.sh        --scope=fold_db/crates --scope=schema_service/crates --scope=lastdb_node --scope=lastdb_host --scope=lastdb_uds --scope=lastdb_identity
bash scripts/lints/lint-spawn-instrument.sh --scope=schema_service/crates --scope=lastdb_node --scope=lastdb_host
bash scripts/lints/lint-tracing-egress.sh   --scope=fold_db/crates --scope=schema_service/crates --scope=lastdb_node --scope=lastdb_host --scope=lastdb_uds --scope=lastdb_identity --strict
bash scripts/lints/lint-byte-slice.sh       --scope=fold_db/crates --scope=schema_service/crates --scope=lastdb_node --scope=lastdb_host --scope=lastdb_uds --scope=lastdb_identity
bash scripts/lints/lint-no-hardcoded-urls.sh
```

PR / merge: this repo is canonical on GitHub since 2026-09-29 (brain
`decision-2026-09-29-fold-venue-back-to-github`). Open PRs with
`gh pr create -R EdgeVector/lastdb --head <branch>` and merge with
`gh pr merge <n> -R EdgeVector/lastdb --auto --squash`. The required check is
`ci-required`, `generated-evidence`, and `benchmark-complete`. The Forgejo copy is archived: do not push to it and do not use
`last-stack-forge-api` for fold.

> NEVER `--all-features` in a sandbox/cloud run (network model downloads).
> See the build and CI record below.

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
- A split must not trade one kind of debt for another. Do not add
  `#[allow(clippy::too_many_arguments)]` (or any other new `#[allow]`) to make
  extracted helpers compile. Group the values the helpers share into a small
  context struct and pass that, or pick a split point with fewer inputs. The
  workspace already carries dozens of these allows from earlier splits; do not
  add to them, and remove the ones in code you touch.
- Check before you push: `python3 scripts/lints/lint-diff-file-size.py --base origin/main`
  and `python3 scripts/lints/lint-diff-fn-size.py --base origin/main` (the second
  needs `pip install tree-sitter==0.23.2 tree-sitter-rust==0.23.2`).
  CI also runs `python3 scripts/lints/lint-diff-new-allows.py --base origin/main`:
  a PR may not add more `too_many_arguments` allows than it removes (override one
  line with `lint:allow-ok <reason>`).
- Brain: `preference-fold-code-size-limits`.

## Current memory and disk boundary

Normal startup and direct logical constructors use native packed storage.
`CoreWarmSet` owns logical records, definitions, and complete selected keys.
The private native queue retains prepared disk bytes until flush. Ordinary
reads cannot use that queue. Native requests complete a durable snapshot and
their clean core memory publication before the ack. No collection timer delays them.

- `fold_db/crates/core/src/resident/` holds the core model, memory charges,
  the warm-set byte limit, and shared counters. Read its module doc and
  `docs/lastdb-rehydrate.md` before a change.
- `ResidentGraph`, `LogicalResidentSet`, and `NativeWriteGraph` are removed.
  No storage opt-in or `LASTDB_RESIDENT_MODE` choice selects the serving path.
- Empty homes initialize directly. Older nonempty formats receive an error;
  startup does not migrate data. Cloud activation stays disabled for this round.
- Dead-code passes use a compiler cascade and a zero-reference scan. Nothing
  else counts as proof.

## Ask the brain for anything project-specific

CLAUDE.md holds commands only. For architecture, security model, build and CI
caveats, current gates/approvals, and gotchas, ask the brain
(`brain ask "<q>"` / `brain get <slug>`):

- `concepts-fold-build-test-ci-safety` — fmt gate, non-test lints (incl. byte-slice ban), workspace path-dep model, downstream submodule pointer policy.
- `concepts-fold-dev-run-and-app-isolation` — dev-node flags, app-isolation opt-out, socket-only-default vs `--with-tcp`, keyless dev, log volume knobs, Lambda/CDK.
- `concepts-fold-error-handling-idioms` — `From<SourceError>` + bare `?` and `.context()` via `fold_db::error_context::ResultExt`; error types; coding standards.
- `concepts-fold-endpoint-registry` — `folddb_profile/environments.json` single source of truth, `build.rs` codegen, no-hardcoded-urls lint.
- `concepts-fold-trust-boundary-loopback` — loopback owner-context invariant + target trust model + door-2 release gate.
- `concepts-fold-db-node-internals` — fold_db core architecture, storage, schema system, feature flags, AI provider config.
- `concepts-fold-schema-service` — schema_service crates, cloud-only deploy, dual-signal canonicalization gate.
- `concepts-observability-conventions` — tracing/redaction/spawn/egress rules.
- `concepts-edgevector-repo-layout`, `projects-monorepo-consolidation` — layout + migration history.
- `concepts-claude-shell-gotchas-edgevector` — zsh glob / sandboxed PATH.
- `fold_db/docs/ATOM_CONTENT_SIZE_LIMIT.md` — atom content size hard limit (256 KiB default, `LASTDB_MAX_ATOM_CONTENT_BYTES` env override, absolute max 1 MiB) and which shape to use for packed fields, growing lists, and large bytes.

Per-crate command files: `fold_db/CLAUDE.md`, `schema_service/CLAUDE.md`.
