<!-- last-stack:generated-evidence:start (managed by last-stack setup; edits inside are overwritten) -->
## Keep raw evidence outside Git (Tom, 2026-10-23)

- Do not commit raw logs, timing samples, request/reply dumps, generated receipts, or compressed copies.
- This rule overrides an older instruction to commit every proof file or preserve raw output in Git.
- Keep raw output outside the source tree, under `LAST_STACK_EVIDENCE_ROOT` when set.
- The default root is `${XDG_STATE_HOME:-$HOME/.local/state}/last-stack/artifacts/<repo>/`.
- Use a unique directory for each run. Benchmark defaults must use this root or the CI runner's temporary directory.
- Add ignored output directories as a local guard. An ignore rule does not exempt a committed file from CI.
- Commit a short Markdown report, useful source scripts, and an optional compact `artifact.json`.
- State the result, method, source revisions, commands, and limits in the report.
- Upload raw output as a workflow artifact. Use a durable archive when review or recovery needs long retention.
- Record the download URL, SHA-256, source revision, and retention period. Verify the upload before local cleanup.
- Keep secrets out of reports, receipts, and uploaded output.
- A PR permits at most 10 changed evidence files and 100,000 total evidence bytes.
- The required `generated-evidence` check reads committed files. Force-add and compression do not override it.
- Unchanged historical output and deletions do not count. Product JSON configuration remains permitted.
- An intentional product data exception needs an exact path and a reason in the accepted base policy.
- Do not weaken the check, split output across small files, or move it to another PR to evade the rule.

Policy and receipt format: `https://github.com/EdgeVector/.github/tree/main/generated-evidence`.
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
