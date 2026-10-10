# AGENTS.md — EdgeVector/lastdb monorepo

**Read `CLAUDE.md` in this directory first.** It is the single instruction file
for this repo — build/test commands, the local agent loop, and the standing
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
