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

> **LastDB is Dynamo-style NoSQL, NOT a table/SQL database.** No WHERE-style
> field filter, no JOIN. Point get by exact key is O(1); a range under one hash
> is O(log M). One whole-schema read exists: a paged listing of a schema's
> primary keys (`keys_only`, `docs/app-api.md`), followed by reads by key. A schema is a
> catalog, not a table. Each primary key holds one record: all fields of the
> schema in one stored value. A named alternate key finds that same record.

Before designing or planning anything against LastDB, read the brain records:

- `concepts-lastdb-canonical-model` — CANONICAL (schema → record → file blob,
  named keys, keyed reads and the key listing). If a code comment disagrees with
  it, it wins; fix the comment.
- `concepts-lastdb-agent-access-model` — day-to-day do / don't rules.

The docs folders were removed on 2026-11-02. `docs/app-api.md` is the app
API contract. `docs/designs/` keeps designs for work in progress. Read a
removed page with `git show 13a2cf188:<path>`.

## Preserve whole-database sharing (Tom, 2026-11-14)

Read the [sharing design](docs/designs/shared-database-mutation-logs.md) before
changing this path. That document is the decided state: the mutation log is the
database, the proposal log is separate, and the two settings are independent
(Tom, 2026-11-18).

The mutation log is the database. Every entry in it is part of the database.
A caller who may read is served every entry. No entry is hidden because of who
wrote it. ExeMem alone decides who may append and what a reader is served. A
member's node applies every entry it is served. It does not reject an entry
because of who wrote it.

`read` (`members` or `anyone`) says who may read the mutation log, the snapshot,
and file ranges. `write` (`proposals` or `direct`) says where an append by an
active member with the Submit role goes from now on. `proposals` means the
proposal log. `direct` means the mutation log. A publisher may change either
setting at any time, in any direction. No rule depends on a combination of the
two, or on what either log already holds. `anyone` with `direct` is valid.

The publisher may always append to the mutation log. An active Submit member
may append to the mutation log only while `write` is `direct`. While `write`
is `proposals`, that member appends to the proposal log, and the log model
refuses a non-publisher append to the mutation log. There is no older path
that puts a member's proposal on the mutation log.

The default is read `members` and write `proposals`, including when no settings
exist. Keep these requirements for that default. Change them only at Tom's
explicit direction.

- A personal database has one log, the mutation log. A shared database has the
  mutation log and the proposal log under one database ID. Their positions are
  independent. Do not combine them, or keep a mixed-log path. There is no
  compatibility with a retired log format. Entries already on the proposal log
  stay until the publisher settles them.
- Every active member can read every mutation-log entry and, while `write` is
  `proposals`, append an encrypted proposal, even with an empty write grant.
  Only the publisher can read or settle proposal-log entries. Members cannot
  read even their own entries on that log.
- Keep the publisher-signed, revisioned configuration per database. ExeMem
  uses the active roster and the settings for transport. The publisher's LastDB
  uses the confirmed member-write map on the publisher's machine to check each
  proposal after downloading and decrypting it. A schema that is not in the map
  is `owner_only`. The first active proposal of a drain confirms that map with
  the service and reuses it for the page. When that confirmation fails, the
  proposal is not accepted and not discarded, so it stays for the next drain.
  An app's displayed permissions grant no write authority.
- Commit allowed changes locally and append them to the mutation log under the
  publisher's key before marking their proposal-log entries processed. Discard
  denied or invalid changes. A queue receipt confirms delivery to the proposal
  log.

The three names, as examples of configurations an app might choose:

- Share: `members` and `proposals`. This is the default.
- Deliver: `anyone` and `proposals`. The reader has no member entry and holds
  the read key from a link. A Submit member still appends to the proposal log
  while `write` is `proposals`.
- Org: `members` and `direct`. An active member with Submit appends to the
  mutation log. ExeMem checks only that the member may write. Order is the
  order ExeMem assigns at append. A reader is served that entry with every
  other entry in the mutation log.

In every case only the publisher publishes a snapshot. A receiver fetches that
snapshot and replays the mutation log on top. The settings do not change bytes
already on the mutation log, and they do not refuse a later settings change.

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

### Scoped extension: record home product proofs (Tom, 2026-10-30/31)

Tom approved the record model plan (#911, item 9): a proof in CI on a fresh home with a
synthetic seed, with product operations, latency comparisons and `benchmark-complete`.
The workspace rule of 2026-10-29/30 also asks for CI product operations before merge.
The proofs run the public CLI and daemon on fresh generated homes.
This authorizes only:

- `scripts/operations/record_home.py`
- `.github/workflows/record-home.yml`
- the `record_home` job in `.github/workflows/ci-required.yml`

The same limits apply: CI only, generated data, no primary copy. Keep every other test target retired.

### Scoped extension: sharing CI and daily integration (Tom, 2026-11-10)

Tom requested: "update CI so that it's always testing for the sharing feature.
It should always test to make sure that share works." He then specified:
"Yeah let's add a daily test instead" rather than live cloud work per push.

The fresh-home record/Share/Files product operation runs on every required CI
invocation, without a path-based skip. Live signed invitations and recipient
registration use the accepted-main DEV product operation.
The existing four-case LastDB–ExeMem DEV suite runs daily, not on every push.
Hourly owned-scope cleanup remains separate from the daily product result.
Cloud work stays serialized, isolated from pull-request code, and within the
existing source-gate, owned-scope, credential, retirement and cleanup limits.
These checks do not substitute native multiwriter sync for app invitation proof.
No local builds, runtime proofs, primary changes or unrelated test restoration.

### Scoped extension: stable release qualification (Tom, 2026-11-03)

Tom explicitly requested: "Move the release flow into CI, fix the missing app
compatibility proofs, keep the one-hour qualification gate, and leave my
primary unchanged."

This authorizes the release qualification and app compatibility operations in
`.github/workflows/stable-release.yml`, `.github/workflows/app-install-smoke.yml`,
`scripts/release/stable_plan.py`, `scripts/release/stable_publish.py`,
`scripts/release/nightly_package.py`, and its packaged-program helper
`scripts/release/nightly_probe.py`. They use only fresh generated CI homes and public packaged
programs.

Keep all other test targets retired. No local LastDB builds, benchmarks, or
runtime proofs. No primary upgrade, restart, reset, copy, or backup change.

Tom decided on 2026-11-16 to stop CI publication of app listings. The client
installs an app from any signed list in its supported API range, so a new
LastDB API number does not need a new listing. A CI republication also carries
no `access` declaration, which the installer refuses for an app that ran
restricted. The stable release does not call `POST /v1/ci/apps` or
`POST /v1/ci/authorize`. `scripts/release/stable_app_publish.py` and
`.github/workflows/app-compatibility-publish.yml` are removed.

The compatibility proof stays. Job `app-compatibility` in
`.github/workflows/stable-release.yml` runs
`scripts/release/stable_plan.py compatibility`, and the one-hour qualification
still has to pass before a Homebrew promotion. The release reads signed lists
to choose which app commits to prove. It does not write listings.

Listings already signed by the former CI publisher key stay valid. An owner
can still publish over one of those listings with the owner key. That rule is
`authorized_transition` in
`lastdb_node/src/bin/lastdb/app_registry_index/mod.rs`, and the pinned CI
publisher keys stay in `lastdb_node/environments.json`.

### Scoped extension: signed snapshot round trip (Tom, 2026-11-13)

Tom asked for an end-to-end DEV check so the owner signed-database snapshot
backup and restore path cannot regress. That path was deleted once before, in
exemem-infra #797 and lastdb #1541.

This authorizes only:

- `scripts/cloud_backup/snapshot_gate.py`
- `.github/workflows/snapshot-roundtrip-dev.yml`
- the `snapshot-roundtrip-dev` precondition in `scripts/release/nightly_plan.py`,
  `.github/workflows/nightly-release.yml`, and `.github/workflows/stable-release.yml`

The check uses a fresh generated home, synthetic records, the public CLI and
daemon, and the existing `cloud-backup-dev` environment. It runs on a push to
main, the daily schedule, and `workflow_dispatch`. Pull requests receive no
cloud credentials. Keep every other test target retired. No local LastDB
builds, benchmarks, or runtime proofs. No primary data.

## Cursor Cloud specific instructions

Owner byte backup and its PROD restore workflow are gone. Personal and private
databases sync and restore through the signed database-id log. Do not add a
second backup path, a flag, or a migration.

The image already has Rust (`rust-toolchain.toml`, 1.96.1) and Node 22.
`install` adds Ruby and the pinned
tree-sitter packages (`tree-sitter==0.23.2`, `tree-sitter-rust==0.23.2`), then builds `lastdb` / `lastdbd` and
`lastdb_app_sdk`. `start` runs `lastdbd` in tmux session `lastdbd_dev` against
`$HOME/.lastdb-dev` (leave `~/.lastdb` alone).

- Health: `target/debug/lastdb --data-dir ~/.lastdb-dev status` (exit 0 when
  `<home>/data/folddb.sock` answers).
- Prove a write with a keyed read: `lastdb get <schema> --key-hash <id>`, or
  `POST /v1/read` with one `key`. A read names a key or a range.
- Do not pass `--all-features`.
- Checks for this repo: `cargo fmt --all -- --check`,
  `cargo build --locked -p lastdb_node --bin lastdb --bin lastdbd`, and
  `npm run typecheck`, `npm run lint`, and `npm run build` in `lastdb_app_sdk`.
