# LastDB

Use one schema with named alternate keys when several keys must find the same records.
Keep canonical field definition reuse separate from mutable record values.
Existing homes move by a copy into a fresh home, not by an in-place conversion.

Search delivery is off by default. Use `lastdbd --enable-search` when LastSeek needs the change feed.

The LastDB monorepo — one cargo workspace for the platform: `fold_db`
(core library), `lastdb_node` (Mini product: `lastdb` + `lastdbd`),
`schema_service` (schema registry), shared host/identity crates,
and related tools.

**Product today is LastDB Mini only.** The headless daemon is `lastdbd`
and the CLI is `lastdb`, both built from `lastdb_node`. There is no
desktop UI, Tauri app, or DMG release train in this tree. The pre-Mini
desktop surface lived on branch `archive/desktop-dmg-pre-removal` and
must not be resurrected here.

Compatibility note: several crate names, package names, command shims,
environment variables, hostnames, and on-disk paths still contain
`fold_db`, `folddb`, or `FOLDDB_*`. Those are literal names preserved for
existing installs and scripts; user-facing product copy should say LastDB.

Migrated from four separate repos on **2026-05-13**. Mini-only cutover
deleted the desktop node on **2026-07-12**.

## Layout

```
.
├── Cargo.toml                  # workspace root, path deps for everything internal
├── .github/workflows/          # GitHub release train (tag push → Homebrew)
├── fold_db/                    # core LastDB library (was EdgeVector/fold_db)
│   └── crates/{core,observability}
├── lastdb_node/                # Mini product: lastdb + lastdbd
├── lastdb_host/                # owner-socket host primitives
├── lastdb_uds/                 # Unix-domain-socket transport
├── lastdb_identity/            # node identity helpers
├── lastdb_app_sdk/             # app SDK (separate docs card may refine this)
├── schema_service/             # advisory schema vocabulary + Lambda
│   └── crates/{schema_types,core,server_lambda}
├── vendor/laststore/           # disk engine: disk format v2 (`laststore::disk_run`)
└── docs/                       # designs for work in progress
```

## What's deployed where

| Component | Where the source lives | Where it deploys |
| --- | --- | --- |
| LastDB Mini (`lastdb` + `lastdbd`) | `lastdb_node` | Homebrew `edgevector/lastdb/lastdb` via [`.github/workflows/stable-release.yml`](.github/workflows/stable-release.yml) → `EdgeVector/homebrew-lastdb` |
| schema_service Lambda | `schema_service/crates/server_lambda` | schema registry via [EdgeVector/schema-infra](https://github.com/EdgeVector/schema-infra) (submodules this repo) |

Releases: nightly GitHub prereleases from `.github/workflows/nightly-release.yml`. There is no DMG
or desktop-app channel.

## Record size limit

A local write uses the 64 KiB record cap. An oversized record returns HTTP 413
`field_value_too_large` with `scope: "record"`. `lastdb status` also prints
`max_peer_field_value`. No write path reads that number.

## Build

```bash
cargo build --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all
```

Default product scope (no `--workspace`) is the Mini-related members in
`default-members` (includes `lastdb_node`, `fold_db`, schema crates, etc.).

### Local `sccache` wrapper

If `~/.cargo/config.toml` enables `rustc-wrapper = "sccache"`, keep the
daemon resident during long workspace builds:

```toml
[env]
SCCACHE_IDLE_TIMEOUT = "0"
```

The default `sccache` idle timeout can let the daemon exit during a build
lull. Under heavy local concurrency, the next rustc invocation can race the
dead or dying daemon and fail with `sccache ... (exit status: 254)` even
though the crate itself is unchanged. `SCCACHE_IDLE_TIMEOUT = "0"` removes
that death-during-lull race; use `RUSTC_WRAPPER= cargo ...` as a per-build
escape hatch if a project needs to bypass the wrapper.

## Local development

### LastDB Mini (`lastdb` / `lastdbd`)

From the workspace root:

```bash
# release-shaped binaries (what Homebrew ships)
cargo build --release -p lastdb_node --bin lastdb --bin lastdbd

# point a throwaway home — never use Tom's live ~/.lastdb as a first proof surface
export LASTDB_HOME="$(mktemp -d /tmp/lastdb-dev-XXXXXX)"
./target/release/lastdbd
# in another shell:
./target/release/lastdb status
# exit 0 only when the owner socket answers /health; exit 1 prints
# `lastdbd: not reachable — <reason>` on line 1
```

Public install path (end users):

```bash
brew install edgevector/lastdb/lastdb
brew services start lastdb   # runs lastdbd
lastdb status
```

Use `lastdb --help` to see the commands in the installed release.

| Command | Use |
|---|---|
| `status` | Check whether the daemon is reachable and inspect its health. |
| `schema` | Inspect an installed schema and its keys. |
| `get` | Read an exact record. Add `--hydrate FILE` to save its verified file. |
| `get-keys` | Page through keys or list named alternate keys under a hash. |
| `mutate` | Submit a conditional write and supply the record’s file when needed. |
| `app` | Browse the signed registry and manage apps. |
| `backup` | Configure backup and restore an accepted recovery point. |
| `share` | Create a publication, invite readers, and optionally grant restricted contributor writes. |
| `db` | List the local database and accepted shares. |
| `admin` | Diagnose or maintain a home. |

Supply file bytes in the same `mutate --request-file` operation as the record.
Use a JSON object with `mutation` set to the complete conditional request and
`file` set to an object with `source` and optional `name` and `media_type`. The
source path is relative to the request file, or to the working directory for
stdin. A record with a required file cannot be written without its file. A
bare record replacement keeps the existing file. A replacement with
`file: null` in the same envelope removes an optional file.

### Share files

LastDB Files shares a separate encrypted database with read-only recipients.
Use absolute paths for `OWNER`, `PUBLICATION`, and `RECIPIENT`.
`OWNER` is an existing home with saved Exemem credentials for the selected
environment. Create the publication at a new path with an existing parent
outside that home using `lastdb --data-dir OWNER share create --into
PUBLICATION --env dev`. Run
`lastdbd --data-dir PUBLICATION --share-owner-home OWNER` to publish
automatically. Stopping that daemon pauses publication.

In another terminal, add a file with `lastdb --data-dir PUBLICATION share files
add FILE`. The app accepts regular files of any type, up to 128 files and
256 MiB total per share, with a 32 KiB file catalog.

The recipient needs only a local LastDB identity. In DEV, create one at a new path
with an existing parent using `lastdb admin backup init --into RECIPIENT
--recovery-phrase-out RECIPIENT/recovery-phrase.txt`. The recipient exports a
public contact with `lastdb --data-dir RECIPIENT share contact --out CONTACT`.
Verify the returned recipient key through a trusted channel and issue an
invitation with
`lastdb --data-dir OWNER share invite --home PUBLICATION --recipient CONTACT
--recipient-key RECIPIENT_KEY --out INVITATION`. If the daemon is still
publishing its first backup, wait and retry. The recipient verifies the
publisher key from `share create` through a trusted channel. Run
`lastdbd --data-dir RECIPIENT` and accept the invitation into that node.

```sh
lastdb --data-dir RECIPIENT share files receive INVITATION --publisher PUBLISHER_KEY --name trip
lastdb --data-dir RECIPIENT db list
lastdb --data-dir RECIPIENT --db trip share files list
lastdb --data-dir RECIPIENT --db trip share files save --file FILE_ID --out PATH
```

The daemon follows the publication automatically. Once `db list` shows it
as ready, use its local name or the database ID returned by `receive`.
`share accept` registers the same database. List and save use the latest
verified data while offline, including after the invitation expires.

Invitations authorize downloads for 24 hours by default. Set `share invite
--expires-in-hours` to 1–168 hours when issuing one. To resume updates after
expiry, accept a fresh invitation for the same publication on the same node.
Existing app-managed caches still support their explicit `--cache` and
`--handle` workflow. Reaccept the invitation to register one for `--db`.

### Share with restricted contributors

The app decides which records go into the publication and which recipients can
contribute to each schema. Create the publication with
`share create --write-mode publisher-validated` and run the same automatic
publisher as above. The default mode, `publisher-only`, gives the publisher
exclusive write access.

Read invitations and write grants are separate. To let a verified recipient
add comments or remove their own reactions, give `share contributors grant`
a JSON rules file like this one, using the installed schema's exact definition
ID. A rule action is `create`, `replace`, or `delete`. `purge` is never granted.

```json
[
  {
    "schema": "photos/Comments",
    "definition_id": "<64-character schema definition ID>",
    "action": "create"
  }
]
```

### Address a shared database

Run the recipient's daemon and accept the invitation into that node. The
node registers the database immediately and marks it ready once it has
verified the first recovery point. It follows updates automatically while
running. Accept a replacement invitation on the same node to renew access.
Readers can use the last verified copy offline. A received database serves
primary and named alternate keys, ascending ranges under one hash, and
revision-checked inline files. The replica refuses scans, descending queries,
and queries that filter records after key lookup.

```sh
lastdb share accept INVITATION --publisher PUBLISHER_KEY --name trip
lastdb db list
lastdb --db trip get photos/Comments --key-hash photo-42 --key-range comment-7
lastdb --db trip get-keys photos/Comments --key-hash photo-42
```

`--data-dir` selects the daemon home. `--db` selects its database by ID or
local name. Omitting it keeps the daemon's ordinary default. The publisher
continues to run the publication daemon for its separate home and can use
the publication ID there. An unknown database selector fails the request.
Accept the invitation again to register an existing Files share. If the
invitation has expired, use a fresh one from the publisher.

Attach a write grant once with `lastdb share contributors accept trip --grant
GRANT`, then use `lastdb --db trip mutate --request-file MUTATION`. Supply the
schema definition ID in the ordinary mutation JSON. The contributor grant
allows an absent create, or a replace or delete of a record that writer
created, at the record's expected revision. A `submitted` receipt confirms
upload to the publisher's private inbox. The publisher must accept the
optional revision field before a writer sends it. To retry
the same request after a transport failure, pass the same `--mutation-id`.
The publisher checks the current grant and commits allowed writes to its own
mutation journal, retaining the verified contributor key. `share contributors
revoke` stops future acceptance under that grant. Read invitations and issued
upload permissions expire independently.

Exemem serves encrypted Backup V2 recovery points and restricts stream
access. LastDB owns encryption and write validation. The publisher uses
snapshots to establish or replace a recovery base.

Required CI exercises local publication setup in both write modes, ordinary
database selection, and importing, listing and saving a multi-chunk file
with the built CLI and daemon. Separate
DEV operations verified automatic Files updates and three-node recovery of
accepted writes with private contribution streams.
[Files DEV proof](https://github.com/EdgeVector/lastdb/pull/1416) ·
[Accepted journal DEV proof](https://github.com/EdgeVector/lastdb/pull/1437)

Immediate read revocation, interrupted import recovery, and Org sharing remain
separate work.

Never restart Tom's primary `lastdbd` / brain from agent work. Never use
the live `~/.lastdb` home as the first place a candidate binary is proven —
use a fresh empty home with synthetic data (`lastdb-safe-upgrade --probe-only` /
smoke helpers). Never copy `~/.lastdb`.

### Schema service deploys

[EdgeVector/schema-infra](https://github.com/EdgeVector/schema-infra) owns the
CDK and the deploy scripts for the advisory schema service Lambda. It consumes this repository as a git submodule.

### Downstream submodule pointer policy

A downstream repo must pin its submodule of this repository to a commit
reachable from `EdgeVector/lastdb` `origin/main`. Do not deploy off-main,
cherry-picked, or dangling commits through a submodule gitlink; land the change
through a normal PR first, then bump the submodule pointer to that merged
commit.

## Why the monorepo (and what disappeared with it)

Before 2026-05-13 these crates lived in four repos with a diamond-shaped
git-rev dep graph. Two cron bots ran every 2h to keep revs in lockstep
(~4h end-to-end cascade lag) because cargo would otherwise compile two
copies of `fold_db` whenever revs drifted (the "dual-fold_db trap").
Path deps in a single workspace make that trap mathematically
impossible.

What got deleted with the migration:

- The cross-repo bump-cascade bot (3 workflows across 3 repos)
- A CI lint enforcing single-line `rev = "..."` pins
- ~80 lines of "dual-fold_db trap" warning comments
- A `[patch."https://github.com/EdgeVector/fold_db.git"]` workaround
- Three separate CI workflows (replaced by one)

What got deleted later (Mini-only cutover, 2026-07-12):

- Desktop/Tauri node and DMG release train (restore:
  `archive/desktop-dmg-pre-removal`)
- In-tree `file_to_markdown` product path as an active shipping surface

## Workspace dependencies — how they resolve

The root `Cargo.toml` declares `[workspace.dependencies]` with **path
deps** for every internal crate (`fold_db`, `observability`,
`schema_service_*`, …). Member crates pull them via
`fold_db = { workspace = true }`. Cargo unifies path deps by definition
— `cargo tree -i observability` shows one entry. **No `[patch]` section.**

## Observability conventions

When you touch any `tracing::*!`, `tokio::spawn`, `reqwest::Client::new()`,
or sensitive-field log site, conventions live in the workspace brain /
agent docs (observability-conventions). CI-enforced rules:

- **Structured fields** — `tracing::info!(field = %value, "msg")`.
- **Redaction** — `password / token / api_key / secret / auth_token / email / phone / ssn` MUST go through `redact!()` / `redact_id!()`. Override per-line: `// lint:redaction-ok <reason>`.
- **`tokio::spawn`** — chain `.instrument(Span::current())` or `.in_current_span()`. Bare spawn fails CI.
- **Outbound `reqwest`** — every `Client::new() / ::builder() / ::default()` needs a comment within 3 preceding lines: `propagate / loopback / skip-s3 / skip-3p`.

## Archived predecessor repos

These exist read-only and should not be cloned for active work:

- [EdgeVector/fold_db](https://github.com/EdgeVector/fold_db)
- [EdgeVector/schema_service](https://github.com/EdgeVector/schema_service)
- [EdgeVector/fold_db_node](https://github.com/EdgeVector/fold_db_node) (pre-Mini desktop node)
- [EdgeVector/file_to_markdown](https://github.com/EdgeVector/file_to_markdown)

Desktop/Tauri/DMG product code is also archived on branch
`archive/desktop-dmg-pre-removal` in this monorepo — not a live product path.

## License

Apache-2.0. See [`LICENSE`](LICENSE). Vendored third-party code under
`vendor/` keeps its own license files.
