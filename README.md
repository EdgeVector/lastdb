# LastDB

Use one schema with named alternate keys when several keys must find the same records.
Keep canonical field definition reuse separate from mutable record values.
Existing homes move by a copy into a fresh home, not by an in-place conversion.

Every record write also appends one entry to a numbered list of changed keys, in the same transaction.
An entry holds a number, the schema, the record key, and a delete flag. It holds no values.
An app reads `POST /v1/x/changes` with `after`, `limit`, and `schemas`, keeps the returned `id` and `through`, and reads the records by key.
LastDB keeps the newest 250,000 entries and no state for a reader. When `gap` is true or `id` differs, the app rebuilds from key pages.

The app API is defined in `lastdb_node/app-api.json`.

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
├── .github/workflows/          # CI gates and GitHub release train (tag push → Homebrew)
├── app_identity_crypto/        # Ed25519 + JCS + signed registry-list helpers
├── cloud_backup_protocol/      # canonical native cloud backup wire contract
├── database_policy/            # spec catalog and share admission
├── fold_db/                    # core LastDB engine, pure model, and observability
│   └── crates/{core,model,observability}
├── lastdb_app_sdk/             # TypeScript app SDK (data client)
├── lastdb_node/                # Mini product: lastdb + lastdbd (host, UDS, sync)
├── mutation_log/               # pure published-log state machine
├── schema_service/             # advisory schema vocabulary + Lambda
│   └── crates/{schema_types,core,server_lambda}
├── scripts/                    # CI lints, benchmarks, release, and cloud checks
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

`lastdb status` prints the daemon, the home and, when a cloud connection is saved, a `Cloud sync:` line.
A `Snapshot:` line (after the `Cloud sync:` line when there is one) says where the newest snapshot of the personal database is: when it
was taken (local time), its files and size, the relay, and how far it covers this device
(`Snapshot: none yet` before the first). If a relay refuses a snapshot file for its size, a
`Snapshot blocked:` line says which file and why. The same facts are in `/api/status` as
`cloud_sync.snapshot`.

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
| `share` | Export contacts and manage invitations on a named database. |
| `db` | List the local database and accepted shares. |
| `admin` | Diagnose or maintain a home. |

Name a file by path in the same `mutate --request-file` operation as the record.
The command does not copy the file, and the file must stay where it is.
Use a JSON object with `mutation` set to the complete conditional request and
`file` set to an object with `source` and optional `name` and `media_type`. The
source path is relative to the request file, or to the working directory for
stdin. A record with a required file cannot be written without its file. A
request with no `file` key sends no file, so a bare record replacement keeps
the existing file. On a replace, `file: null` in the same envelope clears an
optional file.

### Publish an app

From a clean app checkout, run `lastdb app publish --manifest lastdb-app.json`
with the node running on the paid ExeMem account's connected LastDB home.
The publisher identity comes from that account. LastDB obtains a publishing
credential for the request when needed and revokes it when the request ends.

Existing publishers can use their original signing key with `--key-file`.
Pass `--key-file -` to read that key from standard input. CI can supply a
developer API key using `EXEMEM_DEV_API_KEY`.

When the manifest sets `"audience": "private"`, `lastdb app publish` saves a
mode-0600 private list locator (`private:<env>:<account>`) under the managed
apps directory and prints that locator in the publish reply. `lastdb app install <app>`
and `lastdb app list <app>` check saved private list locators when `<app>` is not
on the public list, without storing raw private URLs in install receipts. Run
`lastdb app show <app>` (or `--save` to omit the raw URL) to inspect an owned app
and its list location.

Write each schema field once. In `schemas`, `fields` is an object from the
field name to an object with a required `type` and optional `description`,
`classifications`, `interest_category`, `version`, `declaration`, and `hash`.
`schema_type` and `descriptive_name` can be left out. When `record_keys` is
present, `schema_type` follows `record_keys.primary.kind`. A blank
`descriptive_name` becomes the schema's local name. Names in the object are
stored in sorted order. The older form, a `fields` array plus separate
`field_*` maps, is still read. Do not mix the two in one entry.

```json
{
  "name": "LastSecretIndex",
  "purpose_statement": "Index of the secrets this app stores.",
  "record_keys": { "primary": { "kind": "hash" }, "alternates": {} },
  "fields": {
    "secret_metadata_bucket_key": {
      "type": "String",
      "description": "Bucket that holds the secret metadata."
    },
    "secret_metadata_list_json": { "type": "String" },
    "secret_metadata_updated_at": { "type": "DateTime" }
  }
}
```

### Share a database

Use [People](https://github.com/EdgeVector/people) to share a named database with verified
contacts and circles. TypeScript apps use `peopleClient` from
[`@lastdb/people-client`](https://github.com/EdgeVector/people/tree/main/client),
which ships with People, to create a share, seal or accept an invitation, and
get its stable database ID. Use `@lastdb/app-sdk`'s data client to work with
the records.
[App SDK sharing guide](lastdb_app_sdk/README.md#set-up-sharing-with-people)

Personal and shared databases sync through signed history (AcceptEntry) and a
grant-gating relay. The personal database is sealed: the relay stores the
streams of every device of the owner and cannot read them, and a new device
or a restore needs only the recovery phrase
(`lastdb backup restore --recovery-phrase-stdin`). Each write goes into the
spool of accepted writes, is signed as the next entry of its device's stream,
and is applied on the other devices by the store's last-writer stamp, so the
later edit wins. A snapshot of the store's own files, named by a signed
`snapshot` entry, is the checkpoint: a restore installs the newest one and
reads only the entries after it (`lastdb backup snapshot` takes one now). The
relay keeps one snapshot, the entries after it and the files they name: after
each snapshot the node sweeps every other stored file (`lastdb backup reclaim`
sweeps now). See the
[home on disk](docs/home-format.md),
[personal database design](docs/designs/personal-database-signed-log.md),
[signed log on the local machinery](docs/designs/signed-log-on-local-machinery.md),
[cloud API](docs/designs/cloud-api.md) and
[relay protocol](docs/designs/relay-protocol.md).

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
