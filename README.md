# LastDB

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
├── schema_service/             # cloud schema registry
│   ├── crates/{core,client,server_shared,server_http,server_lambda,worker,…}
│   └── openapi.yaml
├── vendor/{laststore,lastdb_docstore}/
└── docs/                       # operator + release docs (Mini / Homebrew)
```

## What's deployed where

| Component | Where the source lives | Where it deploys |
| --- | --- | --- |
| LastDB Mini (`lastdb` + `lastdbd`) | `lastdb_node` | Homebrew `edgevector/lastdb/lastdb` via tag → [`.github/workflows/release.yml`](.github/workflows/release.yml) → `EdgeVector/homebrew-lastdb` |
| schema_service Lambda | `schema_service/crates/server_lambda` | schema registry via [EdgeVector/schema-infra](https://github.com/EdgeVector/schema-infra) (submodules this repo) |
| schema_service compile worker | `schema_service/crates/worker` | DockerImageFunction via schema-infra |

Release process: [`docs/RELEASING.md`](docs/RELEASING.md). There is no DMG
or desktop-app channel.

## Atom content size limit

**Atoms are not a blob store.** Each field value is hard-capped (default
**64 KiB** serialized JSON before encryption). Override with env
`LASTDB_MAX_ATOM_CONTENT_BYTES` (absolute max **1 MiB**). Oversized writes
return HTTP 413 `atom_content_too_large`. Large/opaque bytes → file-blob /
CAS. Full write-up: [`fold_db/docs/ATOM_CONTENT_SIZE_LIMIT.md`](fold_db/docs/ATOM_CONTENT_SIZE_LIMIT.md).
Operators: `lastdb status` shows `Limits: max_atom_content=…`.

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

Mini ships the default `lastdb` / `lastdbd` binaries without the in-process
FastEmbed semantic-search path; local runs use exact matching plus
service-owned Search/schema resolver surfaces. For the release proof of the
clean default no-embedder lane:

```bash
scripts/release/validate-no-ai-install.sh
```

Never restart Tom's primary `lastdbd` / brain from agent work. Never use
the live `~/.lastdb` home as the first place a candidate binary is proven —
use CoW/ephemeral homes (`lastdb-safe-upgrade` / smoke helpers).

### schema_service dev server

```bash
cargo run -p schema_service_server_http --bin schema_service -- --port 9102 --db-path schema_registry
```

Use `--features fastembed` on that command only when validating the real
local embedding path.

### Schema service deploys

[EdgeVector/schema-infra](https://github.com/EdgeVector/schema-infra) owns the
CDK and the deploy scripts for the schema service (Lambda, fastembed layer,
worker container image). It consumes this repository as a git submodule.

### Downstream submodule pointer policy

A downstream repo must pin its submodule of this repository to a commit
reachable from `EdgeVector/lastdb` `origin/main`. Do not deploy off-main,
cherry-picked, or dangling commits through a submodule gitlink; land the change
through a normal PR first, then bump the submodule pointer to that merged
commit.

Downstream CI can enforce this from the parent repo after submodules are
checked out:

```bash
bash <submodule>/scripts/ci/verify-fold-gitlink-main.sh . <submodule> origin/main
```

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
