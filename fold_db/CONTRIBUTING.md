# Contributing to fold_db

`fold_db` is the **core LastDB library** inside the `EdgeVector/lastdb` monorepo.
It is not a standalone app, not a Node/React project, and not a desktop UI.

Product entry points live in sibling crate `lastdb_node` (`lastdb` CLI +
`lastdbd` headless daemon). The old `fold_db_node` desktop host, Tauri shell,
and embedded React frontend (`static-react`) were removed on **2026-07-12**
(Mini-only cutover). Do not add frontend packages, Vite, or React under
`fold_db/`.

## Getting started

1. Work from a **portal worktree** of `EdgeVector/lastdb` (not the empty portal
   dir). See workspace `README.md` / `bin/wt start`.
2. Create a feature branch from `main`.
3. Make changes under `fold_db/` (and related crates if needed).
4. Run tests from the **workspace root**:
   ```bash
   cargo test -p fold_db --lib
   cargo clippy -p fold_db --all-targets -- -D warnings
   cargo fmt -p fold_db
   ```
5. Open a PR on **GitHub** (`gh -R EdgeVector/lastdb pr create`).
   LastGit and the Forgejo copy are retired.

## Prerequisites

- Rust toolchain matching the workspace `rust-toolchain.toml`
- No Node.js / npm is required for `fold_db` itself

## Building and testing

From the monorepo root:

```bash
# Library only
cargo build -p fold_db
cargo test -p fold_db --lib
cargo test -p fold_db --test kernel_constitution_test

# Product Mini binaries (optional for library work)
cargo build -p lastdb_node
```

There is **no** `src/server/static-react`, **no** `npm test`, and **no**
frontend embed step. If a tool or old doc claims otherwise, it is stale —
trust this file and the root `README.md`.

## Layout (current)

```
fold_db/
├── crates/
│   ├── core/           # fold_db library (storage, schema, sync, …)
│   └── observability/  # shared tracing/Sentry helpers
├── docs/               # library design + operator notes
├── CLAUDE.md           # agent command cheatsheet
└── README.md           # library overview
```

Workspace product crates (outside this dir): `lastdb_node`, `lastdb_host`,
`lastdb_uds`, `schema_service`, etc. See monorepo root `Cargo.toml`.

## Code style

- `cargo fmt` / `cargo clippy` clean for touched crates
- No silent failures — propagate or handle errors explicitly
- Prefer domain error types already used in the crate
- Do not reintroduce desktop UI, Tauri, or React surfaces

## What not to contribute here

- React / Vite / npm package trees under `fold_db/`
- Tauri or DMG packaging
- Resurrection of `fold_db_node` or `static-react`
- TCP `:9001` full-node server as the default product path (Mini is UDS-only)

Historical full-node code lives only on branch `archive/desktop-dmg-pre-removal`.
