# fold_db

Core **LastDB** storage/schema library inside the [`EdgeVector/lastdb`](https://github.com/EdgeVector/lastdb)
monorepo (`crates/core` + `crates/observability`).

This directory is a **Rust library only**. There is no React app, no Vite
frontend, no Tauri shell, and no desktop DMG in this tree.

## Product surface

Use **LastDB Mini** from the sibling crate `lastdb_node`:

| Binary | Role |
| --- | --- |
| `lastdbd` | Headless daemon on Unix socket `~/.lastdb/data/folddb.sock` |
| `lastdb` | Control CLI (`status`, `ops`, …) |

```bash
brew install edgevector/lastdb/lastdb
brew services start lastdb
lastdb status
```

Build from this monorepo (workspace root):

```bash
cargo build -p lastdb_node --release
```

## This crate

```bash
# from monorepo root
cargo build -p fold_db
cargo test -p fold_db --lib
cargo clippy -p fold_db --all-targets -- -D warnings
```

- Storage: LastStore / docstore (vendored under monorepo `vendor/`)
- Optional cloud sync (feature-gated; Mini hosts opt in)
- Observability helpers in `crates/observability`

Compatibility note: package name, paths, and env vars may still say
`fold_db` / `FOLDDB_*` for existing installs; user-facing product name is
**LastDB**.

## What was removed (do not resurrect)

On **2026-07-12** (Mini-only cutover) the monorepo deleted:

- `fold_db_node` (desktop full node + HTTP UI host)
- Embedded React frontend (`static-react`)
- Tauri app / DMG release train
- `file_to_markdown` and face-detection product path

Restore point only: git branch `archive/desktop-dmg-pre-removal`.

Empty Node lockfiles / Vercel project metadata under `fold_db/` were also
removed so tooling no longer mis-detects this library as a JavaScript app.

## Docs

- Monorepo overview: [`../README.md`](../README.md)
- Contributing to this library: [`CONTRIBUTING.md`](CONTRIBUTING.md)
- Atom size limit: [`docs/ATOM_CONTENT_SIZE_LIMIT.md`](docs/ATOM_CONTENT_SIZE_LIMIT.md)
- Release (Homebrew Mini tarball): [`../docs/RELEASING.md`](../docs/RELEASING.md)

## License

Apache-2.0 — see the repository root `LICENSE`.
