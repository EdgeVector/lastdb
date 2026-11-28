# schema_service

Shared schema types and the offline advice vocabulary for LastDB app
developers. There is no Schema Service HTTP endpoint in this repo. Each
database's signed log carries its schemas; install places definitions from
the app's own `lastdb-app.json` at the signed commit.

Canonical field names use full concept and property names. New fields cannot
use the audited app abbreviations such as `bge_*` and `rle_*`. The direct
checks live in `crates/core/src/lib.rs`.

## Workspace layout

| Crate                 | Purpose                                                                                              |
| --------------------- | ---------------------------------------------------------------------------------------------------- |
| `crates/schema_types` | Shared declarative schema types, key layouts, and portable schema definitions consumed by `fold_db`. |
| `crates/core`         | Pure deterministic canonical field vocabulary, field-name policy, and offline schema advice.         |

[EdgeVector/schema-advisor](https://github.com/EdgeVector/schema-advisor) depends
on both crates for offline review and planning.

## Build

```bash
cargo build --locked -p schema_types -p schema_service_core
cargo clippy --locked -p schema_service_core --lib -- -D warnings
```
