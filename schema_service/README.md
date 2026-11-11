# schema_service

Advisory Schema Service for LastDB's curated canonical field vocabulary.

Canonical field names use full concept and property names. New fields cannot
use the audited app abbreviations such as `bge_*` and `rle_*`. The direct
checks live in `crates/core/src/field_name_policy.rs`.

## Cloud-only deployment

Production runs on AWS Lambda (`schema.folddb.com`) deployed via
[EdgeVector/schema-infra](https://github.com/EdgeVector/schema-infra). There is
no user-installable binary, Homebrew formula, or local HTTP dev server.

## Workspace layout

| Crate                  | Purpose                                                                                                   |
| ---------------------- | --------------------------------------------------------------------------------------------------------- |
| `crates/schema_types`  | Shared declarative schema types, key layouts, and portable schema definitions consumed by `fold_db`.      |
| `crates/core`          | Pure deterministic canonical field vocabulary, field-name policy, and `POST /v1/advice/schema` engine.    |
| `crates/server_lambda` | AWS Lambda handler serving `/v1/health`, `/v1/fields/vocabulary`, and `POST /v1/advice/schema`.           |

## HTTP surface

| Method | Path                    | Description                                                      |
| ------ | ----------------------- | ---------------------------------------------------------------- |
| GET    | `/health`, `/v1/health` | Liveness probe                                                   |
| GET    | `/v1/fields/vocabulary` | Read the curated canonical field vocabulary                      |
| POST   | `/v1/advice/schema`     | Deterministic advice for a draft schema declaration (read-only)  |

## Verify vocabulary locally in CI

```bash
cargo build --locked -p schema_service_server_lambda --bin server_lambda
./target/debug/server_lambda --print-vocabulary | jq '.name_policy_version, (.fields | length)'
```
