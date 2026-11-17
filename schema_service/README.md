# schema_service

Advisory Schema Service for LastDB's curated field vocabulary and schemas
from apps in the public LastDB App Registry.

Canonical field names use full concept and property names. New fields cannot
use the audited app abbreviations such as `bge_*` and `rle_*`. The direct
checks live in `crates/core/src/lib.rs`.

## Cloud-only deployment

Production runs on AWS Lambda (`schema.folddb.com`) deployed via
[EdgeVector/schema-infra](https://github.com/EdgeVector/schema-infra). There is
no user-installable binary, Homebrew formula, or local HTTP dev server.

## Workspace layout

| Crate                  | Purpose                                                                                                   |
| ---------------------- | --------------------------------------------------------------------------------------------------------- |
| `crates/schema_types`  | Shared declarative schema types, key layouts, and portable schema definitions consumed by `fold_db`.      |
| `crates/core`          | Pure deterministic canonical field vocabulary, field-name policy, and `POST /v1/advice/schema` engine.    |
| `crates/server_lambda` | AWS Lambda handler serving `/v1/health`, `/v1/fields/vocabulary`, `POST /v1/advice/schema`, and `POST /v1/schemas`. |

## HTTP surface

| Method | Path                    | Description                                                      |
| ------ | ----------------------- | ---------------------------------------------------------------- |
| GET    | `/health`, `/v1/health` | Liveness probe                                                   |
| GET    | `/v1/fields/vocabulary` | Read the curated canonical field vocabulary                      |
| POST   | `/v1/advice/schema`     | Field advice and similar published schemas for a draft           |
| POST   | `/v1/schemas`           | Register one listed app's schema definitions for one channel     |

## Published schemas

`lastdb app publish` pushes the app's schema definitions in the same command.
`POST /v1/schemas` checks the body, reads the signed registry list for that
channel and API version, checks the commit and publisher key, verifies the
publisher signature, and stores the definitions. The statement the publisher
signs is built by `schema_push_statement` in `app_identity_crypto`.

The catalog object is `schema-catalog-v2/catalog.json` in the private bucket.
One record per app and channel replaces the previous record for that pair.
A conditional write uses the object's ETag, and a conflict re-reads and
retries. Advice reads the object through a 60 second cache.

The catalog status is `current` when that read succeeds, including when the
object does not exist yet, and `unavailable` when the read fails. Field
advice uses the bundled vocabulary in both states. A `similar_schema`
finding includes the schema identity and its app, commit, channel, and API
version.

## Verify vocabulary locally in CI

```bash
cargo build --locked -p schema_service_server_lambda --bin server_lambda
./target/debug/server_lambda --print-vocabulary | jq '.name_policy_version, (.fields | length)'
```
