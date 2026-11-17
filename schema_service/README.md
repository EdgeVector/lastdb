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
| `crates/server_lambda` | AWS Lambda handler serving `/v1/health`, `/v1/fields/vocabulary`, and `POST /v1/advice/schema`.           |

## HTTP surface

| Method | Path                    | Description                                                      |
| ------ | ----------------------- | ---------------------------------------------------------------- |
| GET    | `/health`, `/v1/health` | Liveness probe                                                   |
| GET    | `/v1/fields/vocabulary` | Read the curated canonical field vocabulary                      |
| POST   | `/v1/advice/schema`     | Field advice and similar published schemas for a draft           |

## Public app catalog

An EventBridge invocation refreshes a private catalog object every five
minutes. The reader verifies registry lists and publisher statements, fetches
public GitHub manifests by exact commit, and uses LastDB's owned schema
normalization. A manifest may write each field once, as an object of field
names; that form is expanded to the stored field list before normalization.
The older field list is still read. Active listings supply advice examples.
The stored manifest definitions retain their commit identity when a listing
disappears.

Advice reports the catalog as `current`, `stale`, or `unavailable`. Field
advice uses the bundled vocabulary in all three states. A `similar_schema`
finding includes the schema identity and its app, commit, channel and API
version.

## Verify vocabulary locally in CI

```bash
cargo build --locked -p schema_service_server_lambda --bin server_lambda
./target/debug/server_lambda --print-vocabulary | jq '.name_policy_version, (.fields | length)'
```
