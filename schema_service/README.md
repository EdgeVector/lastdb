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
| `crates/server_lambda` | AWS Lambda handler serving `/v1/health`, `/v1/fields/vocabulary`, `POST /v1/advice/schema`, `POST /v1/schemas`, and `GET /v1/schemas/<definition_id>`. |

## HTTP surface

| Method | Path                    | Description                                                      |
| ------ | ----------------------- | ---------------------------------------------------------------- |
| GET    | `/health`, `/v1/health` | Liveness probe                                                   |
| GET    | `/v1/fields/vocabulary` | Read the curated canonical field vocabulary                      |
| POST   | `/v1/advice/schema`     | Field advice and similar published schemas for a draft           |
| POST   | `/v1/schemas`           | Register one listed app's schema definitions for one channel     |
| GET    | `/v1/schemas/<definition_id>` | Read one stored schema definition by its definition id     |

## Published schemas

`lastdb app publish` pushes the app's schema definitions in the same command.
Each entry is the wire-form definition the node holds, with `identity_hash`
and explicit record keys. The schema namespace does not have to equal the
pushing app. A manifest may name a schema another app introduced.

`POST /v1/schemas` checks the body, reads the signed registry list for that
channel and API version, checks the commit and publisher key, and verifies the
publisher signature. The statement the publisher signs is built by
`schema_push_statement` in `app_identity_crypto`.

For each definition the service computes `PortableSchemaId::from_catalog` from
that definition's own `identity_hash`. It does not accept a definition id from
the caller. A definition that has no identity hash, no record keys, an untyped
field, a hash that does not match, or `Any` refuses the whole push.

Each definition is then stored as its own immutable object at
`schema-catalog-v2/definitions/<definition_id>.json`. The object body is
`{"definition_id": "<id>", "definition": <wire-form definition>}`. The write
uses `If-None-Match: *`. An object that already exists is success, because the
same id is the same bytes. Those objects are written before the catalog record.

The catalog object is `schema-catalog-v2/catalog.json` in the private bucket.
One record per app and channel replaces the previous record for that pair.
The record keeps `definitions` and adds `definition_ids`, a map from schema
name to definition id. A conditional write uses the object's ETag, and a
conflict re-reads and retries. Advice reads the object through a 60 second
cache. The reply to a push is `{"registered": <n>, "definition_ids": {...}}`.

`GET /v1/schemas/<definition_id>` returns that stored object and sends
`Cache-Control: public, max-age=31536000, immutable`. No credentials are
required. An id that is not 64 lowercase hex is HTTP 400. A missing object is
HTTP 404. Storage that cannot be reached is HTTP 503. `GET /v1/schemas` and
`POST /v1/schemas/<definition_id>` are ordinary 404s.

The catalog status is `current` when the advice read succeeds, including when
the object does not exist yet, and `unavailable` when the read fails. Field
advice uses the bundled vocabulary in both states. A `similar_schema`
finding includes the schema identity and its app, commit, channel, and API
version. The retired object `advisory-catalog-v1/catalog.json` is not read,
written, or converted.

## Verify vocabulary locally in CI

```bash
cargo build --locked -p schema_service_server_lambda --bin server_lambda
./target/debug/server_lambda --print-vocabulary | jq '.name_policy_version, (.fields | length)'
```
