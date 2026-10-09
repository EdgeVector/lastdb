# App Identity — identity_hash migration notes (Lane B2a)

Tracking notes for the `owner_app_id` → `identity_hash` change shipped in
Lane B2a of [app_identity v3.1](../../exemem-workspace/docs/designs/app_identity.md).
Written for the owners of the downstream lanes (B2b/B2c/B2d, C, D) who
inherit this change.

## What changed

`DeclarativeSchemaDefinition` gained `owner_app_id: Option<String>`, and
`compute_identity_hash` now mixes it into the hash **when it is `Some`**:

```
owner_app_id == Some("fbrain")  →  sha256("app:fbrain:" + readable_name + ":" + sorted_fields)
owner_app_id == None            →  sha256(            readable_name + ":" + sorted_fields)   # unchanged
```

`canonical_name()` / `parse_canonical_name()` map between
`(name, owner_app_id)` and the canonical `"{owner_app_id}/{name}"` form.

## Why this does NOT ripple (yet)

The change is **inert for every schema that exists today**: `owner_app_id`
is a brand-new field that defaults to `None` on construction and
deserialization, and nothing in-tree sets it yet. For `owner_app_id ==
None` the hash bytes are byte-identical to the pre-B2a scheme, so:

- **Snapshot diffing** — unaffected; legacy schema hashes are stable.
- **View canonicalization** — unaffected; view output schemas have no
  `owner_app_id`.
- **Transform input matching** — unaffected; transforms reference
  schemas by their existing (un-namespaced) identity hash.

The new scheme only activates once a caller sets `owner_app_id`, which
first happens in Lane B2b (`POST /v1/apps` + `POST /v1/schemas` with a
dev cert) and Lane D (fbrain republish under `fbrain/*`).

## `compute_identity_hash` call sites (for the Lane D destructive reset)

These recompute the identity hash and will pick up `owner_app_id`
automatically once it is set on the schema before the call:

- `fold_db/crates/core/src/schema/core.rs` (registration)
- `fold_db/crates/core/src/triggers/mod.rs` (firing-time capture)
- `schema_service/crates/core/src/state.rs` (`add_schema` path)
- `schema_service/crates/core/src/state_expansion.rs`
- `schema_service/crates/core/src/builtin_schemas.rs` (seeds — stay `None`)

Built-in / seed schemas (`SystemSeed` / `StarterSeed`) intentionally keep
`owner_app_id == None`, so their hashes never change.

## Open follow-ups

- **Lane B2b** populates `SnapshotEnvelope.apps[]` (empty in B2a) via the
  `POST /v1/apps` endpoint, and starts setting `owner_app_id` on
  user-registered schemas.
- **Snapshot `version` persistence** — the monotonic counter resets to 0
  on every redeploy in B2a (in-memory only). Clients compare snapshot
  bytes, not the version alone, so this is safe for now; persist it if a
  consumer ever needs cross-restart monotonicity.
