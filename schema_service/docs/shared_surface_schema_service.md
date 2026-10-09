# Schema service shared-surface governance

Status: governance layer retained; storage boundary superseded by
[direct_schema_registration.md](direct_schema_registration.md)

Related design: [Native cached catalog resolver configuration](local_schema_resolver_config.md)

## Decision

The schema service governs the global catalog used by LastDB durable writes.
Shared surfaces add explicit governance metadata for data exposed across app,
user, or distribution boundaries.

Earlier versions of this document treated app-private schemas as Mini-local
storage that stayed outside Schema Service. That boundary is superseded for new
product writes: private implementation details may remain private in visibility
and ownership, but durable records still resolve to catalog identities and use
catalog-shaped storage. The shared-surface workflow is the publish/attach
governance layer, not the permission to have a second on-disk schema language.

## Classification

A schema is **private** when it is only an implementation detail of one app.
Private schemas are not externally published, but they still resolve to catalog
structure before durable write. Using the same app and schema on another device
does not make the schema shared.

A schema is **shared** when its owner deliberately exposes it for at least one
of these purposes:

- cross-app reads, links, or writes;
- a published or externally discoverable data slice;
- a shared protocol or public/importable contract;
- coordinated compatibility, migration, or lifecycle management.

Sharing is explicit intent, not an inference based on popularity, installation
count, or a schema's descriptive similarity to another schema.

## Required invariants

1. Mini never treats app-local field dialects as the durable storage language.
2. Local resolution may reuse an existing catalog schema or catalog components.
   It may never mint a catalog identity locally.
3. A cache miss, stale pack, ambiguity, unsupported input, or denied reuse
   falls back to live Schema Service when online, or rejects when offline.
4. Resolver snapshots and embedding artifacts contain service-approved catalog
   schemas and fields.
5. Shared-surface publish/attach adds explicit governance metadata.
6. Shared records carry ownership, visibility, compatibility, lifecycle, and
   provenance metadata sufficient for governance and audit.
7. Removing a schema from a shared index must not silently convert existing
   stored data into an app-local dialect.

## Product flow

```text
app or agent write
  -> propose local field names
  -> resolve to a catalog schema
  -> apply edge adapter
  -> store catalog-shaped record

explicit shared-surface publish/attach
  -> validate sharing intent and metadata
  -> try cached native resolver against catalog snapshot
      -> confident existing match: attach existing shared canonical
      -> miss/ambiguous/stale: call live schema service
  -> service reuses or creates the authoritative catalog contract
  -> persist publish/attach metadata
```

The attachment records governance and compatibility metadata for the external
surface. Edge adapters bridge caller field names to catalog fields before
storage; adapters do not become the stored language.

## API shape

Legacy private declaration bodies looked like:

```json
{
  "namespace": "notes",
  "descriptive_name": "Note",
  "fields": []
}
```

The shared workflow still requires explicit intent and governance metadata:

```json
{
  "local_schema_id": "<app-namespaced-id>",
  "surface": {
    "visibility": "shared",
    "purpose": "cross_app_read",
    "owner_app_id": "com.example.notes",
    "contract_name": "Published Note",
    "compatibility": "backward_compatible"
  }
}
```

The exact transport endpoint is an implementation detail, but it must be
separate from generic private declaration. The service rejects requests that
do not assert an allowed shared purpose or lack required ownership metadata.

## Local-first facade (PR 4/5)

`schema_service_client::local_first_resolver` provides
`LocalFirstSchemaResolver` with three modes:

| Mode | Behavior |
| --- | --- |
| `shadow` | Always call live; compare local native decisions for telemetry only |
| `enforce_existing_only` | Return local `UseExisting` / `UseComponents` when pack policy allows; else live |
| `live_only` | Kill switch — never use the local pack |

Mini mounts:

- `POST /api/apps/shared-surface/publish-attach` — validate sharing intent,
  resolve via the facade (default `live_only` until a pack runtime is wired),
  register novel contracts live with `shared_surface` metadata, and persist
  an attachment under `{home}/shared_surface_attachments.json`.
- `GET /api/apps/shared-surface/attachments` — list local attachments.

As of 2026-07-15, this Mini route is the only in-tree product entry point for
shared-surface publish/attach. It delegates to
`lastdb_node::schema_resolver_host::publish_attach_with_host_config`, which is
the install-owned wrapper around `LocalFirstSchemaResolver`; `exec` and router
code do not construct live-only schema clients or resolver facades directly.

Private declare (`POST /api/schemas/declare`, `POST /api/apps/declare-schema`)
is legacy compatibility for local fixture and migration paths. New durable
writes should enter through the resolve/adapt/store flow described in
[direct_schema_registration.md](direct_schema_registration.md).

## Existing app migration

- LastSecrets, fbrain, and fkanban should converge on catalog-backed durable
  records. Existing local declarations are migration inputs, not the target
  product shape.
- Any schema that fbrain, fkanban, or another app deliberately exposes to
  another app must use the new shared publish/attach workflow instead.

## Non-goals

- Automatically creating catalog schemas from arbitrary private proposals.
- Treating cross-device use by the same app as a shared surface.
- Storing app-local field names as durable data.
- Restoring or integrating with the removed `fold_db_node` desktop surface.
- Allowing downloaded configuration to create canonical schemas locally.

## Executable contract (PR 0)

Rust types and validation live in
`schema_service_core::shared_surface`:

| Item | Role |
| --- | --- |
| `SharedSurfacePublishAttachRequest` | Explicit publish/attach body (`local_schema_id` + `surface`) |
| `validate_shared_surface_request` / `validate_surface_metadata` | Reject missing/invalid sharing intent |
| `RegistrationClass` + `inventory_registrations` | Classify existing rows: shared / private_legacy_bootstrap / system_owned / unknown |
| `project_shared_only_schemas` | Shared-only resolver projection (excludes private, unknown, superseded) |
| `observe_legacy_schema_caller` | Observe-mode metric on legacy `POST /v1/schemas` |

`SchemaServiceState::inventory_shared_surface_registrations` and
`shared_only_projection_schemas` expose inventory and projection over the
live registry. Attachment metadata is not persisted on schema rows yet;
until it is, projection includes system-owned seeds only and private
legacy bootstrap rows stay out of resolver packs.

**Resolver-pack source dataset (PR 1):**

| Surface | Role |
| --- | --- |
| `export_shared_only_snapshot` / `GET /v1/snapshot/shared-only` | Live shared-only export for pack inputs |
| `project_snapshot_shared_only` | Pure filter over a `SnapshotEnvelope` (also used offline) |
| `schema_resolver_pack_publish build` | Always projects before writing pack artifacts |

Registered schemas with private visibility never appear in the shared-only pack
schema/embedding artifacts even if the operator feeds a full
`GET /v1/snapshot` file into `build`.

Ordinary registered Mini declare bodies (`{namespace,schema}` /
`{app_id,schema}`) do not deserialize as
`SharedSurfacePublishAttachRequest` — contract tests enforce that
boundary so ordinary registration cannot be interpreted as sharing
intent.

Caller inventory: [shared_surface_caller_inventory.md](shared_surface_caller_inventory.md)

## Mini pack runtime (follow-up)

Install-owned config and Shadow/Enforce wiring for
`POST /api/apps/shared-surface/publish-attach` are documented in
[mini_schema_resolver_config.md](mini_schema_resolver_config.md).
Default remains LiveOnly. Mini currently clamps pack-backed local eval to
LiveOnly because it no longer ships an in-process semantic embedder; schema-service
operator tooling owns FastEmbed pack generation.
