# Mini schema resolver install config

Mini always resolves through the live Schema Service. It does not ship a local
proposal embedder or resolver-pack runtime. Live resolution remains catalog-backed;
this configuration does not permit local durable identity creation.

Related: [direct_schema_registration.md](direct_schema_registration.md),
[shared_surface_schema_service.md](shared_surface_schema_service.md).

## File and timeout

The optional file is `{LASTDB_HOME}/schema_resolver.json` (default home `~/.lastdb`):

```json
{
  "request_timeout_seconds": 10
}
```

`request_timeout_seconds` is a nonnegative integer. The default is 10 seconds.
Owner-socket calls clamp the configured value to 1–20 seconds, including a zero
value clamped to one. The same duration bounds both the HTTP request and the
complete resolve or publish/attach operation. Existing timeout errors remain.

A missing, empty, unreadable or invalid JSON file uses the default. An invalid
timeout type also uses the default. Read and parse failures retain their warnings.
Unknown JSON keys are ignored, so existing files need no rewrite.

## Retired pack settings

Mini previously parsed pack mode, enable/kill switches, origin/channel, refresh,
cache, download limits, trusted keys, embedder identity and enforcement reports.
Every combination ultimately used the live service because Mini had no local
embedder. These keys are now ignored, including their value types; an invalid
value in a retired key no longer discards an otherwise valid timeout.

The former pack environment overlays have no effect:

```text
SCHEMA_RESOLVER_ENABLED
SCHEMA_RESOLVER_MODE
SCHEMA_RESOLVER_ALLOW_ENFORCE
SCHEMA_RESOLVER_KILL_SWITCH
SCHEMA_RESOLVER_BASE_URL
SCHEMA_RESOLVER_CHANNEL
SCHEMA_RESOLVER_EXPECTED_EMBEDDER_ID
SCHEMA_RESOLVER_TRUSTED_KEYS
```

There is no timeout environment overlay. Configure the JSON value above.
The former mode/fallback diagnostics are removed along with the inactive branches.

## Routes and ownership

- `POST /api/apps/declare-schema` and its strict compatibility adapter,
  `POST /api/schemas/declare`, use this timeout for direct live resolve.
- `POST /api/apps/shared-surface/publish-attach` uses this timeout and the node
  signing identity for shared publish/attach.
- `GET /api/apps/shared-surface/attachments` lists local attachments.

The existing live facade retains proposal-ID matching, validation and shared
publish/attach outcomes. Catalog load, mapper installation, schema-sync audit and
attachment persistence remain owned by their existing callers.

The shared client library still provides shadow/enforce modes to callers that
supply a pack runtime and embedder. Schema-service tooling owns pack generation;
its [local resolver configuration](local_schema_resolver_config.md) is separate
from Mini's timeout. The service's `SCHEMA_NATIVE_COMPONENT_COVER_RESOLVE` gate
also remains separate from this file.
