# Shared-surface caller inventory

Status: living inventory for the catalog-registration migration

Related: [Schema service boundary](shared_surface_schema_service.md)

## Goal

Find and retire callers that can create a durable schema without first
registering it with Schema Service. Shared-surface metadata remains a separate
governance concern; registration itself applies to every schema.

## Registration classes

| Class | Meaning | In shared-only projection? |
| --- | --- | --- |
| `shared` | Explicit shared-surface metadata accepted | Yes |
| `private_legacy_bootstrap` | Legacy app-namespaced schema registered without current governance metadata | No |
| `system_owned` | SystemSeed / StarterSeed / `is_system_schema` | Yes |
| `unknown` | Unowned user schema, or offer-without-metadata | No |

Superseded rows are counted under `deprecated_or_superseded` and always
excluded from the projection.

## Known callers (code inventory)

| Caller | Path today | Class / intent | Target |
| --- | --- | --- | --- |
| LastDB Mini `POST /api/schemas/declare` | Strict compatibility adapter into the canonical Mini catalog-sync implementation | Catalog-backed | Keep temporarily; unknown fields, migration intent, and local mint fail closed |
| LastDB Mini `POST /api/apps/declare-schema` | Canonical resolve/register/exact-load operation with durable audit evidence | Catalog-backed | Product entry point; novel shapes and expansions register through Schema Service |
| LastSecrets | Mini direct catalog declare + shared-surface publish when shared | Catalog-backed / explicit shared surface | Keep off private registry claims |
| fbrain init / bootstrap | Mini app-schema declaration | Catalog-backed | Keep registration and binding inside Mini |
| fkanban init / bootstrap | Mini `POST /api/apps/declare-schema`; app performs probes before config bind | Catalog-backed | CI forbids direct Schema Service and legacy-route transports |
| Live `POST /v1/schemas` with `offer_to_shared_discovery=false` + owner | Cert-free local claim into registry | Private legacy if persisted | Stop for private apps |
| Live `POST /v1/schemas` with `offer_to_shared_discovery=true` | DevCert-gated shared offer | Incomplete shared (no full surface envelope yet) | Shared publish/attach |
| Live `POST /v1/schemas` unowned | Mutation-gated "new shared mutation" | Unknown | Require surface metadata |
| Builtin / Schema.org seeders | SystemSeed / StarterSeed at boot | System owned | Stay in projection |
| Resolver pack publisher | Shared-only projection → pack artifacts (`project_snapshot_shared_only` / `GET /v1/snapshot/shared-only`) | Done | PR `schema-service-shared-registry-projection` |
| `SchemaServiceClient::resolve_schemas` | Always live HTTP for generic registry resolution; no shared publish/attach product caller in-repo | N/A | Keep out of shared publish/attach |
| LastDB Mini `POST /api/apps/shared-surface/publish-attach` | Single shared publish/attach product route; delegates to `schema_resolver_host::publish_attach_with_host_config` and then `LocalFirstSchemaResolver` | Explicit shared surface | Keep as the only product entry |

## Runtime measurement

On every `POST /v1/schemas` the service emits observe-mode telemetry:

```text
target=schema_service::shared_surface
metric=schema_shared_surface_legacy_caller_total
caller_kind=local_claim|shared_offer_without_surface|unowned_registration|explicit_shared_surface
```

Registry sweep (operator / tests):

```rust
let inventory = state.inventory_shared_surface_registrations()?;
// inventory.{shared, private_legacy_bootstrap, system_owned, unknown, ...}
let projected = state.shared_only_projection_schemas()?;
```

## Exit criteria for this inventory

1. Every Mini declaration resolves to an already registered catalog identity
   or registers a novel schema through Schema Service before load/write.
2. fbrain/fkanban bootstrap both prove registration before binding a schema.
3. No product caller accepts `local_mint` as a durable schema identity.
4. Shared-only projection contains zero `private_legacy_bootstrap` rows.
5. Legacy caller metrics show no unexpected `unowned_registration` spikes
   before enforcement is enabled.

Mini app schema synchronization appends every accepted or rejected proposal to
`$LASTDB_HOME/logs/schema-sync-audit.jsonl`. Successful responses include an
`audit_event_id` and `bind_eligible=true`. Audit persistence failure refuses the
bind response; rerunning the idempotent catalog sync repairs the local evidence.
The log rotates at 10 MiB (override with
`LASTDB_SCHEMA_SYNC_AUDIT_MAX_BYTES`) and retains one previous segment.
