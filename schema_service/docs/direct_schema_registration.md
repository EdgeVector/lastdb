# Direct schema registration and one-language storage

Status: approved design direction (Tom, 2026-07-14)

Brain: `design-direct-schema-service-registration`,
`preferences-everything-through-schema-service`

Related:
[mini_schema_resolver_config.md](mini_schema_resolver_config.md),
[cached catalog resolver configuration](local_schema_resolver_config.md),
[schema_registration_distribution_gate.md](schema_registration_distribution_gate.md),
[shared_surface_schema_service.md](shared_surface_schema_service.md)

## Decision

LastDB and Schema Service use one structural language. Apps and agents may
propose local names at the API edge, but durable storage uses Schema Service
catalog identities and catalog-shaped records only.

Mini does not have a product path where an agent or app can freely mint a novel
durable schema and keep records under app-local field names. When the local
resolver is enabled, it is a reuse mechanism over a signed Schema Service pack.
It is not a local schema authority.

## Locked Flow

```text
propose / write (possibly with app-local field names)
  -> hard resolve against the local signed pack
       mode: enforce_existing_only
  <- catalog identity + adapter OR reject
  -> edge applies adapter
       local fields -> catalog fields
  -> DB persists the catalog schema identity and catalog-shaped record
```

On a miss, ambiguity, stale pack, unsupported input, or disabled resolver, Mini
calls the live Schema Service. If live resolution is unavailable and no
confident pack reuse exists, the write fails closed. A queued proposal is not a
registered identity and cannot authorize durable writes. Offline does not mean
inventing or using a local schema.

## Store Invariant

The database stores only:

- global catalog schema identities;
- records shaped to those catalog schemas;
- metadata that proves how the catalog identity was chosen.

The database does not store:

- app-local schema identities as the durable product path;
- records under app-local field dialects;
- adapter-mapped local names as the on-disk language;
- free local mints produced by agents during normal writes.

Adapters are edge-only - adapters at the edge. Inbound adapters map proposal
fields to catalog fields before mutation. Outbound adapters may map catalog
fields back to a client view for compatibility, but that view does not become
the stored representation.

## Resolver Modes

| Mode | Product meaning |
| --- | --- |
| `enforce_existing_only` | Reuse an existing catalog schema from the signed pack when confidence gates pass. Otherwise use live Schema Service or reject. |
| `shadow` | Measure local pack decisions while live Schema Service remains authoritative. |
| `live_only` | Kill-switch mode. Mini calls live Schema Service and still stores catalog-shaped records only. |
| free local mint | Not a product mode for durable app or agent writes. |

`allow_enforce` is an operator gate for rollout; it does not change the store
invariant. If enforcement is not allowed, Mini must clamp to Shadow or LiveOnly
rather than accepting free local creation.

## Creating New Catalog Schemas

New structural language is created deliberately through Schema Service, not by
Mini proxying arbitrary local declares.

The create path is direct-to-Schema-Service and gated by the service's ownership
and abuse controls. A successful create returns a global catalog identity. Mini
then loads that catalog identity and stores records under it like any other
catalog schema.

This keeps namespace as an ownership and capability boundary, not a license to
fork the structural language.

## Edge Adapter Contract

Resolution returns both a catalog target and the adapter needed for the caller's
proposal:

```text
proposal:  { todoTitle, isDone }
catalog:   Task { title, status }
adapter:   todoTitle -> title
           isDone    -> status
stored:    { title, status } under the Task catalog schema identity
```

Apps should learn and eventually speak catalog field names directly when they
can. The adapter is still first-class because it lets existing clients continue
through the transition without introducing a second on-disk language.

## Relation To Shared Surfaces

The earlier shared-surface design remains useful for governance metadata:
visibility, compatibility, ownership, lifecycle, and audit. It is no longer the
boundary that decides whether Mini may freely mint durable private structure.

Every durable app or agent write follows the same one-language rule. A shared
surface adds external governance and publish/attach metadata on top of the
catalog identity; it does not create a separate storage model.

## Migration Notes

- Existing local/private records may require one-time migration or compatibility
  reads. New product writes should target catalog identities.
- Resolver packs contain Schema Service catalog data and policy. They never
  authorize Mini to create catalog identities locally.
- Docs or code that describe generic Mini local declare as the happy path for
  durable app schemas should be treated as legacy rollout material and updated
  to this model.

## Non-Goals

- Reintroducing a WASM resolver.
- Adding runtime enforcement in this documentation-only change.
- Designing the full app store or public distribution UX.
- Allowing automatic catalog creation from arbitrary agent proposals.
