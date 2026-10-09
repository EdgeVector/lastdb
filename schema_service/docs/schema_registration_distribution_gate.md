# Schema registration: retired distribution proxy notes

Status: superseded for durable writes by
[direct_schema_registration.md](direct_schema_registration.md)

Brain: `preferences-everything-through-schema-service`

The original slice below described a local-solo path where Mini could mint
private app schemas and defer Schema Service until distribution. That is no
longer the product model for durable app or agent writes. Mini may still use
local development fixtures or compatibility reads, but new durable writes must
resolve to a Schema Service catalog identity and store catalog-shaped records.

## Slice

| Mode | Schemas | Schema Service |
| --- | --- | --- |
| **Solo / local-only app** | Legacy local-mint path; not the durable product path for new writes | Superseded by direct catalog resolve |
| **App used by other people** (download / install / depend) | Registered catalog identities plus publish metadata | Required |

Receivers of third-party apps load **registered** identities only. They do not
trust unregistered local invents from the publisher's laptop.

Durable writes now follow this path:

```text
build app locally
  -> propose schemas / field names at the edge
  -> resolve to Schema Service catalog identities
  -> store catalog-shaped records

publish / share app for others
  -> POST /api/apps/shared-surface/publish-attach
       records explicit shared-surface metadata for the catalog identity
  -> POST /api/apps/verify-distribution-ready
       fails if any required identity is missing on the service
  -> package/installers may proceed only when verify succeeds
```

## Wire surfaces (Mini)

### Declaration

- `POST /api/apps/declare-schema` — canonical app operation that resolves and
  exact-loads an existing catalog identity (`resolution: "reuse"`) or
  registers a novel shape/expansion through Schema Service before exact-load
- `POST /api/schemas/declare` — strict compatibility adapter into that same
  implementation; it is not a second lifecycle

Both routes must converge on the same invariant: resolve/register first, adapt
at the edge, and persist under a global catalog schema identity. There is no
local-only declaration mode for new durable app or agent writes.

Both request shapes accept only `intent: "catalog_sync"` (or its default).
Unknown controls such as `copy_rows`, `local_mint`, or an implicit migration
intent are rejected and audited. Field additions reuse predecessor molecules
through catalog field mappers. Incompatible key-layout migration is a separate,
explicitly designed operation and cannot be smuggled into declaration.

### Distribution Readiness

- `POST /api/apps/shared-surface/publish-attach`

  Publishes or attaches explicit shared-surface metadata for a catalog identity.
  This is the only Mini product entry for app/shared-surface publication.

- `POST /api/apps/verify-distribution-ready`

  Request:

  ```json
  {
    "app_id": "fbrain",
    "schema_identities": ["<identity_hash>", "..."]
  }
  ```

  Behavior: for each identity, `GET` Schema Service. Ready iff **all** resolve.
  Response includes `ready: true|false` and per-id status.

## Spam / trust

- Resolver pack reuse avoids hitting Schema Service for common writes.
- Shared-surface publish hits the service; creation is gated by Schema Service
  ownership and abuse controls. Mini no longer proxies arbitrary app schemas
  into the registry.
- Third-party install path must call verify (or equivalent) before treating
  an app as distributable.

## Non-goals (this PR)

- Full app-store packaging UX
- Changing fbrain/fkanban init (already service-backed for consumers)
- Enforcing PoW difficulty retune (separate)
- Requiring Schema.org absorption of app schemas
