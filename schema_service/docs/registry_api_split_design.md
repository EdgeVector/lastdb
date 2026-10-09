# Schema registry API split — index / resolve / publish

**Status:** Design-ready contract. Drafted with Claude, 2026-07-07, from the
North Star `schema-service-local-first-schema-sync` (fbrain). Unblocks the
endpoint-implementation cards; the prior "paused pending a simpler model" gate
was cleared by Tom on 2026-07-07 (see "Cleared conflict" below).
**Scope:** the `/v1/*` contract in `openapi.yaml` and the handlers in
`crates/server_shared` / `crates/core`. This document is the spec those
implementation cards target; it does **not** itself change runtime behavior.
**North Star:** *the registry is replicated for dedup; the registry is
protected for mutation.*

## TL;DR — what to build

Split today's overloaded `POST /v1/schemas` into three roles with distinct
trust tiers:

| Endpoint | Role | Persists shared state? | Default auth |
|---|---|---|---|
| `GET  /v1/registry/index`   | **Knowledge** — signed, compact dedup snapshot for local caches | no | public (read) |
| `POST /v1/schemas/resolve`  | **Dedup for cache misses** — stateless "does an equivalent already exist?" | no | public (read), light anti-abuse |
| `POST /v1/schemas`          | **Mutation / publish** — contribute a schema into the shared registry | yes | node-key + PoW, or DevCert (namespaced) |

The rule that drives every decision below: **reads of registry knowledge are
open and cheap; writes to the shared registry are protected and deferred.**
Normal local note/file/event ingestion must never synchronously depend on a
protected write — it dedups against a cached `index`, falls back to a stateless
`resolve` on a cache miss, and only queues a `publish` for later async sync.

### Cleared conflict (read this before re-litigating)

The earlier hesitation was that a *local* WASM resolver looked like it made the
service redundant or moved source-of-truth onto the node. It does not. The
local resolver is a **cache/dedup shortcut over `schema_service`**, not a
local-only primitive:

- The **service remains the source of truth** for creation, classification,
  audit, supersession, and bootstrap.
- A high-confidence **local** match reuses a **service-canonical** schema
  (identity hashes originate from, and are reconciled against, the service).
- A local **miss** still calls the service (`resolve`, then maybe `publish`).

So the split below is not "move the registry to the node." It is "replicate the
registry *knowledge* to the node for fast dedup, and keep every *mutation*
behind the service's trust gate."

---

## Where this sits relative to today's endpoints

Today `POST /v1/schemas` (`add_schema`) already carries an
`offer_to_shared_discovery: bool` flag (default `false`) that partially
prefigures this split:

- `false` — a **local namespace claim**. The node already computes the
  deterministic `identity_hash`, so this "claims" a content-addressed namespace
  with no DevCert.
- `true` — an **offer into shared discovery**, gated on the owner's DevCert
  (`X-Exemem-Dev-Cert` + `X-Signature`).

The related read/dedup surfaces that this design generalizes or replaces:

- `POST /v1/schemas/batch-check-reuse` (`batch_check_reuse`) — the closest
  existing analog to `resolve`; returns a `SchemaReuseMatch` per proposed
  `descriptive_name`. `resolve` supersedes it (see "Compatibility").
- `GET /v1/snapshot` (`snapshot_export`) — the **full** hydration snapshot
  (schemas + views + transforms + all three embedding caches), gated in prod by
  `X-API-Key`. `registry/index` is a **narrower, signed, publicly-cacheable**
  projection meant for continuous dedup — not a hydration/replace payload.

This design **reframes** the claim-vs-offer distinction rather than deleting it:

- The `false` "local claim" call becomes **unnecessary** — a pure local claim
  needs no network round-trip at all (the node owns the deterministic identity
  hash locally; see "Local claim needs no endpoint"). The claim path stays
  accepted during the compatibility window, then is retired.
- The `true` "offer to shared discovery" call **is** the new
  `POST /v1/schemas` publish semantics, made the endpoint's only job.

---

## 1. `GET /v1/registry/index` — signed dedup snapshot (knowledge)

The compact, signed projection of the shared registry that a node caches and
dedups against locally. Public and cacheable; carries no WASM and no per-node
data.

### Response shape

```jsonc
{
  "registry_version": 48213,          // monotonic u64, bumped on every mutation
  "merkle_root": "b3:9f8c…",          // BLAKE3 root over the canonical entry set
  "generated_at": "2026-07-07T18:04:11Z",
  "embedder_version": "bge-small-en-v1.5",  // embedding space of `field_embeddings`
  "entries": [
    {
      "identity_hash": "sha256:1a2b…", // canonical schema identity (service-issued)
      "descriptive_name": "Recipe Collection",
      "owner_app_id": "fbrain",         // null for un-namespaced/system schemas
      "source": "user",                 // system_seed | starter_seed | user
      "fields": ["title", "ingredients", "steps"],
      "field_descriptions": {           // optional; present when known
        "title": "human-readable recipe title"
      },
      "field_embeddings": {             // optional compact vectors, embedder_version space
        "title": "f16b64:…"             // base64 of f16-quantized vector, or omitted
      },
      "purpose_statement": "Recipes the user wants to cook."
    }
    // …
  ],
  "supersessions": {                    // superseded_identity_hash -> canonical_identity_hash
    "sha256:old…": "sha256:new…"
  },
  "signature": {
    "alg": "Ed25519",
    "key_id": "sha256:…",               // sha256(SPKI DER) of the registry signing key
    "sig": "base64…"                    // signature over the canonical bytes (below)
  }
}
```

### Versioning & signature semantics

- **`registry_version`** is a monotonic `u64` bumped on every accepted mutation
  (`POST /v1/schemas`, admin dedupe/deprecate, supersession). It is the cache
  key: a client stores the version it last synced and asks for deltas.
- **`merkle_root`** is a BLAKE3 root over the sorted, canonicalized entry set
  (each leaf = `blake3(JCS(entry))`, sorted by `identity_hash`). It lets a
  client verify integrity of a delta-assembled index without re-fetching the
  whole thing, and lets two nodes compare registry state by a single hash.
- **Signature** is Ed25519 over `JCS({everything except signature})` — i.e. the
  canonical serialization of the body with the `signature` field removed. Reuse
  the existing `app_identity_crypto` canonicalization (`json_canon`) so the
  registry signer and node verifier cannot silently diverge (same discipline as
  the DevCert JCS-parity test). The **registry signing key** is a new,
  service-held Ed25519 key (distinct from the exemem ES256 *root* and from
  per-developer keys); its public SPKI is distributed to nodes via config
  (`REGISTRY_INDEX_PUBKEYS`, a *set* to allow rotation, mirroring
  `APP_IDENTITY_ROOT_PUBKEYS`). A node MUST reject an index whose signature does
  not verify against a configured pubkey.
- **Delta fetch:** `GET /v1/registry/index?since=<version>` returns only entries
  and supersessions changed after `<version>`, plus the new
  `registry_version` / `merkle_root` / `signature`. A response field
  `full: bool` indicates whether the body is a full snapshot (the server MAY
  force a full response — e.g. `since` too old / compacted — signaled by
  `full: true`). `since` omitted ⇒ full index.
- **Caching:** the server SHOULD emit strong `ETag: "<registry_version>"` and
  honor `If-None-Match` (`304`). The index is CDN/edge-cacheable because it is
  identical for all readers and self-authenticating via the signature.

### Auth

**Public read.** The index is shared, non-sensitive registry knowledge and is
signed, so it needs no caller identity. Anti-scrape/DoS protection is
network-level (rate limit, CDN), not per-request auth. It never contains
per-node/user data or WASM.

---

## 2. `POST /v1/schemas/resolve` — stateless dedup for cache misses

When a node's local dedup against the cached `index` is *uncertain* (a near
miss, an embedding gap, or a stale cache), it asks the service to resolve one or
more proposals. **This endpoint does not persist shared registry state** — it is
a pure query over the current registry. (It may touch a read-through embedding
cache; "mostly-stateless" = no *registry* mutation, no publish.)

### Request

```jsonc
{
  "client_registry_version": 48100,    // optional; what the caller last synced
  "proposals": [
    {
      "descriptive_name": "Recipe Collection",
      "fields": ["title", "ingredients", "steps"],
      "field_descriptions": { "title": "…" },   // optional
      "purpose_statement": "Recipes the user wants to cook.", // optional
      "identity_hash": "sha256:…"       // optional; the node's locally-computed id
    }
  ]
}
```

### Response — one outcome per proposal

```jsonc
{
  "registry_version": 48213,            // server's current version
  "cache_stale": true,                  // true iff client_registry_version < registry_version
  "results": {
    "Recipe Collection": {
      "outcome": "reuse",               // reuse | novel | candidate_equivalent | refresh
      "match": { /* SchemaReuseMatch: schema, is_exact_match, field_rename_map,
                    is_superset, unmapped_fields, matched_descriptive_name */ },
      "candidates": [ /* for candidate_equivalent: ranked SchemaReuseMatch[] */ ],
      "confidence": 0.94
    }
  }
}
```

The four outcomes (straight from the North Star):

- **`reuse`** — an equivalent shared schema exists; `match` names the canonical
  schema and the `field_rename_map` to adopt it. The node maps its
  `local_schema_hash -> shared_schema_hash` and reuses the canonical.
- **`novel`** — no equivalent; the node keeps its **local** schema identity and,
  if the schema is publishable/consented, queues a `publish`.
- **`candidate_equivalent`** — one or more *plausible* equivalents that need a
  user/app decision (not safe to auto-merge). `candidates[]` is the ranked list;
  the node surfaces or defers the choice and does not silently rebind.
- **`refresh`** — the client's cache is stale (`client_registry_version` behind);
  the node should re-pull `GET /v1/registry/index?since=…` and re-dedup locally
  before deciding. (`cache_stale` at the top level is the same signal.)

### Auth

**Public read**, same rationale as `index` — it returns only registry knowledge
and writes nothing shared. To bound abuse (this path can run an embedder), apply
a **light anti-abuse tier**: per-IP/per-node rate limiting, a batch-size cap on
`proposals`, and OPTIONALLY a cheap **proof-of-work** stamp
(`X-PoW: <nonce>` over a rotating server challenge) required only above a
free-tier request rate. PoW here is a throttle on the *expensive read*, not an
identity gate. No DevCert required — resolving is not mutating.

---

## 3. `POST /v1/schemas` — mutation / publish only

Reduced to its single real job: **contribute a schema into the shared
registry** (what today's `offer_to_shared_discovery: true` does). Creation,
canonicalization, classification, supersession, and audit all still happen
**here, on the service** — this is the source-of-truth write path.

### Request (unchanged envelope, `offer` semantics implied)

Keep `AddSchemaRequest` (`schema`, `mutation_mappers`) — a publish is
inherently an "offer to shared discovery," so the `offer_to_shared_discovery`
flag becomes redundant on this endpoint (accepted-but-ignored during the compat
window; a publish always offers). Response is unchanged (`AddSchemaResponse`:
`201` newly added/expanded, `200` already existed, `400` validation).

### Auth tiers on publish (this is where protection lives)

Two accepted writer credentials, chosen by *what* is being published:

1. **App-namespaced publish → DevCert + DevSignature (required).**
   Any schema carrying an `owner_app_id`, or any publish that claims/creates a
   namespaced canonical, requires the app owner's `X-Exemem-Dev-Cert` (ES256,
   exemem root) + `X-Signature` (Ed25519 `schema_claim` envelope over the
   `schema` sub-object) — the gate already implemented for
   `offer_to_shared_discovery=true`. `401` cert_required/invalid/expired,
   `403` dev_revoked, `503` app_identity_not_configured (unchanged from the
   transforms/apps gate).

2. **Un-attested community publish → node-key + proof-of-work.**
   A node with no DevCert may still contribute a *non-namespaced* schema to the
   shared commons, but must present:
   - `X-Node-Key` / `X-Node-Signature` — an Ed25519 signature by the node's own
     key over the request (identifies the submitter for rate-limiting/revocation
     without a central issuer), **and**
   - `X-PoW` — a proof-of-work stamp over a server-issued, time-boxed challenge,
     scaled so that one publish is cheap but bulk spam is expensive.

   This is the anti-spam boundary the North Star calls for: PoW/quotas apply to
   *shared registry mutation*, never to local ingestion. `429` when over quota,
   `401 pow_required`/`pow_invalid` when the stamp is missing/stale.

**Never** required for the two read endpoints above.

### Local claim needs no endpoint

A pure local namespace claim (today's `offer_to_shared_discovery=false`) is
**removed from the wire**: the node computes the deterministic `identity_hash`
locally and records ownership in its own store. No service round-trip, so
ingestion cannot be blocked by service availability or a write gate. The old
`false`-flag call is accepted during the compat window (below) but nodes on the
new resolver stop emitting it.

---

## Path → trust-tier matrix (summary)

| Path | Tier | Credential |
|---|---|---|
| `GET /v1/registry/index` | public read | none (signed body); rate-limited |
| `POST /v1/schemas/resolve` | public read | none; rate-limit + optional light PoW |
| `POST /v1/schemas` (namespaced) | app-owner | DevCert (ES256) + DevSignature (Ed25519) |
| `POST /v1/schemas` (community, un-namespaced) | anti-spam | node-key sig + proof-of-work + quota |
| local namespace claim | none | *no endpoint* — node-local, deterministic hash |
| `GET /v1/snapshot` (full hydration) | operator | `X-API-Key` (prod), unchanged |

---

## Compatibility plan for existing `POST /v1/schemas` clients

Existing clients (notably `fold_db_node`'s `schema_service_client`, plus the
dogfood/app-identity flows) call `POST /v1/schemas` with and without
`offer_to_shared_discovery`, and call `POST /v1/schemas/batch-check-reuse` for
dedup. Migration is staged so nothing breaks:

1. **Additive first.** Ship `GET /v1/registry/index` and
   `POST /v1/schemas/resolve` as **new** routes. `POST /v1/schemas` and
   `batch-check-reuse` keep their exact current behavior. No client change is
   forced by this step.
2. **`batch-check-reuse` → `resolve` alias.** Implement `batch-check-reuse` as a
   thin adapter over the `resolve` core (map `SchemaLookupEntry[]` →
   `proposals[]`, project `results` back to the old `matches` map). Mark it
   `deprecated: true` in `openapi.yaml`. Existing callers keep working; new
   callers use `resolve`.
3. **`offer_to_shared_discovery` becomes advisory.** On `POST /v1/schemas`,
   treat any call as a publish/offer; a `false` flag from an old client is
   accepted and logged (observe-mode counter) but does not create a distinct
   "claim-only" server record — the node now owns local claims itself. Keep the
   field in the schema (accepted, ignored) for at least one release.
4. **Node cutover.** `fold_db_node` moves to: dedup against cached `index` →
   `resolve` on miss → local claim in-process → queue `publish` for
   consented/publishable novels. This is a node-side change tracked by its own
   card(s); the service stays back-compatible throughout.
5. **Retire the claim path.** Once nodes no longer emit `false`-flag claims
   (confirmed via the observe-mode counter reaching ~zero), drop the field and
   the claim-only branch from the handler. `batch-check-reuse` is removed only
   after callers are confirmed migrated (a later, separate card).

No breaking change lands in a single step; each removal is gated on telemetry
showing the old path is unused.

---

## Dev-first / observe-mode sequencing (hardening after rollout)

The auth *tightening* on `POST /v1/schemas` and the new read endpoints roll out
behind guards so we never flip a hard gate on prod before the local-first
resolver is proven. Order:

1. **Dev only, additive.** Land `index` + `resolve` on the **dev** deploy
   (`schema_service` actix binary / dev Lambda) first. Prod unchanged.
   (Consistent with the resolver-pack work already shipped in **shadow mode**,
   PR #193 / #188 — the local resolver runs alongside the service without
   changing outcomes.)
2. **Observe mode for the new write tiers.** Implement the node-key+PoW and the
   DevCert-on-community-publish checks in **observe (log-only)** mode first:
   evaluate the credential, emit a structured metric
   (`would_reject{reason=…}`), but **do not** reject. Watch the counters on dev
   with real dogfood traffic (the `dogfood-fbrain` / app-identity flows) to
   confirm legitimate publishes would pass and only spam would trip.
3. **Enforce on dev.** Flip the write tiers to enforcing on dev once observe
   shows a clean separation; keep prod in observe.
4. **Enforce on prod (human-gated cutover).** Only after dev is stable and the
   node fleet has migrated does prod flip to enforcing. This is the one step
   that is a deliberate human cutover (it can reject a real publish), so it is
   **not** auto-merged/auto-flipped — it's a config change a human lands per the
   no-prod-cutover-while-a-plan-is-in-flight rule.

Every stage is reversible by config (`REGISTRY_WRITE_ENFORCE=observe|enforce`,
per env), so a regression is a flag flip, not a redeploy.

---

## Endpoint-implementation cards this unblocks (suggested split)

Each is independently landable against this contract:

1. `GET /v1/registry/index` — response type, BLAKE3 merkle build, Ed25519
   signing key + config, `?since` delta + ETag/304. Dev-first.
2. `POST /v1/schemas/resolve` — resolve core, four-outcome mapping, batch cap +
   rate limit; re-express `batch-check-reuse` as an alias.
3. `POST /v1/schemas` publish hardening — node-key+PoW tier and community-vs-
   namespaced routing, all in **observe mode** first.
4. Node-side cutover in `fold_db_node` — cached-index dedup → resolve →
   local claim → deferred publish queue (`pending_schema_publications`).
5. Prod enforcement cutover (config; human-gated) — last.

Open questions deferred to those cards (not blockers for this contract):
compact-embedding quantization format (`f16` vs int8) and the exact PoW
difficulty curve are tuning knobs to settle with dev telemetry, not design
forks.
