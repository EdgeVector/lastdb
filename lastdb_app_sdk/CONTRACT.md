# The app contract — `@lastdb/app-sdk`

This SDK is the **firm boundary between LastDB core and a zero-UI app.** An app
that needs no UI of its own — it connects to a node, writes into its own
namespace, reads it back, searches it, and reacts to a fixed set of refusals —
should need *only* the primitives documented here. If something a zero-UI app
needs is missing from this list, that is a gap in the contract, not something
the app should reach around the SDK to do (as the Brain's vendored subset and
the Kanban's bespoke socket client currently do — see "Convergence gaps").

The README is the full API reference. This document is the **contract**: the
small, stable surface an app depends on, and the guarantee that it does not
change shape underneath the app without a test breaking first.

---

## The app-facing primitives

### 1. Connect + identity

```ts
const app = await connect({ appId, socketPath });     // dev node: UDS-only
const app = await connect({ appId, baseUrl });        // production: TCP + consent
```

- **`appId`** is the app's identity. Every capability, every stored token, and
  every scoped read/write is keyed by it. The node decides an app's access
  scope `S(A)` from the *verified* `app_id` on the capability — the app never
  names or widens its own scope.
- **Transport** is chosen by which of `socketPath` / `baseUrl` you pass. A local
  node is reached over its **Unix-domain data-plane socket** by default (the
  `baseUrl` case socket-discovers it; see the README's "Socket-first
  discovery"). An external app-like caller connecting over that socket is the
  primary path — exercised end-to-end in `test/integration.uds.test.ts`.
- **`defaultHeaders`** carries a production node's required `X-User-Hash`
  identity header on every request. A UDS caller does not need it (the socket
  carries kernel peer credentials).
- **`timeoutMs`** bounds each transport request (default 30s), including data
  path calls, so a wedged node rejects with `TransportError` instead of hanging.
- **`requireApiVersion`** is the node request-grammar version the app needs.
  `connect` reads `GET /api/version` first and throws `NodeTooOldError` — one
  line that names the fix — when the node reports less (a node that predates
  the route reports `0`). Declare it in the app manifest; raise it only when
  the app starts sending a key an older node would refuse. A data route that
  answers `400 {kind:"unknown_key"}` throws the same error: that is the node
  saying "this client is newer than me", never "you sent a bad request".

### 2. Consent lifecycle (production nodes)

```ts
const { requestId } = await app.requestConsent('wildcard' | { explicit: [...] });
const capability   = await app.awaitConsent(requestId, { timeoutMs });
```

`requestConsent` → poll (`awaitConsent` / `pollConsentOnce`) → the SDK stores
the granted capability keyed by `(appId, node)` and auto-attaches it thereafter.
A dev node governs isolation with `folddb app trust` instead and does not serve
consent.

### 3. Data path — the app's own namespace

```ts
await app.mutate(schema, { mutationType, fields, key });   // write one row
await app.list(schema, { limit, cursor });                  // keys-only membership page
await app.query(schema, { fields, limit, offset });        // read (paginated)
await app.queryAll(schema, { fields });                    // drain past the page cap
await app.queryBatch([{ schemaName, filter }]);            // independent reads, one request
await app.search(query, { k, target });                    // node-scoped associative search
```

- **Schema registration is mandatory; app-facing alias mapping is optional.**
  Every schema used on the data path must first resolve to an identity
  registered with Schema Service. `connect({ schemaResolver })` lets an app
  address that registered schema by an app-local alias while the SDK sends the
  catalog identity/name on the wire. A resolver may return
  `{ nodeSchemaName, fields }`, where `fields` maps app field names to node
  field names. Omitting the resolver is valid only when the caller already
  supplies the registered catalog identity/name. Missing aliases or fields are
  pass-through; pass-through never authorizes an unregistered schema.
- **Field mapping is applied only at the data-path boundary.** `query` /
  `queryAll` map requested field names app→node before the request and reverse
  row `fields` plus field-keyed `metadata` node→app on read. `mutate` maps
  `fields_and_values` app→node before write. The field map must be one-to-one
  for reverse mapping to be unambiguous.
- **`mutate`** writes one row; `key` (`{hash, range}`) addresses it, and a
  `QueryRow.keyValue` from a prior read can be passed straight back to
  update/delete that exact row. An optional `expected` **CAS precondition**
  (`{type:"absent",field}` | `{type:"value",field,value}`) is forwarded verbatim
  under the node's `expected` key; a failed precondition returns
  `409 {error:"cas_conflict"}`, mapped to `CasConflictError`.
- **`query`** always paginates — a plain call caps at the node's default page
  (100). Check `result.page?.hasMore` or use `queryAll`.
- **`queryBatch`** sends independent reads, possibly from several schemas, in one
  `POST /api/queries/batch` and returns one `{ok, result | error}` outcome per
  item in request order. A failing item carries the typed error `query` would
  throw; the call throws only for a whole-request refusal. Items are not joined and
  share no work. More than 64 items are chunked. A node without the route (404/405)
  is served by plain `query` calls, with the answer remembered per client.
- **`list`** returns only live record keys plus an opaque cursor. Drain while
  `has_more` is true, then point-read the members whose fields you need. It is
  not implemented as unfiltered `queryAll({allowFullScan:true})`.
- **`queryAll`** dedupes rows by record key across pages and guards the
  node's unstable offset pagination path. If a later page contains no new keys
  while the node still reports more rows, or a completed drain collects fewer
  unique keys than `total_count`, it throws `QueryPaginationError` instead of
  returning a silently incomplete result.
- **Row envelope**: every row carries `key` + `keyValue` + `fields` +
  `metadata` + `authorPubKey` — the app-scoped write metadata a real app needs
  for provenance, not just bare field values.
- **`search`** is node-authoritative: hits come only from schemas in the app's
  `S(A)`; the optional `target` can only *narrow*, never widen.

### 4. Owner / host helpers

```ts
const identity = await app.autoIdentity();
const schemas = await app.listSchemas();
const own = await app.resolveSchema({ ownerAppId, descriptiveName, fields });
```

These helpers cover owner-context endpoints that host apps need during setup
and diagnostics but that are not part of the capability-scoped data plane:

- **`autoIdentity`** wraps `GET /api/system/auto-identity` and returns either
  the local owner `userHash` (for `X-User-Hash`) or the node's canonical
  not-provisioned 503 as `{ provisioned: false, reason, next }`.
- **`listSchemas`** wraps `GET /api/schemas` and normalizes loaded schema
  entries (`name`, `identityHash`, `ownerAppId`, `descriptiveName`, `fields`).
- **`resolveSchema`** resolves an app-owned descriptor by `ownerAppId` +
  `descriptiveName`, with an optional exact field-set guard, so apps do not
  re-derive canonical hashes by raw route parsing.

These calls intentionally do not attach `X-App-Capability` and do not describe
the app's access scope `S(A)`. They are for owner/host callers such as fbrain
and fsituations.

### 5. The machine-actionable error contract

Every refusal maps to a typed error carrying a **stable discriminator** — an app
branches on the discriminator, never on free text. See `src/errors.ts`.

| Situation | Type | Machine-actionable field |
|---|---|---|
| App not registered | `UnknownAppError` | — |
| App is sandbox-tier | `AppInSandboxError` | — |
| Owner denied / revoked / expired consent | `ConsentDeniedError` / `CapabilityRevokedError` / `ConsentExpiredError` | — |
| Namespace / identity / write isolation refusal | `PermissionDeniedError` | `.category` (`namespace_denied` \| `unverified_identity` \| `write_denied`) |
| Per-write capability refusal | `CapabilityDeniedError` | `.denialReason` — one of the eight `CAPABILITY_DENIAL_REASONS`, plus `.detail` |
| Request-shape / schema-state rejection | `RequestRejectedError` | `.kind` + verbatim `.body` (+ `.body.key` / `.body.try` on socket rejections) |
| CAS precondition failed (`mutate` `expected`) | `CasConflictError` | `.schema` / `.field` / `.key` / `.expected` / `.actual` + verbatim `.body` |
| A route the node no longer serves | `UnexpectedResponseError` | `.status` + `.body` |

For the eight discriminated capability reasons, `capabilityDenialReaction()`
returns the design's prescribed reaction (`discardToken` / `reacquire` /
`retryOnce` / `surface`) as pure data the app can act on.

### Request-shape rejections are discriminated on the socket too

`RequestRejectedError` was written around a caveat: production 400s were *not*
uniform, so the SDK surfaces the raw body verbatim and lets the app decide. On
the owner socket they were worse than non-uniform — every parse failure in
`lastdb_host::wire` collapsed to the 11 bytes `Bad Request`, with no `kind` for
the SDK to populate. The first `/api/query` anyone writes by hand omits `fields`
and hit exactly that path.

Socket rejections now serve one shape, built in `lastdb_host::reject`:

```json
{ "ok": false, "kind": "missing_required_key",
  "error": "a required key is missing from the request body",
  "key": "fields",
  "try": ["{\"schema_name\":\"<Schema>\",\"fields\":[],\"filter\":{\"HashKey\":\"<key>\"}}"] }
```

- `kind` is the discriminator the SDK already parses — no new SDK surface.
- `key` names the offending key from a closed vocabulary; `try` carries
  remediation that runs as written.
- Every byte comes from a compile-time constant on the node side, so naming the
  failed parse rule does not weaken **I4** — no caller-supplied value is echoed.

Pinned from both directions: `ERRORS.missingRequiredKey400` in
[`test/fixtures/wire.ts`](test/fixtures/wire.ts) here, and the
`reject_bodies_are_built_only_from_static_bytes` / `require_keys_*` tests in
`lastdb_host`.

---

## Why route changes break in one place

The exact JSON of every request the SDK emits and every response/error the node
returns is pinned in **[`test/fixtures/wire.ts`](test/fixtures/wire.ts)** — the
compatibility fixtures. Two test suites drive off them:

- **`test/contract.test.ts`** — asserts the SDK emits each pinned request shape
  and parses each pinned response/error into the right typed result.
- **`test/integration.uds.test.ts`** — stands up a LastDB-shaped fake node on a
  real Unix socket and drives the SDK's real transport through the full
  lifecycle (connect → mutate → query → search → the error shapes).

When a LastDB route changes shape, update the fixture to the node's new shape:
the failing assertions name exactly which SDK surface must move with it. That is
the single point of breakage the card asked for — an app never discovers a wire
drift in production.

The fixtures are verified against `origin/main` Mini / `lastdbd` data-plane
behavior: consent + capability write gate, `/api/query` + `/api/mutation` +
`/api/app/search` over UDS, and the discriminated 403 reasons under
`fold_db/crates/core/src/access/capability_denial.rs` (when present in-tree).
Do not treat a deleted `fold_db_node` crate path as the product surface —
local apps use Mini owner/app sockets (`socketPath` / `~/.lastdb/data/folddb.sock`).

---

## Convergence gaps (tracked, not closed here)

The SDK is the intended contract, but two in-tree apps do not yet consume it as
such:

- **Brain (`fbrain`)** vendors a subset of these primitives plus its own
  write/capability glue rather than depending on `@lastdb/app-sdk` directly.
- **Kanban (`fkanban`)** bypasses the SDK entirely with its own client/socket
  wrappers.

Converging them onto this contract is follow-up work (filed as separate cards),
not part of hardening the contract itself. The direction is captured in
`north-star-lastdb-core-host-app-extraction` and
`design-lastdb-apps-work-like-brain-kanban`.
