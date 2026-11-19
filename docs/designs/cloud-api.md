# Cloud API (ExeMem): one contract

Status: design for review (2026-11-19). This is not the live contract yet.
When this document and `docs/designs/cloud-api.openapi.json` differ, the
OpenAPI file wins for paths, auth, and error shapes.

Tom (2026-11-19): action-based interface — three verbs, node nouns, one
standard envelope. Cloud storage may change without LastDB knowing.

## Why

Today the cloud surface is a hand-dispatched tree under
`POST /api/cloud/backups/v2/…` with no route spec and no real wire version.
The pin lint never checks the next cloud against the last stable node (#1799).

## Sources (verified)

| Source | Role |
|---|---|
| `exemem-infra` `storage_service` | Dispatch |
| `cloud_backup_protocol` | Wire types |
| `lastdb_node` `log_sync/`, `named_grant/`, `cloud_backup_client/` | Call sites |
| `scripts/cloud_backup/snapshot_attempt_account.py` | lastdb DEV snapshot-proof |

## Hard rules

- No dual paths, aliases, fallback, or migration.
- No unit tests. E2E DEV checks in CI are fine.
- Share, Deliver, Org exist only in People.
- No cloud storage nouns in routes or request fields.

## Shape

```
POST /api/cloud/v3/fetch
POST /api/cloud/v3/push
POST /api/cloud/v3/manage
POST /api/cloud/v3/account
POST /api/cloud/v3/maintenance
```

### Standard envelope (Decision 16)

**Product** (fetch / push / manage) — always includes `database_id`:

```json
{ "database_id": "<uuid>", "intent": "<enum>", "args": { } }
→ { "database_id": "<uuid>", "intent": "<same>", "result": { } }
```

**Account / maintenance** — no `database_id` (Decision 18):

```json
{ "intent": "<enum>", "args": { } }
→ { "intent": "<same>", "result": { } }
```

Auth stays in headers and tokens — not in the envelope. Errors stay
`{ "code", "field" }` (+ `minimum_version` on 426).

`manage`/`create` uses the product envelope: the **node chooses**
`database_id` (UUID) today (`native.rs` `CreateBody`) and sends it.

OpenAPI: per-intent `args` / `result`, tagged by `intent`; request body is
`oneOf` envelope or array of envelopes (Decision 17) where batching applies.

### Batching (Decision 17)

Each of `fetch`, `push`, and `manage` accepts **either** one envelope **or** a
JSON **array** of envelopes for that same action. Actions never mix on one
call.

- Array response: same length and order. Each item is
  `{database_id, intent, result}` **or**
  `{database_id, intent, error: {code, field}}`.
- Items are **independent**: one failure does not fail the others; nothing is
  atomic across items.
- HTTP **200** when the batch was processed. Whole-request failures (auth,
  malformed body, 426 `upgrade_required`, over the request size cap, or over
  the item-count cap) return the normal error body with **no** array.
- Auth: same token for the call; each item is authorized against its own
  `database_id`.
- In-intent lists (`entries[]`, `grants[]`, `node_keys[]`) are unchanged.

**Caps (from existing limits):**

| Cap | Value | Why |
|---|---|---|
| Max items per batch | **90** | Same as `mutation_log::APPEND_BATCH_LIMIT` and `LIST_PAGE_LIMIT`. One batch of databases is the same cardinality as one append/list page of work. |
| Request body | **256 KiB** (`CONTROL_JSON_MAX_BYTES`) | Same control-JSON ceiling the node already enforces before POST. Over → whole-request error. |
| Response body | **8 MiB** (`lastdb_node` `http::JSON_LIMIT`) | Same bound used when reading list/append replies. |

**Response overflow:** process items in order. When encoding the next success
would exceed 8 MiB, that item and every remaining item get
`error: { "code": "batch_too_large", "field": "batch.response" }` with no
`result`. Earlier items keep their results. Prefer per-item errors over
rejecting the whole batch so independence holds.

Over **90** items → whole-request `{ "code": "batch_too_large", "field": "batch.items" }`.

### Intents

| Action | Intents |
|---|---|
| `fetch` | `mutations_since`, `snapshot`, `file` |
| `push` | `mutations`, `snapshot`, `file` |
| `manage` | `create`, `grant`, `revoke`, `access`, `delete` |
| `account` | `enroll_start`, `enroll_finish`, `login_start`, `login_finish`, `register`, `status`, `key_create`, `key_list`, `key_revoke`, `payment_setup_intent`, `payment_customer_session`, `payment_methods`, `payment_detach_method`, `payment_status`, `payment_checkout`, `payment_portal` |
| `maintenance` | `snapshot_proof_provision`, `snapshot_proof_retire`, `snapshot_proof_status`, `snapshot_proof_inspect` |

**Added earlier:** `manage`/`access`, `manage`/`delete`.
**Added (Decision 18):** `account`, `maintenance` — fold former auth/developer
URLs and DEV snapshot-proof into the same action+intent model.

## Examples (one per action)

### fetch

```json
{
  "database_id": "11111111-1111-4111-8111-111111111111",
  "intent": "mutations_since",
  "args": { "cursor": "opaque-cursor", "limit": 100 }
}
```

```json
{
  "database_id": "11111111-1111-4111-8111-111111111111",
  "intent": "mutations_since",
  "result": {
    "cursor": "opaque-latest",
    "mutations": [
      {
        "stream": "direct",
        "cursor": "opaque-1",
        "writer_node_key": "…",
        "device_id": "…",
        "writer_seq": 1,
        "authored_at_ms": 0,
        "namespace": "…",
        "payload_b64": "…"
      }
    ]
  }
}
```

### push

```json
{
  "database_id": "11111111-1111-4111-8111-111111111111",
  "intent": "mutations",
  "args": {
    "device_id": "device-1",
    "entries": [
      {
        "writer_seq": 1,
        "authored_at_ms": 0,
        "namespace": "…",
        "payload_b64": "…"
      }
    ]
  }
}
```

```json
{
  "database_id": "11111111-1111-4111-8111-111111111111",
  "intent": "mutations",
  "result": { "landed": "direct", "cursor": "opaque-2", "accepted": 1 }
}
```

### manage

```json
{
  "database_id": "11111111-1111-4111-8111-111111111111",
  "intent": "delete",
  "args": {}
}
```

```json
{
  "database_id": "11111111-1111-4111-8111-111111111111",
  "intent": "delete",
  "result": { "status": "accepted" }
}
```

### batch fetch (three databases)

```json
[
  {
    "database_id": "11111111-1111-4111-8111-111111111111",
    "intent": "mutations_since",
    "args": { "cursor": "c-a", "limit": 50 }
  },
  {
    "database_id": "22222222-2222-4222-8222-222222222222",
    "intent": "mutations_since",
    "args": { "cursor": "c-b", "limit": 50 }
  },
  {
    "database_id": "33333333-3333-4333-8333-333333333333",
    "intent": "mutations_since",
    "args": {}
  }
]
```

```json
[
  {
    "database_id": "11111111-1111-4111-8111-111111111111",
    "intent": "mutations_since",
    "result": { "cursor": "c-a-next", "mutations": [] }
  },
  {
    "database_id": "22222222-2222-4222-8222-222222222222",
    "intent": "mutations_since",
    "error": { "code": "unauthorized", "field": "database.grant" }
  },
  {
    "database_id": "33333333-3333-4333-8333-333333333333",
    "intent": "mutations_since",
    "result": { "cursor": "c-c", "mutations": [] }
  }
]
```

## Decisions (Tom, 2026-11-19)

1. Protocol **v3**.
2. Version in **header + token**; no body `version`.
3. **426** `upgrade_required` + `minimum_version`.
4. Signed `grant#<node key>` — **no list, no revision**.
5. **No** protocol header on auth-service.
6. Live OpenAPI under **`cloud_backup_protocol/`**.
7. DEV snapshot-proof stays, marked **`x-lastdb-environment: dev`** (lastdb CI);
   lives under **`maintenance`** (Decision 18).
8. Action-based surface: `fetch` / `push` / `manage` (+ `account` /
   `maintenance` in Decision 18).
9. `push`/`mutations`: no `kind`; `landed` from grant.
10. `fetch`/`mutations_since`: publisher sees direct+proposal; receivers direct
    only; accept = publisher push; entries tagged `stream`.
11. **Silent rejections** — no discard call/field; node’s `Discarded` settle
    goes away.
12. **Link rules + access under manage** (`rules` on create/grant;
    `manage`/`access`).
13. **Cleanup ExeMem-internal** — no public ops endpoint.
14. **Ingested proposals deletable** when the publisher’s fetch `cursor` moves
    past them (internal cleanup). Safe: node advances cursor only after durable
    apply (`run.rs`).
15. **`manage`/`delete`** — publisher deletes the database and all its cloud
    data (mutations, proposals, files, snapshots, grants, link rules) so the
    owner reclaims storage. ExeMem performs deletion (sync or background);
    `result.status` is `done` or `accepted`. This is the user-facing reclaim
    path; retention/cleanup jobs stay internal (Decision 13).
16. **Standard envelope** — every fetch/push/manage call uses
    `{database_id, intent, args}` / `{database_id, intent, result}`.
17. **Batching** — each action accepts one envelope or an array of envelopes
    for that action only. Independent per-item results/errors; HTTP 200 for a
    processed batch. Max **90** items; request ≤ 256 KiB; response ≤ 8 MiB with
    per-item `batch_too_large` when the response budget is exhausted.
18. **`account` and `maintenance` actions.** Former `/api/auth/*`,
    `/api/developer/*`, and DEV snapshot-proof become intents under
    `POST /api/cloud/v3/account` and `POST /api/cloud/v3/maintenance`.
    Same envelope style **without** `database_id`. One intent namespace even if
    deploy splits hosts behind the ExeMem HTTP API gateway (node today calls
    `exemem_api_url_for` for both `/api/auth/*` and `/api/developer/*`).
    **No protocol header on `account`** (extends Decision 5). **Protocol
    header on `maintenance`** (storage DEV tooling). Payments are Stripe
    pass-through inside `result.stripe` (and typed status fields).
    `enroll_*` / `login_*` must not be batched (challenge/response).

## Account and maintenance

### Hosts

| Intents | Today’s paths (node) | Host |
|---|---|---|
| `register`, payments, keys, status | `/api/auth/cli/register`, `/api/auth/payments/*`, `/api/developer/api-keys` | Same ExeMem API base URL |
| `enroll_*`, `login_*` | prior auth-service surface | Same action; gateway may route to auth Lambda |
| `snapshot_proof_*` | `/api/cloud/backups/v2/dev-snapshot-proof` | ExeMem storage; now `maintenance` |

No second request shape. Gateway may fan intents to different Lambdas.

### Account batching

Allowed for `register`, `status`, `key_*`, `payment_*`. **Rejected** for
`enroll_start` / `enroll_finish` / `login_start` / `login_finish` (whole-request
`{ "code":"batch_not_allowed", "field":"batch.items" }` if the array contains
any of those).

### Maintenance — considered and rejected

| Candidate | Verdict |
|---|---|
| DEV snapshot-proof | **Keep** — lastdb CI (`snapshot_attempt_account.py`) |
| Node `/health`, `/api/version` | **Reject** — local UDS only, not cloud |
| Reclaim / retention / `*_dev` cleanup | **Reject** — ExeMem-internal (Decision 13) |
| Payment / keys | **Reject here** — under `account` |

## Intent args and results

### fetch / `mutations_since`

| args | `cursor?`, `limit?` |
| result | `cursor` (latest; folds head), `mutations[]` |

`Mutation`: `stream` (`direct`\|`proposal`), `cursor`, `writer_node_key`,
`device_id`, `writer_seq`, `authored_at_ms`, `namespace`, `payload_b64`,
`incorporated_from?`.

### fetch / `snapshot`

| args | `restore_point_id?`, `part_handle?` |
| result | `result: describe` → `restore_point_id`, `part_handles[]` · or `result: part` → `part_handle`, `bytes_b64`, `length` |

(Nested `result` tag inside the envelope `result` object distinguishes describe
vs part.)

### fetch / `file`

| args | `file_handle`, `offset`, `length` |
| result | `file_handle`, `offset`, `length`, `bytes_b64` |

### push / `mutations`

| args | `device_id`, `entries[]` |
| result | `landed` (`direct`\|`proposal`), `cursor`, `accepted` |

### push / `snapshot`

| args | `stage` (`prepare`\|`publish`), `ticket?`, `restore_point_id?` |
| result | `stage`, `ticket`, `restore_point_id?`, `upload_url?` |

### push / `file`

| args | `stage` (`begin`\|`complete`), `ticket?`, `size_bytes?`, `content_sha256?` |
| result | `stage`, `ticket`, `file_handle?`, `upload_url?` |

### manage / `create`

| args | `rules?` |
| result | `access?` (`state`, `role?`, `signed_grant?`) |

### manage / `grant`

| args | `grants[]` (`node_key`, `signed_grant`), `rules?` |
| result | `ok`, `access?` |

### manage / `revoke`

| args | `node_keys[]` |
| result | `ok` |

### manage / `access`

| args | `mutation_id?` |
| result | `state`, `role?`, `signed_grant?`, `contribution_status?` (`submitted`\|`published`\|`unknown`) |

### manage / `delete`

| args | `{}` (empty object) |
| result | `status` (`done`\|`accepted`) |

Publisher only. Deletes all cloud data for `database_id`.

### account (no `database_id`)

| intent | args (summary) | result (summary) |
|---|---|---|
| `enroll_start` / `login_start` | `{}` | `challenge`, `session` |
| `enroll_finish` / `login_finish` | `session`, `credential` | `ok`, `account_id?` |
| `register` | `public_key`, `timestamp`, `signature`, `device_id` | `ok`, `api_key`, `account_id` |
| `status` | `{}` | `ok`, `account_id`, `configured`, … |
| `key_create` | `{}` | `key`, `prefix`, `created_at` |
| `key_list` | `{}` | `keys[]` |
| `key_revoke` | `prefix` | `ok`, `revoked` |
| `payment_setup_intent` / `payment_customer_session` / `payment_methods` / `payment_checkout` / `payment_portal` | (see OpenAPI) | Stripe pass-through in `stripe` |
| `payment_detach_method` | `method_id` | Stripe pass-through |
| `payment_status` | `{}` | typed subscription flags (CLI today) |

### maintenance (DEV-only)

| intent | args | result |
|---|---|---|
| `snapshot_proof_provision` | `proof_id?` | `ok`, `proof_id` |
| `snapshot_proof_retire` | `proof_id` | `ok` |
| `snapshot_proof_status` | `proof_id` | `ok`, `status` |
| `snapshot_proof_inspect` | `proof_id` | `ok`, `detail?` |

## Use cases

| Use case | Intent |
|---|---|
| Create database | `manage`/`create` |
| Delete database / reclaim storage | `manage`/`delete` |
| Append / propose | `push`/`mutations` |
| Read mutations (+ publisher proposals) | `fetch`/`mutations_since` |
| Accept proposal | publisher `push`/`mutations` |
| Ignore proposal | silent; ExeMem deletes after fetch cursor advances |
| Upload / read file | `push`/`file`, `fetch`/`file` |
| Snapshot backup / restore | `push`/`snapshot`, `fetch`/`snapshot` |
| Grant / revoke | `manage`/`grant`, `manage`/`revoke` |
| Link rules | optional `rules` on create/grant |
| Self-access / enrollment / contribution status | `manage`/`access` |
| Retention / reclaim jobs / `*_dev` cleanup | ExeMem-internal |

## Snapshot actions

| Current | Disposition |
|---|---|
| prepare, publish | `push`/`snapshot` |
| read, read-for-restore, download | `fetch`/`snapshot` |
| retention, reclaim*, `*_dev` | ExeMem-internal |
| index-status, index-history | Absent |

## Old → new

| Old (v2) | New (v3) |
|---|---|
| `…/mutation-log/create-database` | `manage`/`create` |
| *(no delete route)* | `manage`/`delete` |
| `…/mutation-log/access` | `manage`/`access` |
| `…/mutation-log/append` | `push`/`mutations` |
| `…/mutation-log/head` | `fetch`/`mutations_since` |
| `…/mutation-log/list` | `fetch`/`mutations_since` |
| `…/mutation-log/inbox/list` | `fetch`/`mutations_since` (publisher) |
| `…/mutation-log/inbox/settle` (accept) | publisher `push`/`mutations` |
| `…/mutation-log/inbox/settle` (discard) | removed (silent; Decision 11/14) |
| `…/mutation-log/settle-submissions` | dropped |
| `…/mutation-log/blob/bytes` | `push`/`file` |
| `…/mutation-log/delivery/object` | `fetch`/`file` |
| `…/snapshot/bytes` prepare/publish | `push`/`snapshot` |
| `…/snapshot/bytes` read* / download | `fetch`/`snapshot` |
| `…/snapshot/bytes` retention/reclaim/`*_dev` | ExeMem-internal |
| `…/share/members/set` | `manage`/`grant` / `revoke` |
| `…/share/members/get` / `list` | dropped |
| `…/share/members/read` | `manage`/`access` |
| `…/share/members/settings/*` | `rules` on create/grant; read via access |
| `…/dev-snapshot-proof/{op}` | `maintenance` / `snapshot_proof_*` |
| `/api/auth/enroll/*`, `/api/auth/login/*` | `account` / `enroll_*`, `login_*` |
| `/api/auth/cli/register` | `account` / `register` |
| `/api/developer/register`, `/api/developer/status` | `account` / `register`, `status` |
| `/api/developer/api-keys` (and keys/*) | `account` / `key_*` |
| `/api/developer/payments/*`, `/api/auth/payments/*` | `account` / `payment_*` |

## Auth

| Scheme | Where |
|---|---|
| `accountBearer` | `account` (enrollment / session bearer) |
| `apiKey` | `account` key ops; today also `manage`/`create` and `push`/`file` (open Q) |
| `lastDbNode` | fetch / push / manage |
| `githubOidc` | `maintenance` snapshot-proof |

`X-LastDB-Cloud-Protocol: 3` on **fetch / push / manage / maintenance**.
**Not** on **account**.

## Errors

```
{ "code": "<snake_case>", "field": "<static.name>" }
```

## CI plan

(a) Spec ↔ dispatch. (b) Types from tagged envelopes. (c) Next cloud vs last
stable released node (#1799). No unit tests.

## Where the node still names a resource

| # | Issue | Recommendation |
|---|---|---|
| R1 | Presigned `upload_url` may reveal host | Opaque string |
| R2 | Node still builds `BlobPath` today | Opaque `file_handle` |
| R3 | Snapshot multipart layout | Opaque `part_handle` |
| R4 | Range size ↔ storage limit | Optional `max_range_bytes` |

## Open questions for Tom

1. **Does `manage`/`create` and `push`/`file` still need `em_` `X-API-Key` on
   top of `LastDBNode`?** Code today always sends both for create
   (`log_sync/http.rs`) and blob upload (`SignedBlobClient`). The node token
   already binds path + body + account scope.
   **Recommend:** drop the `X-API-Key` requirement once `LastDBNode` is proven
   to carry account binding for billing/abuse checks; until then keep dual-auth
   as today. Prefer one credential for product storage calls.
