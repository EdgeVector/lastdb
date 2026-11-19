# Cloud API (ExeMem): one contract

Status: design for review (2026-11-19). This is not the live contract yet.
When this document and `docs/designs/cloud-api.openapi.json` differ, the
OpenAPI file wins for paths, auth, and error shapes.

Tom (2026-11-19): action-based interface. The node Ed25519 key is the
identity. Every node request is signed. Cloud storage may change without
LastDB knowing.

## Why

Today the cloud surface is a hand-dispatched tree under
`POST /api/cloud/backups/v2/…` with no route spec and no real wire version.
The pin lint never checks the next cloud against the last stable node (#1799).

## Sources (verified)

| Source | Role |
|---|---|
| `exemem-infra` `storage_service` | Dispatch |
| `cloud_backup_protocol` `NodeReadToken` | Today’s signed POST proof |
| `lastdb_node` `log_sync/`, `named_grant/`, `cloud_backup_client/`, `cloud/enrollment.rs` | Call sites |
| `scripts/cloud_backup/snapshot_attempt_account.py` | lastdb DEV snapshot-proof |
| `app_cmds/publish_credentials.rs` | Paid-account check on app publish |

## Hard rules

- No dual paths, aliases, fallback, or migration.
- No unit tests. E2E DEV checks in CI are fine.
- Share, Deliver, Org exist only in People.
- No cloud storage nouns in routes or request fields.
- Auth and identity stay **out of the body envelope** (headers + signature).

## Shape

```
POST /api/cloud/v3/fetch
POST /api/cloud/v3/push
POST /api/cloud/v3/manage
POST /api/cloud/v3/account
POST /api/cloud/v3/maintenance
```

### Body envelope (Decision 16)

**Product** (fetch / push / manage):

```json
{ "database_id": "<uuid>", "intent": "<enum>", "args": { } }
→ { "database_id": "<uuid>", "intent": "<same>", "result": { } }
```

**Account / maintenance** (no `database_id`):

```json
{ "intent": "<enum>", "args": { } }
→ { "intent": "<same>", "result": { } }
```

Errors: `{ "code", "field" }` (+ `minimum_version` on 426).
`manage`/`create`: the **node chooses** `database_id` (UUID), as today.

### Intents

| Action | Intents |
|---|---|
| `fetch` | `mutations_since`, `snapshot`, `file` |
| `push` | `mutations`, `snapshot`, `file` |
| `manage` | `create`, `grant`, `revoke`, `access`, `delete` |
| `account` | `register`, `payment`, `deactivate`, `add_device`, `remove_device`, `databases`, `status` |
| `maintenance` | `snapshot_proof_provision`, `snapshot_proof_retire`, `snapshot_proof_status`, `snapshot_proof_inspect` |

## Node identity and signing (Decisions 19–20)

### Headers on every **node** request

| Header | Format | Meaning |
|---|---|---|
| `X-LastDB-Node-Key` | 64 lowercase hex chars (Ed25519 public key) | Account identity. Same key that signs. |
| `X-LastDB-Device-Id` | UUID string (`uuid` crate / UUIDv4 text) | Which registered device sent this request. |
| `X-LastDB-Issued-At` | decimal unix milliseconds | Signature time. |
| `X-LastDB-Nonce` | 64 lowercase hex chars (32 random bytes) | One nonce per request. |
| `X-LastDB-Signature` | base64url (no pad), 64 raw signature bytes | Ed25519 over the signed material below. |
| `X-LastDB-Cloud-Protocol` | integer `3` | On fetch/push/manage/maintenance only. **Not** on account. |

These stay out of the JSON envelope. They are covered by the signature so they
cannot be swapped.

**Why headers (not body):** Decision 16 already keeps auth out of the envelope;
today’s `NodeReadToken` already binds `node_public_key` outside the business
body (`guest_permission.rs` / `NodeReadToken`). Headers keep every intent’s
`args` free of identity fields and match Tom’s direction.

### What is signed

UTF-8 lines joined by `\n` (no trailing newline after the last line):

```
POST
<request path including /api>
<protocol or ->
<node key hex>
<device id>
<issued-at ms decimal>
<nonce hex>
<body sha256 hex>
```

- Path example: `/api/cloud/v3/fetch`
- Protocol: `3` when the protocol header is required; literal `-` for `account`
- Body hash: SHA-256 of the raw request body bytes, lowercase hex (64 chars)
- Signature: Ed25519 over the **UTF-8 bytes of that string** (same pattern as
  today’s token digest signing)

Clock skew: ±5 minutes (`SHARE_REQUEST_SKEW_MS` today). Nonces are
single-use per (node key, nonce) in a short window.

### Security scheme

OpenAPI scheme `nodeSignature` replaces `LastDBNode` bearer, `accountBearer`,
and `em_` `X-API-Key` for all node traffic. **Maintenance** (CI) keeps
`githubOidc` only — it is not a node.

### One node key for every device (Decision 21)

**No per-device keys.** Every registered device of an account signs with the
**same** node Ed25519 private key. `X-LastDB-Device-Id` is identification and
bookkeeping (which machine spoke; list/remove devices) — **not** a security
boundary. ExeMem authenticates the node key, not a per-device credential.

**Accepted risk (Tom):** a stolen device that holds the node private key has
full account access (“If the laptop is stolen, then basically anything can be
accessible”). Removing the device id after theft stops that machine from being
listed as active; it does **not** revoke the key. There is **no node-key
rotation** (Decision 28). A compromised node key means a **new account**.

### Device registration vs request device

- `register` — first device of a **new** account; header `X-LastDB-Device-Id`
  **is** that device. Rate-limited (Decision 24). Node key must not already
  own an account.
- `add_device` — enroll another device of an **existing** account. The caller
  already holds the **same** node private key (from the recovery phrase or an
  existing device). **No `proof` body field** — the Ed25519 signature over the
  request is the proof. Header `X-LastDB-Device-Id` **is the new device**.
  ExeMem accepts this when the node key is already registered, even if the
  header device id is not yet registered (Decision 29 exception to the usual
  unknown-device 401). Args are `{}`.
- `remove_device` — header device must already be registered; `args.device_id`
  is removed from the bookkeeping list.
- All **other** intents — header device must be registered for that node key.

ExeMem rejects:

| Case | HTTP | Error |
|---|---|---|
| Bad signature | 401 | `{ "code":"unauthorized", "field":"request.signature" }` |
| Expired / skewed `Issued-At` | 401 | `{ "code":"unauthorized", "field":"request.issued_at" }` |
| Reused nonce | 401 | `{ "code":"unauthorized", "field":"request.nonce" }` |
| Unknown / never registered device (except `add_device`) | 401 | `{ "code":"unauthorized", "field":"device.id" }` |
| Removed device | 401 | `{ "code":"unauthorized", "field":"device.revoked" }` |
| `add_device` when node key is not a known account | 401 | `{ "code":"unauthorized", "field":"account.node_key" }` |
| `register` rate limit | 429 | `{ "code":"rate_limited", "field":"account.register" }` |

Members/grants remain keyed by **node key** (not device).

### Example (fetch)

```http
POST /api/cloud/v3/fetch HTTP/1.1
X-LastDB-Cloud-Protocol: 3
X-LastDB-Node-Key: <64 hex>
X-LastDB-Device-Id: 11111111-1111-4111-8111-111111111111
X-LastDB-Issued-At: 1730000000000
X-LastDB-Nonce: <64 hex>
X-LastDB-Signature: <base64url>
Content-Type: application/json

{
  "database_id": "22222222-2222-4222-8222-222222222222",
  "intent": "mutations_since",
  "args": { "cursor": "opaque", "limit": 50 }
}
```

## Batching (Decision 17)

Each action accepts one envelope **or** an array of envelopes for that action.
Independent items; HTTP 200 for a processed batch. Max **90** items
(`APPEND_BATCH_LIMIT` / `LIST_PAGE_LIMIT`). Request ≤ 256 KiB
(`CONTROL_JSON_MAX_BYTES`). Response ≤ 8 MiB (`JSON_LIMIT`); overflow →
per-item `batch_too_large` on `batch.response`. One signature covers the whole
body (array or single).

## Decisions (Tom, 2026-11-19)

1. Protocol **v3**.
2. Version in **header + token/signature material**; no body `version`.
3. **426** `upgrade_required` + `minimum_version`.
4. Signed `grant#<node key>` — **no list, no revision**.
5. **No** protocol header on **account** (was auth-service).
6. Live OpenAPI under **`cloud_backup_protocol/`**.
7. DEV snapshot-proof under **`maintenance`**, marked
   **`x-lastdb-environment: dev`**.
8. Action-based surface: `fetch` / `push` / `manage` / `account` /
   `maintenance`.
9. `push`/`mutations`: no `kind`; `landed` from grant.
10. `fetch`/`mutations_since`: publisher sees direct+proposal; receivers direct
    only; accept = publisher push; entries tagged `stream`.
11. **Silent rejections** — no discard call; node’s `Discarded` settle goes away.
12. Access under **manage** (`manage`/`access`). No share-settings / `rules`
    surface (Decision 30).
13. Cleanup / retention **ExeMem-internal** — no public ops.
14. Ingested proposals deletable when publisher fetch cursor advances.
15. **`manage`/`delete`** — publisher reclaim of all cloud data.
16. Standard body envelope `{database_id?, intent, args|result}`.
17. **Batching** — single or array; caps above.
18. **`account`**: `register`, `payment`, `deactivate`, `add_device`,
    `remove_device`, `databases`, `status`. **No login.** Node key is
    identity. **No developer accounts / no `em_` API keys.** Auth-service
    enroll/login/passkey routes are **removed from this API surface** (not
    called by lastdb_node; if People still needs them they are People-only).
19. Every node request carries **node public key** and **device id** in signed
    headers.
20. Every node request is **Ed25519-signed** with the node private key under
    one scheme (`nodeSignature` above). Replaces bearer and API-key auth for
    node traffic.
21. **No per-device keys.** All of a node’s devices sign with the node private
    key. Device id is identification and bookkeeping, not a security boundary.
    **Accepted risk:** a stolen device with the node key has full account
    access.
22. **Publishing to the app registry requires a paid ExeMem account**
    (`PAID_ACCOUNT_REQUIRED` on registry publish today).
23. **`payment`** returns a short-lived, single-use, **signed link** that opens
    the hosted payment page in a browser (the browser does not hold the node
    key). No Stripe ops / customer-session / portal pass-through in `args`.
24. **`register` is rate-limited** (keys cost nothing to mint; paid features
    unlock only after payment succeeds). Over limit → HTTP **429**
    `{ "code":"rate_limited", "field":"account.register" }`.
25. **Accepted risk:** with no login, losing **all** devices **and** the
    24-word recovery phrase means losing the account. **No reset.**
26. **`account`/`databases`**: paginated list (opaque cursor) of databases this
    node key owns or has been granted, each with `database_id` and the node’s
    role (`publisher`, or member `writer`/`submit`/`reader`). Used by a new or
    recovered device to learn what to sync.
27. **`account`/`status`**: payment/plan state (`paid`), cloud storage used,
    and registered devices. `deactivate` semantics unchanged.
28. **No node-key rotation.** A compromised node key is remedied only by a
    **new account**.
29. **Connect codes, invites, and joins are People-app only** for now. The
    cloud API has **no** invite, claim-slot, connect-code, SPAKE2, or
    rendezvous surface. `add_device` needs no body `proof`: a valid node-key
    signature for an existing account is enough, and the header device id is
    the new device (unknown-device 401 does not apply to this intent).
30. **No `rules` / share-settings surface.** `manage`/`create` and
    `manage`/`grant` take no settings body. `…/share/members/settings/*` is
    **removed**. **Defaults with no `any` grant:** read is **members only**.
    **Write:** decided per member by their signed grant — a grant that allows
    direct writes lands as `direct`; a submit-level grant lands as `proposal`
    (matches Decision 9 / push `landed`). The **publisher always writes
    direct**.
31. **Public read is a grant, not a setting.** `manage`/`grant` accepts
    grantee **`any`** (reserved value in place of a node key) at **`reader`
    only**. `any` can never get write or submit →
    `{ "code":"invalid_request", "field":"grants.any.role" }`.
    `manage`/`revoke` with `any` turns public read off. ExeMem stores a
    publisher-signed **`grant#any`** entry like other grants.
    **`manage`/`access`** reports public state to the publisher
    (`public_read: true|false`).
    **Fetch under `any`:** the reader still sends `nodeSignature` with **their
    own** node key (and device headers). ExeMem allows **read** intents
    (`fetch`/*) when `grant#any` is active, even if that node key has no
    per-key grant. Push/manage still require a real membership grant.
    Whether an **unregistered or unpaid** node key may read under `any` is
    an open question (recommend **yes**, reads only).

## Account

| intent | args | result |
|---|---|---|
| `register` | `{}` (device = header) | `ok`, `account_id` |
| `payment` | `{}` | `payment_url` (signed, single-use), `expires_at_ms` |
| `deactivate` | `{}` | `ok`, `status` (`done`\|`accepted`) |
| `add_device` | `{}` (new device = header) | `ok`, `device_id` |
| `remove_device` | `device_id` | `ok` |
| `databases` | `cursor?`, `limit?` | `cursor`, `databases[]` (`database_id`, `role`) |
| `status` | `{}` | `paid`, `storage_used_bytes`, `devices[]` |

`register` may return `429` `rate_limited` / `account.register`. App-registry
publish and other paid features unlock only after payment succeeds via the
hosted page opened from `payment_url`.

### `databases` — role and ExeMem source

Each row: `{ "database_id": "<uuid>", "role": "publisher" | "writer" | "reader" }`.
`publisher` = this node key owns the database (create-time account key).
`writer` / `reader` = active member grant (maps today’s wire `Submit` /
`Read` on `ShareMemberRole`).

**Today:** ExeMem has **no** reverse index from node key → databases. Every
membership path needs a known `database_id`:

- Owned: `POST …/mutation-log/create-database` creates one id; no account-wide
  owned list.
- Granted: `…/share/members/{get,list,read}` are per-database (`grant#<node
  key>` inside one DB — Decision 4). Publisher-only `members/list` pages
  members of one DB, not DBs of one member.
- Local node discovery (`GET /api/databases`) is home-local only
  (`docs/app-api.md`: no cloud call lists every accessible database).

**ExeMem must add** an account-scoped reverse index (owned + granted rows) to
serve `account`/`databases`. Until then this intent cannot be implemented from
existing list APIs alone.

Pagination: opaque `cursor` (omit on first page); `limit` default/max aligned
with batch caps (max **90**). Response `cursor` is the next page token (omit
or empty when done).

### `status` — fields and ExeMem source

| Field | Meaning | Today |
|---|---|---|
| `paid` | boolean — active paid plan | Yes — auth-service `GET /api/auth/payments/status` (`status == "active"`, or unpaid gate `payment.subscription` / `PAID_ACCOUNT_REQUIRED`) |
| `storage_used_bytes` | uint — cloud bytes charged to this account | **Not exposed** — ExeMem must start reporting it; no account usage API on the wire today |
| storage limit | — | **None exposed today** — omit from the result (do not invent a limit) |
| `devices[]` | `{ device_id, added_at_ms? }` | Register exists (`POST /api/auth/cli/register`); **no device list** and **no `added_at`** on the wire today. ExeMem must list registered devices; include `added_at_ms` when stored |

Second / recovered device: derive the **same** node key from the 24-word
recovery phrase (local identity; not a cloud API), sign `add_device` with it,
and put the new device UUID in `X-LastDB-Device-Id`. Then call `databases`
(sync) and `status`. Device-to-device pairing without the phrase is a People
app concern (Decision 29), not this API.

## Maintenance

DEV snapshot-proof only (`githubOidc`). Considered and rejected: local
`/health`/`/api/version` (UDS), reclaim/retention (ExeMem-internal).

## Product intent args (summary)

| Intent | args → result |
|---|---|
| `fetch`/`mutations_since` | `cursor?`, `limit?` → `cursor`, `mutations[]` (`stream`, …) |
| `fetch`/`snapshot` | `restore_point_id?`, `part_handle?` → describe or part |
| `fetch`/`file` | `file_handle`, `offset`, `length` → bytes |
| `push`/`mutations` | `entries[]` (writer device = header) → `landed`, `cursor`, `accepted` |
| `push`/`snapshot` | `stage`, `ticket?`, … → ticket / `upload_url?` |
| `push`/`file` | `stage`, `ticket?`, … → `file_handle?` |
| `manage`/`create` | `{}` → `access?` (`public_read`, …) |
| `manage`/`grant` | `grants[]` (`grantee` = node-key hex \| `any`, `role`) → `ok` |
| `manage`/`revoke` | `grantees[]` (hex \| `any`) → `ok` |
| `manage`/`access` | `mutation_id?` → `state`, `public_read`, … |
| `manage`/`delete` | `{}` → `status` |

## Use cases

| Use case | Intent |
|---|---|
| Create / delete database | `manage`/`create`, `manage`/`delete` |
| Append / propose / accept | `push`/`mutations` |
| Read mutations (+ publisher proposals) | `fetch`/`mutations_since` |
| Ignore proposal | silent; ExeMem deletes after fetch cursor advances |
| File / snapshot | `push`/`file`, `fetch`/`file`, `push`/`snapshot`, `fetch`/`snapshot` |
| Grant / revoke / access (incl. public read `any`) | `manage`/`grant`, `revoke`, `access` |
| Register account / devices / pay / deactivate | `account`/`register`, `add_device`, `remove_device`, `payment`, `deactivate` |
| New/recovered device: find DBs to sync | `account`/`databases` |
| Account paid state, storage used, devices | `account`/`status` |
| DEV snapshot-proof | `maintenance`/* |
| Reclaim/retention jobs | ExeMem-internal |

## Old → new

| Old | New |
|---|---|
| `…/mutation-log/create-database` | `manage`/`create` |
| *(no delete)* | `manage`/`delete` |
| `…/mutation-log/access` | `manage`/`access` |
| `…/mutation-log/append` | `push`/`mutations` |
| `…/mutation-log/head` / `list` | `fetch`/`mutations_since` |
| `…/inbox/list` | `fetch`/`mutations_since` (publisher) |
| `…/inbox/settle` (accept) | publisher `push`/`mutations` |
| `…/inbox/settle` (discard) | **removed** (silent) |
| `…/settle-submissions` | **removed** |
| `…/blob/bytes` | `push`/`file` |
| `…/delivery/object` | `fetch`/`file` |
| `…/snapshot/bytes` prepare/publish | `push`/`snapshot` |
| `…/snapshot/bytes` read*/download | `fetch`/`snapshot` |
| `…/snapshot/bytes` retention/reclaim/`*_dev` | ExeMem-internal |
| `…/share/members/set` | `manage`/`grant` / `revoke` |
| `…/share/members/get` / `list` | **removed** |
| `…/share/members/read` | `manage`/`access` (includes `public_read`) |
| `…/share/members/settings/*` | **removed** (Decision 30; no settings intent) |
| `ShareSettings.read: anyone` | `manage`/`grant` with `grantee: "any"`, `role: "reader"` (`grant#any`) |
| `lastdb-read-v1.` share links | Link still carries the **content key** (People delivers it). Cloud read auth is `grant#any` + the reader’s `nodeSignature`, not the old settings flag. |
| `…/dev-snapshot-proof/{op}` | `maintenance` / `snapshot_proof_*` |
| `/api/auth/enroll/*`, `/api/auth/login/*` | **removed** from this API |
| `/api/auth/cli/register` | `account`/`register` |
| `/api/developer/*` (register/status/keys) | **removed** (no dev accounts) |
| `/api/auth/payments/*`, `/api/developer/payments/*` | `account`/`payment` |
| `Authorization: LastDBNode …` bearer blob | `nodeSignature` headers |
| `X-API-Key` / `em_` keys | **removed** |

## Auth schemes (OpenAPI)

| Scheme | Where |
|---|---|
| `nodeSignature` | fetch, push, manage, account |
| `githubOidc` | maintenance only |

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

1. Under `grant#any`, may an **unregistered** and/or **unpaid** node key
   `fetch` (reads only)? **Recommend yes** — valid `nodeSignature` with their
   own key is enough when `grant#any` is active; paid/register gates stay on
   push/manage/account. Confirm or restrict.
