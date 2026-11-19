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

Fully typed args/results are in **Intent reference** below and in
`cloud-api.openapi.json` (`additionalProperties: false`, intent
discriminators, staged snapshot/file).

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

## Errors (closed set)

HTTP status → `code` (body always `{ "code", "field" }` + optional `minimum_version`):

| HTTP | code | Typical `field` |
|---|---|---|
| 401 | `unauthorized` | `request.signature`, `request.issued_at`, `request.nonce`, `device.id`, `device.revoked`, `account.node_key` |
| 403 | `forbidden` | `grant.absent`, `grant.revoked`, `database.role` |
| 403 | `payment_required` | `payment.subscription` |
| 400 | `invalid_request` | `grants.any.role`, intent-specific arg paths |
| 400 | `batch_too_large` | `batch.response` |
| 404 | `not_found` | `database.id`, `restore_point_id`, `file_handle` |
| 409 | `conflict` | `mutation_log.conflict` |
| 409 | `predecessor_conflict` | `mutation_log.cursor` |
| 409 | `cursor_expired` | `mutation_log.cursor` |
| 426 | `upgrade_required` | `request.protocol` (+ `minimum_version`) |
| 429 | `rate_limited` | `account.register` |

Sources: `ProtocolError` / `ErrorCode` in `cloud_backup_protocol/src/lib.rs`; node maps in `log_sync/wire.rs`.

## Intent reference

Every intent below is a closed typed envelope. Product intents carry
`database_id` in the envelope. Full JSON Schema: `cloud-api.openapi.json`.

Opaque strings: `cursor` ≤256, `file_handle` ≤512, `part_handle` ≤256,
`ticket` ≤256, `upload_url`/`payment_url`/`download_url` ≤8192.

Byte payloads: mutation `payload_b64` = **base64url** (no pad); file
`ciphertext_b64` = **standard base64** (delivery/object today). Range max
**180 KiB** (`OBJECT_RANGE`). Control JSON ≤ **256 KiB**; batch ≤ **90**;
response ≤ **8 MiB**.

### `fetch` / `mutations_since`

Purpose: page mutation log (publisher also sees proposal stream).
Who: member with read+; publisher sees `stream: proposal` entries too.
Source: `mutation-log/list`, `mutation-log/inbox/list` (`log_sync/wire.rs`).

| args | type | req |
|---|---|---|
| `cursor` | opaque string ≤256 | no |
| `limit` | int 1…90 | no |

| result | type | req |
|---|---|---|
| `cursor` | opaque string | yes |
| `mutations[]` | see OpenAPI `MutationEntryOut` | yes |

Errors: `cursor_expired`, `predecessor_conflict`, `forbidden`, `unauthorized`.

```json
{ "database_id": "…", "intent": "mutations_since", "args": { "limit": 50 } }
→ { "database_id": "…", "intent": "mutations_since", "result": { "cursor": "c-next", "mutations": [] } }
```

### `fetch` / `snapshot` (`stage: describe`)

Purpose: describe a restore point (or latest).
Who: member/publisher/public-read under `grant#any`.
Source: `snapshot/bytes` action `read` (`log_sync/snapshot/`).

| args | type | req |
|---|---|---|
| `stage` | `"describe"` | yes |
| `restore_point_id` | 64 hex | no |

| result | type | req |
|---|---|---|
| `stage` | `"describe"` | yes |
| `restore_point_id` | 64 hex | no |
| `through` | uint | no |
| `file_count` | uint | no |
| `name` | string ≤256 | no |

### `fetch` / `snapshot` (`stage: part`)

Purpose: get a presigned download for one snapshot part page.
Who: same as describe.
Source: `snapshot/bytes` action `download`.

| args | type | req |
|---|---|---|
| `stage` | `"part"` | yes |
| `restore_point_id` | 64 hex | yes |
| `part_handle` | opaque ≤256 | yes |

| result | type | req |
|---|---|---|
| `stage` | `"part"` | yes |
| `download_url` | uri ≤8192 | yes |
| `headers` | string map ≤32 keys | yes |

### `fetch` / `file`

Purpose: read a ciphertext byte range of a file blob.
Who: member/publisher/public-read.
Source: `mutation-log/delivery/object` (`log_sync/restore.rs`).

| args | type | req |
|---|---|---|
| `file_handle` | opaque ≤512 | yes |
| `offset` | uint | yes |
| `length` | uint 1…184320 | yes |

| result | type | req |
|---|---|---|
| `total_bytes` | uint ≥1 | yes |
| `offset` | uint | yes |
| `length` | uint 1…184320 | yes |
| `ciphertext_b64` | standard base64 | yes |

### `push` / `mutations`

Purpose: append mutations (land direct or proposal from grant).
Who: publisher (always direct); member with writer→direct or submit→proposal.
Source: `mutation-log/append` (`EntryWire` / `AppendResponse`).

| args | type | req |
|---|---|---|
| `entries[]` | `MutationEntryIn` 1…90 | yes |

Writer device = `X-LastDB-Device-Id`.

| result | type | req |
|---|---|---|
| `landed` | `direct`\|`proposal` | yes |
| `cursor` | opaque | yes |
| `accepted[]` | uint delivery_seq | yes |
| `next_writer_seq` | uint ≥1 | yes |
| `duplicate` | bool | no |

### `push` / `snapshot` (`stage: prepare`)

Purpose: obtain PUT URLs for snapshot files (≤32 per call).
Who: publisher.
Source: `snapshot/bytes` action `prepare`. Client PUTs to `upload_url` (not this API).

### `push` / `snapshot` (`stage: publish`)

Purpose: commit Manifest; returns `restore_point_id` (= snapshot digest).
Who: publisher.
Source: `snapshot/bytes` action `publish`.

### `push` / `file` (`stage: prepare` \| `complete`)

Purpose: prepare upload grant or verify completed upload.
Who: member/publisher with write.
Source: `mutation-log/blob/bytes` (`signed_blob.rs`). Binary PUT uses `upload_url`.

### `manage` / `create`

Purpose: create cloud database; `database_id` is node-chosen (envelope).
Who: registered paid account (publisher).
Source: `mutation-log/create-database` (`kind`, optional `spec`).

| args | type | req |
|---|---|---|
| `kind` | `personal`\|`shareable` | yes |
| `name` | string ≤128 | no (needed for shareable today) |
| `schema_version` | uint | no |

| result | type | req |
|---|---|---|
| `state` | `"native"` | yes |
| `public_read` | bool | yes (false) |

### `manage` / `grant`

Purpose: set member grants and/or public read (`grantee: "any"`).
Who: publisher.
Source: `share/members/set` (`MemberEntry`); v3 adds `any`.

| args | type | req |
|---|---|---|
| `grants[]` | `{ grantee: hex\|"any", role: writer\|submit\|reader }` 1…98 | yes |

`any` + non-`reader` → `invalid_request` / `grants.any.role`.

| result | type | req |
|---|---|---|
| `ok` | bool | yes |

### `manage` / `revoke`

Purpose: revoke members or turn public read off (`any`).
Who: publisher.

| args | type | req |
|---|---|---|
| `grantees[]` | hex \| `"any"` 1…98 | yes |

| result | type | req |
|---|---|---|
| `ok` | bool | yes |

### `manage` / `access`

Purpose: caller's relationship to the database (+ publisher public flag).
Who: any node that can authenticate; result may be `role: none`.
Source: folds `mutation-log/access` (liveness) + `share/members/read` (+ `grant#any`).

| args | type | req |
|---|---|---|
| *(none)* | | |

| result | type | req |
|---|---|---|
| `role` | `publisher`\|`member`\|`public`\|`none` | yes |
| `level` | `read`\|`submit`\|`write` | yes |
| `public` | bool | yes (publisher: `grant#any` active; others false) |
| `expires_at_ms` | uint | no |

```json
{ "database_id": "…", "intent": "access", "args": {} }
→ { "database_id": "…", "intent": "access", "result": { "role": "publisher", "level": "write", "public": true } }
```

### `manage` / `delete`

Purpose: publisher reclaim of all cloud data for the database.
Who: publisher.

| args | | |
|---|---|---|
| *(none)* | | |

| result | type | req |
|---|---|---|
| `status` | `done`\|`accepted` | yes |

### `account` / `register`

Purpose: first device of a new account.
Who: unsigned-as-account yet; node key must be new.
Source: `/api/auth/cli/register`.

| result | type | req |
|---|---|---|
| `ok` | bool | yes |
| `account_id` | 32 hex | yes |

Errors: `429` `rate_limited` / `account.register`.

### `account` / `payment`

Purpose: short-lived signed browser payment link.
Who: registered device.
Source: replaces checkout/portal URL fetch.

| result | type | req |
|---|---|---|
| `payment_url` | uri ≤8192 | yes |
| `expires_at_ms` | uint | yes |

### `account` / `deactivate`

| result | `ok`, `status` (`done`\|`accepted`) |

### `account` / `add_device`

Purpose: enroll header device for existing account (no body proof).
Who: holder of existing account node key; device may be unregistered.

| result | `ok`, `device_id` (header echo) |

### `account` / `remove_device`

| args | `device_id` (uuid) required |
| result | `ok` |

### `account` / `databases`

Purpose: page owned/granted databases for sync discovery.
Who: registered device. **ExeMem must add reverse index** (none today).

| args | `cursor?`, `limit?` 1…90 |
| result | `cursor?`, `databases[]` `{ database_id, role }` |

### `account` / `status`

| result | `paid`, `storage_used_bytes`, `devices[]` `{ device_id, added_at_ms? }` |

`paid` from payments/status today; storage/devices list not on wire yet.

### `maintenance` / `snapshot_proof_*` (DEV, `githubOidc`)

Source: `scripts/cloud_backup/snapshot_attempt_account.py`.

| intent | args | result |
|---|---|---|
| `snapshot_proof_provision` | `public_key`, `device_id`, `database_id_version` 1\|2 | `account_id`, `account_generation`, `database_id` |
| `snapshot_proof_status` | `run_id`, `run_attempt` | `phase`, `account_id`, `account_generation`, `database_id`, `retry_after_ms`, `released_bytes` |
| `snapshot_proof_retire` | same as status | same as status |
| `snapshot_proof_inspect` | `through` 1…32 | `database_id`, `entries` map, `facts` map |

## Judgment calls (for Tom)

1. **`account_id` format:** typed as **32 hex** (`user_hash` today), not UUID.
2. **`manage`/`create` args:** kept `kind` + optional `name`/`schema_version` from create-database `spec` (not empty `{}`).
3. **`manage`/`access`:** dropped `mutation_id` as Tom asked. Sole caller today is `log_sync/contributions.rs` `status()` for proposal polling (`submitted`/`published`/`discarded`/`unknown`). That status path is **not** in this surface — needs a later intent or publisher-only fetch if still required.
4. **`expires_at_ms` on access:** optional; **not on wire today** — included only because Tom listed it.
5. **Cursors:** opaque strings wrapping today’s decimal `delivery_seq` (not exposing decimal in the public schema).
6. **`file_handle` / `part_handle` / `ticket`:** opaque; lengths 512/256/256 chosen from BlobPath / grant sizes.
7. **Snapshot describe result:** flattened (`through`, `file_count`, `name`) instead of returning full `Manifest.files[]` (can be large); full file list still available via part/prepare flows.
8. **Push snapshot stages:** only `prepare` + `publish` on this API; binary PUT stays off-API to presigned URLs (same as today).
9. **Push file stages:** `prepare` + `complete` (today’s actions); no `upload` stage on this API.
10. **Fetch ciphertext encoding:** kept **standard base64** to match `delivery/object` (not base64url).
11. **Grant roles:** `writer` \| `submit` \| `reader` (maps today’s Submit/Read + direct-write).
12. **`storage_used_bytes` / device `added_at_ms`:** required/optional fields for `status` even though ExeMem does not expose them yet (Decision 27).
13. **`duplicate` on push/mutations:** optional bool from today’s `result: duplicate`.
14. **Inspect `entries`/`facts`:** open string maps with maxProperties (only place maps appear), matching DEV broker bags.
15. **Public-read fetch auth:** unregistered/unpaid may read under `grant#any` left as open question (recommend yes).

## Open questions for Tom

1. Under `grant#any`, may an **unregistered** and/or **unpaid** node key
   `fetch` (reads only)? **Recommend yes** — valid `nodeSignature` with their
   own key is enough when `grant#any` is active; paid/register gates stay on
   push/manage/account. Confirm or restrict.

