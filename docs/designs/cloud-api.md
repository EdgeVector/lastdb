# Cloud API (ExeMem): one contract

Status: design for review (2026-11-19). This is not the live contract yet.
When this document and `docs/designs/cloud-api.openapi.json` differ, the
OpenAPI file wins for paths, auth, and error shapes.

Tom (2026-11-19): action-based interface. The node Ed25519 key is the
identity. Every node request is signed. The cloud stays blind to database
contents. No dual paths, aliases, fallback, or migration. No unit tests.

## Shape

```
POST /api/cloud/v3/fetch
POST /api/cloud/v3/push
POST /api/cloud/v3/manage
POST /api/cloud/v3/account
POST /api/cloud/v3/maintenance
```

### Envelope

**Product** (fetch / push / manage):

```json
{ "database_id": "<64 hex>", "intent": "<enum>", "args": { } }
→ { "database_id": "<64 hex>", "intent": "<same>", "result": { } }
```

**Account / maintenance** (no `database_id`):

```json
{ "intent": "<enum>", "args": { } }
→ { "intent": "<same>", "result": { } }
```

`database_id` is the database’s Ed25519 **public key**, 64 lowercase hex
(Decision 36). The node chooses it at create by derivation from the node seed
and a random `db_salt`.

Body is one envelope or an array of **1–90**. Request ≤ **256 KiB**, response
≤ **8 MiB**. Items run in array order and independently. A processed batch is
HTTP **200**. A failed item is
`{ "database_id"?, "intent", "error": { "code", "field" } }` in its slot.
Response overflow is per-item `batch_too_large` / `batch.response`. Auth,
version, and size failures reject the **whole** request.

Whole-request error body: `{ "code", "field" }` (+ `minimum_version` on 426).

### Intents

| Action | Intents |
|---|---|
| `fetch` | `mutations_since`, `snapshot`, `file` |
| `push` | `mutations`, `file`, `snapshot` |
| `manage` | `create`, `grant`, `revoke`, `access`, `delete` |
| `account` | `register`, `payment`, `deactivate`, `add_device`, `remove_device`, `databases`, `status` |
| `maintenance` | `snapshot_proof_provision`, `snapshot_proof_status`, `snapshot_proof_retire`, `snapshot_proof_inspect` |

## Auth (`nodeSignature`)

Headers on every **node** request:

| Header | Format |
|---|---|
| `X-LastDB-Node-Key` | 64 lowercase hex (Ed25519 public key) |
| `X-LastDB-Device-Id` | UUID |
| `X-LastDB-Issued-At` | decimal unix milliseconds |
| `X-LastDB-Nonce` | 64 lowercase hex |
| `X-LastDB-Signature` | base64url, no pad (64 signature bytes) |
| `X-LastDB-Cloud-Protocol` | `3` — on fetch/push/manage/maintenance; **not** on account |

Signed material (UTF-8 lines, `\n`, no trailing newline after the last line):

```
POST
<path>
<protocol or ->
<node key hex>
<device id>
<issued-at ms>
<nonce hex>
<body sha256 hex>
```

Protocol slot is `3` when the protocol header is required, else `-`. Body hash
is SHA-256 of the raw body, lowercase hex. One signature covers the whole
batch body.

Clock skew: **±60 seconds** (`SHARE_REQUEST_SKEW_MS` today). Nonces are
single-use per node key and remembered for at least **2×** the skew window.

**Device rule:**

- If the signing key **has an account**, the device id must be registered and
  not removed. Exception: `add_device` (header device is the new one).
- If the signing key **has no account** (unregistered member or public
  reader), the device check is skipped. The device id is only a writer label.

**Maintenance** uses `githubOidc` only — no node headers.

## Database keys (Decision 36)

- `db_salt` = 32 random bytes (64 hex), chosen by the node at create.
- `db_seed = HKDF-SHA256(ikm = node Ed25519 private seed, salt = db_salt,
  info = "lastdb-db-key-v1")`.
- Ed25519 keypair from `db_seed`. `database_id` = db public key (64 hex).
- `manage`/`create` sends `db_salt` and `birth_signature`: Ed25519 by the
  **db key** over
  `lastdb-db-birth-v1\n<database_id>\n<owner node key>\n<db_salt>`.
- ExeMem verifies the birth signature against `database_id`, checks that the
  owner node key is the request signer, and stores owner, salt, and signature.
- The db key signs **nothing else**. After create, the owner’s **node-key**
  request signature authorizes every change. No per-grant signatures.
- Members and public readers get `owner`, `db_salt`, `birth_signature` from
  `manage`/`access` and verify: signature against `database_id`, and `owner`
  matches the publisher People told them about.
- New device: phrase → `add_device` → `databases` (returns `db_salt` for owned
  rows) → re-derive and check `database_id`.
- **No rotation.** A compromised node key exposes every derived db key
  (accepted risk). No per-device keys.
- **Tombstones:** ExeMem keeps id-only tombstones. `create` on a known or
  deleted id → `409` `conflict` / `database.id`. Other intents on a deleted
  id → `404` `not_found` / `database.deleted`.
- Deactivate retires the node key and tombstones every owned database.
- The cloud stays blind to contents (name lives in the node-encrypted
  manifest; no `kind` / schema fields on create).

## Requirements (Decision 38)

The **publisher pays** for every byte stored and served on its databases,
including member uploads and public reads. Members never register or pay.

| Intent | Account | Payment (whose) | Role |
|---|---|---|---|
| `fetch`/* | no | publisher’s | read+ or `any` |
| `push`/`mutations` | no | publisher’s | submit (proposal); write or publisher (direct) |
| `push`/`file` | no | publisher’s | write or publisher |
| `push`/`snapshot` | yes | publisher’s | publisher |
| `manage`/`create` | yes | caller’s | — |
| `manage`/`grant`, `revoke` | yes | publisher’s | publisher |
| `manage`/`access` | no | none | anyone |
| `manage`/`delete` | yes | none (always allowed) | publisher |
| `account`/`register` | no (creates one) | none | — |
| `account`/`databases` | no | none | — |
| `account`/* other | yes | none | — |

**Lapsed publisher:** reads keep working. `push`, `grant`, `create`, and
`snapshot` return `payment_required` for everyone on that publisher’s
databases. `delete` always works.

## Decisions (Tom, 2026-11-19)

1. Protocol **v3**.
2. Version in **header + signature material**; no body `version`.
3. **426** `upgrade_required` + `minimum_version` (not on account).
4. Grants are plain `{grantee, role}`, authorized by the publisher’s
   **request signature**. No per-grant signature, sequence, or `signed_at`.
5. **No** protocol header on **account**.
6. Live OpenAPI under **`cloud_backup_protocol/`**.
7. DEV snapshot-proof under **`maintenance`**, `x-lastdb-environment: dev`.
8. Actions: `fetch` / `push` / `manage` / `account` / `maintenance`.
9. `push`/`mutations`: no `kind`; `landed` from grant (`direct` /
   `proposal`). Publisher always `direct`.
10. `fetch`/`mutations_since`: publisher sees direct+proposal; receivers
    direct only; accept = publisher push with `incorporated_from`; entries
    tagged `stream`.
11. **Silent rejections** — no discard call.
12. Access under **manage**; no share-settings / `rules` surface.
13. Cleanup / retention **ExeMem-internal** — no public ops.
14. Ingested proposals deletable when publisher fetch cursor advances past
    them (Decision 41).
15. **`manage`/`delete`** — publisher reclaim; immediate tombstone.
16. Standard body envelope `{database_id?, intent, args|result}`.
17. **Batching** — single or array; caps above; per-item errors.
18. **`account`**: `register`, `payment`, `deactivate`, `add_device`,
    `remove_device`, `databases`, `status`. No login. No `em_` API keys.
19. Node public key + device id in signed headers.
20. Ed25519 `nodeSignature` for all node traffic.
21. **No per-device keys.** Device id is bookkeeping. Stolen device with the
    node key has full access (accepted risk).
22. App-registry publish requires a paid ExeMem account.
23. **`payment`** → short-lived single-use signed `payment_url`.
24. **`register` rate-limited** per IP → `429` `rate_limited` /
    `account.register`.
25. Lose all devices + recovery phrase → account gone. No reset.
26. **`account`/`databases`**: paginated owned/granted DBs for sync
    discovery; `db_salt` on owned rows.
27. **`account`/`status`**: `paid`, `storage_used_bytes`,
    `transfer_used_bytes`, devices.
28. **No node-key rotation.** Compromised key → new account only.
29. Connect codes / invites / joins are **People-app only**.
30. No `rules` / share-settings. Defaults: members-only read without `any`;
    write vs proposal from each member’s grant; publisher always direct.
31. Public read is **`grant#any`** at **`read` only**. Revoke `any` turns it
    off. No per-grant signatures (with Decision 4).
32. **Device rule** (§ Auth): account keys need a registered device;
    account-less keys skip the device check. Public fetch rate-limited
    `read.public` per IP+database; publisher pays.
33. **Upload `begin`/`commit`.** Node PUTs to storage between stages. File
    begin may omit `upload` when already stored. Snapshot begin pages files;
    commit names the encrypted manifest file. **Downloads:** one presigned
    GET model via `fetch`/`file` (180 KiB base64 ranges **removed**). No
    inline bytes. No storage-event confirmation.
34. **`manage`/`create`** carries only `db_salt` + `birth_signature` (no
    `kind`/`name`/`schema_version`). Cloud stays blind. Today’s node
    `CreateBody` in `log_sync/native.rs` must stop sending kind/spec.
35. Access levels everywhere: **`read` | `submit` | `write`**.
36. **Self-certifying derived `database_id`** (§ Database keys). Tombstones.
    No db-key rotation.
37. **Retention is ExeMem’s.** `advance_cutoff`, `previous`, `destination`
    (r2/b2), plaintext `name`, `upload_id`, and `through` leave the wire.
    `name` (local named-database name the node needs on restore) moves into
    the **encrypted** manifest. `advance_cutoff` was the client choosing
    server deletion — that stops.
38. **Publisher pays** for everything on its databases (§ Requirements).
    Lapsed publisher: reads work; push/grant/create/snapshot →
    `payment_required`; delete always allowed. `status` includes
    `transfer_used_bytes`.
39. **`deactivate`:** cancel subscriptions; tombstone every owned database
    (files, snapshots, issued grants); drop grants held and devices; retire
    the key forever → later calls `unauthorized` / `account.retired`.
40. **Devices:** `remove_device` is bookkeeping only (id never returns);
    cap **256** → `conflict` / `device.limit`.
41. **Cursors / proposals / restore:** opaque forward-only cursors; proposal
    deletion only after a later publisher cursor past it; `cursor_expired`
    recovery = latest snapshot → restore → resume from its cursor.
    Acceptance visible via `incorporated_from` (no status intent).

## Opaque strings and encodings

| Name | Max | Notes |
|---|---|---|
| `cursor`, `file_handle`, `snapshot_upload` | 256 | Opaque; no raw delivery_seq |
| `url` | 8192 | Presigned |
| `headers` | 32 entries, values ≤1024 | String map |
| `payload_b64` | ≤96 KiB decoded | base64url, no pad |
| `db_salt` | 64 hex | 32 bytes |
| `birth_signature` | base64url, 64 bytes | Db-key Ed25519 |
| `restore_point_id` | 64 hex | sha256 of manifest file |

## Intent reference

### `fetch` / `mutations_since`

Who: read/submit/write member, publisher, or anyone under `any`.
Errors: `cursor_expired`, `forbidden`, `not_found`.

| args | req |
|---|---|
| `cursor` opaque | no |
| `limit` 1…90 | no |

| result | req |
|---|---|
| `cursor` | yes |
| `mutations[]` ≤90 | yes |

Each mutation: `stream` (`direct`|`proposal`; proposals to publisher only),
`node_key`, `device_id`, `writer_seq`, `authored_at_ms`, `namespace`,
`payload_b64`, `file?`, `incorporated_from?`.

### `fetch` / `snapshot`

Who: same as mutations_since.
Errors: `not_found`, `forbidden`.
Every call places or extends a **1 h hold** on that restore point.

| args | req |
|---|---|
| `restore_point_id` 64 hex | no (omit = latest) |

| result | req |
|---|---|
| `restore_point_id` | yes |
| `manifest` (`file_handle`) | yes |
| `cursor` | yes |
| `held_until_ms` | yes |

### `fetch` / `file`

Who: same; shares public-read rate limit under `any`.
Errors: `not_found`, `forbidden`, `rate_limited`.

| args | req |
|---|---|
| `file_handle` | yes |

| result | req |
|---|---|
| `url` | yes |
| `headers` | yes |
| `expires_at_ms` | yes |
| `bytes` | yes |
| `sha256` | yes |

Client GETs `url` with `headers`. Same model for snapshot files.

### `push` / `mutations`

Who: write/publisher → `direct`; submit → `proposal`; read → `forbidden`.
Errors: `conflict`/`entries.writer_seq`, `forbidden`, `payment_required`,
`not_found`/`file_handle`.

| args | req |
|---|---|
| `entries[]` 1…90 | yes |

Entry: `writer_seq` ≥1, `authored_at_ms`, `namespace` ≤128, `payload_b64`,
`file?`, `incorporated_from?` (publisher only). File must already be
committed. Submit cannot attach files.

| result | req |
|---|---|
| `landed` | yes |
| `next_writer_seq` | yes |
| `duplicate` | no |

Dedupe by (node key, device, writer_seq).

### `push` / `file` (`begin` | `commit`)

Who: write or publisher.
Errors: `conflict`/`file.unverified`, `payment_required`, `forbidden`.

**begin** args: `bytes` ≥1, `sha256`. Result: `file_handle`, optional
`upload` `{url, headers, expires_at_ms}` (absent = already stored in this
database). No client-chosen handle.

**commit** args: `file_handle`. Result: `{}`. Idempotent.

### `push` / `snapshot` (`begin` | `commit`)

Who: publisher (account required).
Errors: `conflict`/`file.unverified`, `invalid_request`/`cursor`,
`not_found`/`snapshot_upload`, `payment_required`.

**begin** args: `snapshot_upload?`, `files[]` 1…32 `{bytes, sha256}`.
Result: `snapshot_upload`, `files[]` `{file_handle, upload?}`. Repeat to
add pages.

**commit** args: `snapshot_upload`, `manifest` (file_handle in this upload),
`cursor`. Result: `restore_point_id` = sha256(manifest file). Restore point
visible only after commit. Manifest is node-encrypted (paths, handles,
hashes, local name, personal flag).

### `manage` / `create`

Who: registered + paid.
Args: `db_salt`, `birth_signature`. Result: `{}`.
Errors: `conflict`/`database.id`, `invalid_request`/`birth_signature`,
`payment_required`.

### `manage` / `grant`

Who: publisher.
Args: `grants[]` 1…90 `{grantee, role}`. Result: `{}`.
`any` + non-`read` → `invalid_request`/`grants.any.role`. Upsert, idempotent.

### `manage` / `revoke`

Who: publisher.
Args: `grantees[]` 1…90. Result: `{}`. Revoking `any` turns public read off.

### `manage` / `access`

Who: any key (no account). Shares public-read rate limit.
Args: `{}`.

| result | req |
|---|---|
| `role` `publisher`\|`member`\|`public`\|`none` | yes |
| `level` `read`\|`submit`\|`write` | no (omit for `none`) |
| `public_read` | no (publisher only) |
| `owner` | yes |
| `db_salt` | yes |
| `birth_signature` | yes |

### `manage` / `delete`

Who: publisher. Always allowed (even if unpaid).
Args: `{}`. Result: `{}`. Immediate tombstone; ExeMem reclaims later.

### `account` / `register`

Args: `{}`. Result: `{}`. Rate-limited per IP.
Errors: `conflict`/`account.exists`, `unauthorized`/`account.retired`,
`rate_limited`.

### `account` / `payment`

Args: `{}`. Result: `payment_url`, `expires_at_ms`.

### `account` / `deactivate`

Args: `{}`. Result: `{}`. See Decision 39.

### `account` / `add_device`

Args: `{}` (new device = header). Result: `{}`.
Errors: `conflict`/`device.removed`, `device.limit`.

### `account` / `remove_device`

Args: `device_id`. Result: `{}`.

### `account` / `databases`

No account needed. Args: `cursor?`, `limit?` 1…90.
Result: `cursor?`, `databases[]` `{database_id, role, db_salt?}` (`db_salt`
on owned). Excludes deleted.

### `account` / `status`

Result: `paid`, `storage_used_bytes`, `transfer_used_bytes`,
`devices[]` ≤256 `{device_id, added_at_ms}`.

### `maintenance` (DEV, `githubOidc`, **no batching**)

| intent | args | result |
|---|---|---|
| `snapshot_proof_provision` | `node_key`, `device_id`, `database_id`, `db_salt`, `birth_signature` | `{}` |
| `snapshot_proof_status` | `run_id`, `run_attempt` | `phase`, `database_id`, `retry_after_ms?`, `released_bytes?` |
| `snapshot_proof_retire` | same | same |
| `snapshot_proof_inspect` | `through` 1…32 | `database_id`, `entries`, `facts` |

No `account_id` / `account_generation` / `database_id_version`.

## New-device restore (end to end)

1. Re-derive node key from the 24-word phrase → `account`/`add_device`.
2. `account`/`databases` → for owned rows, re-derive db key from `db_salt`
   and check against `database_id`.
3. Per database: `manage`/`access` → verify `birth_signature` and `owner`.
4. `fetch`/`snapshot` → manifest handle, cursor, hold.
5. `fetch`/`file` on the manifest → check sha256 == `restore_point_id`,
   decrypt.
6. Batch `fetch`/`file` (≤90) for links → GET each file, check sha256.
7. Install under the manifest’s name → `mutations_since` from the snapshot
   cursor.

A public reader does steps 3–7 with any key (no account).

## Errors (closed set)

| HTTP | code | fields |
|---|---|---|
| 400 | `invalid_request` | arg paths, `grants.any.role`, `birth_signature`, `cursor` |
| 400 | `batch_too_large` | `batch.response` |
| 401 | `unauthorized` | `request.signature`, `request.issued_at`, `request.nonce`, `device.unknown`, `device.removed`, `account.retired` |
| 403 | `forbidden` | `grant.absent`, `grant.role` |
| 403 | `payment_required` | `payment.subscription` |
| 404 | `not_found` | `database.id`, `database.deleted`, `file_handle`, `restore_point_id`, `snapshot_upload` |
| 409 | `conflict` | `database.id`, `account.exists`, `device.removed`, `device.limit`, `entries.writer_seq`, `file.unverified` |
| 409 | `cursor_expired` | `cursor` |
| 426 | `upgrade_required` | `request.protocol` (not on account) |
| 429 | `rate_limited` | `account.register` (per IP), `read.public` (per IP+database) |

Removed vs earlier draft: `predecessor_conflict` (folded into
`cursor_expired`), `mutation_log.*` fields.

## Cursors, proposals, retention (Decision 41)

- Cursors are opaque, per database, forward-only. One delivery order;
  members skip proposal entries.
- A proposal is deleted only when a later publisher `mutations_since`
  carries a cursor past it.
- ExeMem owns log cutting. It never deletes the latest restore point or one
  under hold.
- On `cursor_expired`: `fetch`/`snapshot` (latest) → restore → resume
  `mutations_since` from that cursor.
- Submitter sees acceptance via a direct entry whose `incorporated_from`
  matches its key, device, and writer_seq.

## Old → new

| Old | New |
|---|---|
| UUID `database_id` | 64-hex db public key |
| `create-database` + `kind`/`spec` | `manage`/`create` `{db_salt, birth_signature}` |
| `share/members/settings/*` | **removed** |
| `ShareSettings.read: anyone` | `grant` `any` / `read` |
| `lastdb-read-v1.` links | People delivers content key; cloud auth = `grant#any` |
| `blob/bytes` prepare/complete | `push`/`file` begin/commit |
| `delivery/object` ranges | `fetch`/`file` presigned GET |
| `snapshot/bytes` prepare/publish + plaintext Manifest | `push`/`snapshot` begin/commit; encrypted manifest file |
| `advance_cutoff` / `previous` / destinations | ExeMem-internal (D37) |
| `Authorization: LastDBNode` / `X-API-Key` | `nodeSignature` |
| inbox settle discard | **removed** (silent) |
| proposal status / `mutation_id` access | `incorporated_from` on direct entries |

## Build work

**ExeMem:** node-key → databases index; storage + transfer counters; device
list; tombstones; restore-point holds; stop reading create `kind`/`spec`.

**Node:** local registry keys by 64-hex ids and stores `db_salt`; stop
sending `CreateBody.kind`/`spec`; derive db keys; verify birth on access;
use `incorporated_from` instead of submission status.

## Open questions for Tom

None. Lapsed-publisher payment is Decision 38 as approved.
