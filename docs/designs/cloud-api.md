# Cloud API (ExeMem): one contract

Status: design for review (2026-11-19). This is not the live contract yet.
When this document and `docs/designs/cloud-api.openapi.json` differ, the
OpenAPI file wins for paths, auth, and error shapes.

Tom (2026-11-19): action-based interface. The node Ed25519 key is the
identity. Every node request is signed. The cloud stays blind to database
contents. No dual paths, aliases, fallback, or migration. No unit tests.

## Trust model

ExeMem (and every relay — see `docs/designs/relay-protocol.md`) is
**untrusted storage and transport**. Honest nodes must independently reach
the **same canonical source of truth** even if a relay is hacked. A
compromised server may reorder delivery, withhold entries, or fork (serve
different histories to different nodes). It must not be able to make honest
nodes **silently disagree**.

| Trusted for | Not trusted for |
|---|---|
| Availability (store and return bytes when it chooses) | Canonical order |
| | Completeness of what any one node has seen |
| | Authorization / acceptance |
| | Integrity or authenticity of payloads |

### Acceptance (local node rule)

**Acceptance is a local node rule.** Relays neither enforce nor know about
it. Each node runs the same deterministic function on signed data. Relays
only store and relay bytes. Any relay-side check (signature shape, quota,
“is this key known”) is **anti-spam / quota policy** that another relay may
do differently; nodes must not rely on it for correctness.

`AcceptEntry(E, S) → accept | reject` — `S` is the node’s already-verified
signed history for `E.database_id`:

1. **Signature.** Ed25519 verify of `E.signature` over `signed_bytes(E)`
   with `E.node_key`. Fail → reject.
2. **Database bind.** `E.database_id` matches the database of `S`. Fail →
   reject (blocks cross-db replay).
3. **Chain link.** `(E.writer_seq, E.prev_hash)` extends the chain in `S`
   for `E.node_key`, or matches an existing entry with the same
   `entry_hash` (idempotent duplicate). Else → reject (gap or
   equivocation evidence).
4. **Publisher authorization.** Let `owner` be the owner node key from the
   verified birth certificate in `S`.
   - If `E.node_key == owner` → authorized (publisher may append data,
     grant, checkpoint, relays, birth continuation).
   - Else `E` must be `kind=data` and the writer must hold a
     **publisher-signed grant** in the publisher spine in `S`:
     - `stream=direct` → effective role `write` under the grant-window
       rules in § Signed history (`grant_publisher_seq`, checkpoint `C`).
     - `stream=proposal` → effective role `submit` or `write` at the
       publisher seq the entry binds (or, if unbound, at the highest
       publisher seq in `S` when the proposal is admitted tentatively).
   - Else → reject.
5. **On reject:** do not apply `E`; retain it as evidence; **stop that
   writer’s chain** at the gap (later seq for that key cannot apply). Do
   not advance **canonical** state past a failed or missing publisher-chain
   link.

Canonical replay (checkpoint pull-in order, tentative Lamport order) is
defined in § Signed history and uses only entries that already passed
`AcceptEntry`. Proposal incorporation content is authorized because the
**publisher** signed the incorporating entry (step 4, owner path); readers
do not re-check the proposer’s grant for incorporated bytes.

**Guarantees (honest nodes):**

- Canonical history and its order come only from **signed data** (writer
  hash chains + the publisher spine). Never from cloud sequence, cursor, or
  receive time.
- `AcceptEntry` is the only admission gate; every honest node computes the
  same result on the same `S`.
- A hacked relay cannot invent write access that honest nodes will accept.
- Reorderings of the same signed set yield the same canonical state.
- Forks, equivocation, and gaps become **detectable signed evidence** once
  publisher heads are compared (own devices and People). On detection,
  nodes stop advancing canonical state for that database.

**Limit (plainly):** A relay can always stop or delay delivery. That is a
**liveness / freshness** failure (for example, withholding the newest
revocation). It cannot make honest nodes apply divergent canonical state
without producing evidence when heads meet. See Open questions for any
extra freshness signal.

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
batch body. **Request signatures authorize the HTTP call only.** They are not
a substitute for per-entry writer signatures (Decision 42).

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
- `manage`/`create` (ExeMem sugar) or a chained `birth` log entry carries
  `db_salt` and `birth_signature`: Ed25519 by the **db key** over
  `lastdb-db-birth-v1\n<database_id>\n<owner node key>\n<db_salt>`.
- **Nodes** verify the birth certificate into `S` before `AcceptEntry` can
  treat anyone as owner. ExeMem may also check birth when indexing a new
  database (anti-spam / billing); that check is not acceptance.
- The db key signs **nothing else**. The birth certificate is how nodes
  learn the **publisher (owner) node key**. The publisher needs **no grant**
  for its own writes.
- Members and public readers obtain `owner`, `db_salt`, `birth_signature`
  (from a birth log entry, People, or ExeMem `access` cache) and verify:
  signature against `database_id`, and `owner` matches the publisher People
  told them about.
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

## Signed history (Decision 42)

### Writer hash chains

Every writer’s entries form a **gapless hash chain** under that writer’s
node key.

- `writer_seq` starts at **1** and increases by exactly **1** per entry.
- `prev_hash` is 32 zero bytes for `writer_seq = 1`; otherwise it is
  `entry_hash` of that writer’s previous entry.
- `entry_hash = SHA-256( signed_bytes || signature_raw_64 )` (signature
  decoded from base64url).
- Two different valid signatures for the same `(node_key, writer_seq)`, or
  two valid entries that claim the same `prev_hash` with different
  `entry_hash`, are **equivocation** — provable fork evidence.
- Dedupe key is `(node_key, writer_seq)`. Same seq with identical
  `entry_hash` is a no-op replay (`duplicate`). Same seq with a different
  `entry_hash` is `conflict` / `entries.writer_seq` at push, and fork
  evidence for readers.
- Cloud delivery sequence, cursor, and receive time are **excluded** from
  signed bytes and **never** define order.

### Signed-bytes layout (common)

All entry kinds share a length-prefixed binary layout. Integers are
big-endian. Hex fields on the wire are lowercase; in signed bytes they are
the **raw 32-byte** value unless noted.

Common header (every kind), in order:

| Field | Encoding |
|---|---|
| domain | UTF-8 label + `0x00` (see kinds below) |
| `database_id` | 32 bytes |
| `node_key` | 32 bytes (writer) |
| `device_id` | UUID as 16 raw bytes |
| `writer_seq` | u64 |
| `prev_hash` | 32 bytes |
| `authored_at_ms` | u64 |
| `lamport` | u64 (≥ previous entry’s `lamport` for this writer; first ≥ 1) |
| `last_seen_publisher_seq` | u64 (0 = none) |
| `last_seen_publisher_hash` | 32 bytes (zeros if seq = 0) |

Then kind-specific fields. The writer signs the concatenation with its
**node** Ed25519 key. Wire field `signature` is base64url, no pad (64 bytes).

`last_seen_publisher_*` is the highest publisher chain head this writer had
fully verified when signing. It carries fork evidence into every entry.

### Entry kinds

| Kind | Domain | Who | Wire / reserved |
|---|---|---|---|
| `data` | `lastdb-mutation-v1` | any writer | `namespace` not `grant` / `checkpoint` |
| `grant` | `lastdb-grant-v1` | publisher only | reserved namespace `grant` |
| `checkpoint` | `lastdb-checkpoint-v1` | publisher only | reserved namespace `checkpoint` |

**Data** fields after the header:

| Field | Encoding |
|---|---|
| `stream` | `0x00` direct, `0x01` proposal |
| `namespace` | u16 length + UTF-8 (1…128), not `grant`/`checkpoint` |
| `payload_sha256` | 32 bytes = SHA-256 of decoded `payload_b64` |
| `has_file` | `0x00` / `0x01` |
| `file_handle` | if has_file: u16 length + UTF-8 (1…256) |
| `file_sha256` | if has_file: 32 bytes |
| `has_incorporated_from` | `0x00` / `0x01` |
| incorporated | if set: `node_key` 32, `device_id` 16, `writer_seq` u64, `signature_sha256` 32 |
| `has_grant_at` | `0x00` / `0x01` |
| `grant_publisher_seq` | if set: u64 — publisher `writer_seq` of the write grant this entry relies on |

Rules for data:

- **Publisher** direct data: `has_grant_at = 0`. Owner needs no grant.
- **Member** direct data (`stream = direct`): `has_grant_at = 1`. See
  grant checks below.
- **Proposal** (`stream = proposal`): writer signature required;
  `has_grant_at = 0`. Submitters need a submit grant at push time for
  ExeMem’s door check; readers do not re-check the submitter’s grant on
  **incorporated** content (publisher vouched by signing the incorporation).
- **Incorporation:** publisher direct data with `has_incorporated_from = 1`.
  Always authorized as publisher. The referenced proposal’s original
  signature stays independently verifiable via `signature_sha256`.

**Grant** fields after the header (Decision 43):

| Field | Encoding |
|---|---|
| `grantee` | `0x00` + 32-byte node key, or `0x01` for `any` |
| `role` | `0x01` read, `0x02` submit, `0x03` write, `0x00` revoked |

One signed grant entry per member key (or one `any` entry). No member-list
document, no share/members/update, no list-revision. Upsert = a new grant
entry at a new publisher `writer_seq`. Revoke = role `revoked`.

`any` is **read-only**. `any` + non-read → `invalid_request` /
`grants.any.role`. Readers never consult `any` for write authorization.

**Checkpoint** fields after the header:

| Field | Encoding |
|---|---|
| `head_count` | u16 |
| heads | `head_count` × (`node_key` 32, `writer_seq` u64, `entry_hash` 32), sorted by `node_key` ascending |

A checkpoint lists each writer chain head the publisher **accepts as
canonical**. See Open questions: whether listing heads conflicts with “no
member list document”.

### Batch transport

`push`/`mutations` entries and `fetch`/`mutations_since` entries each carry
their own `signature` and **every signed field** (including `database_id`,
`node_key`, `device_id`, `prev_hash`, `lamport`, `last_seen_publisher_*`,
kind-specific fields, and decoded `payload_b64` when kind is `data`). Each
entry is independently verifiable regardless of batching or delivery order.

### Publisher spine and canonical state

The **publisher chain** is the canonical spine (owner node key from the
birth certificate).

1. Readers verify the publisher chain gaplessly (`writer_seq`, `prev_hash`).
2. Grant/revocation state is the deterministic fold of `grant` entries in
   publisher `writer_seq` order.
3. **Canonical state** is the deterministic replay of the publisher chain:
   for each `checkpoint` entry, pull in the listed writer entries that are
   not yet canonical, in fixed order: **writer `node_key` ascending, then
   `writer_seq` ascending**. Each pulled entry must verify against its
   writer chain through that head.
4. Member direct writes are **tentative** until a publisher checkpoint
   lists them. Tentative display order (non-canonical): `lamport`
   ascending, then `node_key` ascending. Canonical state never depends on
   cloud order.
5. Therefore member writes need the **publisher node online** to become
   final under this shape. Alternative (fully deterministic merge of all
   writer chains without checkpoints) is an Open question — weaker
   revocation semantics.

### Grant checks for member direct writes

These rules refine step 4 of `AcceptEntry` (§ Trust model) for member
direct data. Let `W` be a member direct data entry with
`grant_publisher_seq = G`, applied when a checkpoint `C` lists it.

1. `W`’s writer signature and hash-chain link verify.
2. Publisher entry at seq `G` is a `grant` that sets `W.node_key` to role
   **`write`** (effective role after folding grants with `seq ≤ G`).
3. No publisher `grant` with seq `R` where `G < R ≤ C.writer_seq` sets that
   key’s role to anything other than `write` (revocation or downgrade
   closes the window).
4. `W.last_seen_publisher_seq` / `hash` match the reader’s publisher chain
   at that seq; mismatch → fork evidence, do not apply.
5. If the reader knows a revocation/downgrade `R` and
   `W.last_seen_publisher_seq ≥ R`, the writer **must have known** about
   `R` and still signed a write under grant `G` → reject, keep evidence.

**Write signed before vs after a revocation by publisher seq:**

- If `C.writer_seq < R`, checkpoint `C` is entirely before the revocation:
  `W` may apply (rules 1–4).
- If `R ≤ C.writer_seq`, rule 3 rejects `W` at `C` even if `W` was authored
  “earlier” in wall time. Publisher seq, not clock time, orders grants.
- Wall-clock `authored_at_ms` is informational and is not used for grant
  validity.

**Proposals:** verify the submitter’s writer signature. Publisher
incorporation is a publisher direct entry (always authorized). Readers do
**not** require a still-valid submit grant on the proposer for incorporated
content; the publisher’s signature is the vouch. `grant#any` is irrelevant
to writes.

### Relay / ExeMem policy vs node acceptance

**Nodes:** always run `AcceptEntry` (§ Trust model) on every entry before
apply — including entries a relay happily stored. Delivery success means
nothing about correctness.

**Relays / ExeMem (optional anti-spam and quota only):** may check request
`nodeSignature`, approximate signature/chain shape, quotas, billing, or a
cheap “key known to this database” door. Those checks are **policy**:
another conforming relay may skip or differ. Nodes must not treat a relay
accept or reject as authorization. A relay that stores a bad entry is
still conforming for correctness purposes; honest nodes will reject it
locally. Error codes such as `entries.signature` on push, if returned, are
spam/quota responses — not part of the acceptance function.

### Forks, withholding, resolution

- Publisher chain gap or `prev_hash` mismatch → withhold/corruption
  detected; stop canonical advance; surface
  `conflict` / `history.withheld` (local/app signal; may also appear if a
  node refuses to proceed).
- Equivocating writer → keep both signed entries; surface
  `conflict` / `history.equivocation`.
- Divergent publisher heads (same seq, different hash, or incompatible
  tips) discovered when **comparing heads** → `conflict` /
  `history.fork`. Compare paths: (1) a node’s own devices compare
  latest-seen signed publisher heads; (2) members exchange heads via
  **People** when they connect (no new cloud intent required); (3) every
  entry embeds `last_seen_publisher_*` so ordinary writes propagate
  evidence.
- On fork/withheld: **stop advancing canonical state** for that database,
  keep the evidence (both signed heads/entries), show the app a
  database-forked/withheld condition, and require a **publisher-signed
  resolution** entry (same publisher key) that explicitly names the chosen
  tip `entry_hash` before canonical progress resumes. Resolution wire shape
  is an Open question if not folded into a checkpoint.

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
4. **Superseded by 43.** Grants are publisher-signed chain entries, not
   request-only ACL rows.
5. **No** protocol header on **account**.
6. Live OpenAPI under **`cloud_backup_protocol/`**.
7. DEV snapshot-proof under **`maintenance`**, `x-lastdb-environment: dev`.
8. Actions: `fetch` / `push` / `manage` / `account` / `maintenance`.
9. `push`/`mutations`: no `kind` field for landed stream; `landed` from
   door grant (`direct` / `proposal`). Publisher always `direct`. Entry
   `stream` is signed and must match what ExeMem lands.
10. `fetch`/`mutations_since`: publisher sees direct+proposal; receivers
    direct only; accept = publisher push with `incorporated_from`; entries
    tagged `stream`.
11. **Silent rejections** — no discard call.
12. Access under **manage**; no share-settings / `rules` surface.
13. Cleanup / retention **ExeMem-internal** — no public ops.
14. Ingested proposals deletable when publisher fetch cursor advances past
    them (Decision 41). Delivery cursor is not canonical order.
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
31. **Superseded by 43.** Public read remains `grant#any` at **`read`
    only**, expressed as a publisher-signed grant entry with grantee
    `any`.
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
35. Access levels everywhere: **`read` | `submit` | `write`** (plus
    signed role `revoked` on grant entries).
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
41. **Cursors / proposals / restore:** opaque forward-only **delivery**
    cursors (pagination only, not canonical order); proposal deletion only
    after a later publisher delivery cursor past it; `cursor_expired`
    recovery = latest snapshot → restore → resume from its cursor.
    Acceptance visible via `incorporated_from` (no status intent).
42. **Per-entry writer signatures + hash chains** (§ Signed history).
    Request signatures are insufficient. **Acceptance is local**
    (`AcceptEntry` in § Trust model). Relays do not enforce it. Failure
    rule: stop that writer’s chain at the gap; retain evidence.
43. **Signed grants in the publisher spine** (§ Signed history). Each
    grant/revocation is a publisher-signed `lastdb-grant-v1` entry in the
    publisher’s ordered direct stream (reserved namespace `grant`).
    `manage`/`grant` and `manage`/`revoke` carry the signed entry for
    transport convenience; authorization is only via `AcceptEntry` over
    the publisher chain + checkpoints. Checkpoint entries
    (`lastdb-checkpoint-v1`) make member writes canonical. Relays are
    untrusted except availability (`relay-protocol.md`).

## Opaque strings and encodings

| Name | Max | Notes |
|---|---|---|
| `cursor`, `file_handle`, `snapshot_upload` | 256 | Opaque; delivery pagination only |
| `url` | 8192 | Presigned |
| `headers` | 32 entries, values ≤1024 | String map |
| `payload_b64` | ≤96 KiB decoded | base64url, no pad |
| `db_salt` | 64 hex | 32 bytes |
| `birth_signature`, entry `signature` | base64url, 64 bytes | Ed25519 |
| `prev_hash`, `entry_hash`, `*_sha256` | 64 hex | 32 bytes |
| `restore_point_id` | 64 hex | sha256 of manifest file |
| reserved namespaces | — | `grant`, `checkpoint` |

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

Each element is a full signed entry (`data` | `grant` | `checkpoint`) as in
§ Signed history. Delivery order is not canonical order.

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

Who (ExeMem door policy, not acceptance): write/publisher → `direct`;
submit → `proposal`; read may be refused as spam/quota.
Errors (policy/transport): `conflict`/`entries.writer_seq`,
`forbidden`, `payment_required`, `not_found`/`file_handle`. A relay may
also return spam rejects; nodes ignore those as correctness signals.

| args | req |
|---|---|
| `entries[]` 1…90 | yes |

Each entry is a full signed `data` or (publisher only) `checkpoint` entry.
File refs must already be committed. Submit cannot attach files. Publisher
`checkpoint` entries are how member writes become canonical. Peers run
`AcceptEntry` on read regardless of push outcome.

| result | req |
|---|---|
| `landed` | yes |
| `next_writer_seq` | yes |
| `duplicate` | no |

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

Who: publisher (ExeMem may require account/payment as product policy).
Args: `entries[]` 1…90 signed `grant` entries (`role` ∈
`read`|`submit`|`write`). Result: `{}`.
Transport sugar: ExeMem stores the entries on the publisher delivery
stream (same bytes as `push`/`mutations` of `kind=grant`). Optional
anti-spam may refuse garbage; **acceptance is only `AcceptEntry` on each
node**. `any` + non-`read` → `invalid_request`/`grants.any.role` (schema).
Under the open relay protocol these entries leave the manage surface and
are plain `log.append` (`relay-protocol.md`).

### `manage` / `revoke`

Who: publisher.
Args: `entries[]` 1…90 signed `grant` entries with `role = revoked`.
Result: `{}`. Same transport as grant. Revoking `any` turns public read
off for nodes that have accepted that entry via `AcceptEntry`.

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

`role` / `level` here are ExeMem’s cached door view for UX only. Nodes
authorize solely via `AcceptEntry` over signed history.

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
   delivery cursor → verify chains, fold publisher grants, replay
   checkpoints to rebuild canonical state; compare publisher heads with
   other devices / People before treating the tip as final.

A public reader does steps 3–7 with any key (no account).

## Errors (closed set)

| HTTP | code | fields |
|---|---|---|
| 400 | `invalid_request` | arg paths, `grants.any.role`, `birth_signature`, `cursor` (relay may also use spam fields; not acceptance) |
| 400 | `batch_too_large` | `batch.response` |
| 401 | `unauthorized` | `request.signature`, `request.issued_at`, `request.nonce`, `device.unknown`, `device.removed`, `account.retired` |
| 403 | `forbidden` | `grant.absent`, `grant.role` |
| 403 | `payment_required` | `payment.subscription` |
| 404 | `not_found` | `database.id`, `database.deleted`, `file_handle`, `restore_point_id`, `snapshot_upload` |
| 409 | `conflict` | `database.id`, `account.exists`, `device.removed`, `device.limit`, `entries.writer_seq`, `file.unverified`, `history.fork`, `history.equivocation`, `history.withheld` |
| 409 | `cursor_expired` | `cursor` |
| 426 | `upgrade_required` | `request.protocol` (not on account) |
| 429 | `rate_limited` | `account.register` (per IP), `read.public` (per IP+database) |

`history.*` fields are primarily reader/app signals when local verification
detects fork, equivocation, or a gap; ExeMem may also return
`entries.writer_seq` on push-time equivocation.

Removed vs earlier draft: `predecessor_conflict` (folded into
`cursor_expired`), `mutation_log.*` fields.

## Cursors, proposals, retention (Decision 41)

- Delivery cursors are opaque, per database, forward-only. They paginate
  ExeMem’s blob store; they do **not** define canonical order.
- One delivery stream may interleave writers; members skip proposal
  entries in delivery.
- A proposal is deleted only when a later publisher delivery
  `mutations_since` carries a cursor past it.
- ExeMem owns log cutting. It never deletes the latest restore point or one
  under hold.
- On `cursor_expired`: `fetch`/`snapshot` (latest) → restore → resume
  `mutations_since` from that cursor, then re-verify chains.
- Submitter sees acceptance via a direct entry whose `incorporated_from`
  matches its key, device, writer_seq, and signature hash.

## CI plan (DEV E2E, not unit tests)

Run against a fresh DEV home with the real CLI/daemon and a DEV ExeMem
(and, per `relay-protocol.md`, the same scenarios against a second
reference relay). No unit tests. No primary data. Correctness assertions
are **node-side** (`AcceptEntry`), not relay reject codes.

1. **Tampered payload accepted by relay:** flip one byte of `payload_b64`
   after signing; if the relay stores it anyway, a reading node still
   rejects via `AcceptEntry` (bad signature), does not apply, stops that
   writer chain.
2. **Payload / namespace / writer_seq / database_id tamper on read:** altered
   entry → node rejects; cross-db replay (wrong `database_id`) fails step 2
   of `AcceptEntry`.
3. **Incorporation mismatch:** publisher entry whose
   `incorporated_from.signature_sha256` does not match the proposal → node
   rejects.
4. **Seq conflict / equivocation:** same `(node_key, writer_seq)` with
   different bytes → node keeps evidence (`history.equivocation`); relay
   may also return a policy conflict on append.
5. **Forged grant:** grant not signed by the publisher → `AcceptEntry`
   rejects; cannot authorize member writes.
6. **Revoked-grant replay:** write whose `grant_publisher_seq` predates a
   later publisher revocation included by checkpoint `C` → node rejects at
   `C`.
7. **Unsigned / bad writer signature:** node rejects; chain stops.
8. **Withheld publisher entry (seq gap):** node detects gap on publisher
   spine, halts canonical advance, surfaces `history.withheld`.
9. **Reordered delivery:** same signed set, different order → identical
   canonical state after checkpoint replay.
10. **Forked publisher history:** divergent tips → head compare detects
    `history.fork`; stop canonical advance; retain both signed tips.
11. **Equivocating writer:** two signed entries same seq different hash →
    `history.equivocation`.

## Old → new

| Old | New |
|---|---|
| UUID `database_id` | 64-hex db public key |
| `create-database` + `kind`/`spec` | `manage`/`create` `{db_salt, birth_signature}` |
| `share/members/settings/*` | **removed** |
| Request-authorized grants only | Publisher-signed `grant` chain entries + checkpoints |
| `ShareSettings.read: anyone` | signed `grant` `any` / `read` |
| `lastdb-read-v1.` links | People delivers content key; cloud auth = `grant#any` |
| `blob/bytes` prepare/complete | `push`/`file` begin/commit |
| `delivery/object` ranges | `fetch`/`file` presigned GET |
| `snapshot/bytes` prepare/publish + plaintext Manifest | `push`/`snapshot` begin/commit; encrypted manifest file |
| `advance_cutoff` / `previous` / destinations | ExeMem-internal (D37) |
| `Authorization: LastDBNode` / `X-API-Key` | `nodeSignature` |
| inbox settle discard | **removed** (silent) |
| proposal status / `mutation_id` access | `incorporated_from` on direct entries |
| Cloud cursor as history order | Signed writer chains + publisher checkpoints |

## Build work

**ExeMem / relays:** store and relay opaque signed entries; optional
anti-spam/quota only; stop treating delivery order or door ACL as
authoritative for nodes; account/billing service remains ExeMem-specific
(`relay-protocol.md`).

**Node:** implement `AcceptEntry`; hash-chain signing; grant fold +
checkpoint replay; head comparison across devices / People; local
fork/withheld/equivocation surfaces; derive db keys; verify birth; use
`incorporated_from` instead of submission status.

## Open questions for Tom

1. **Publisher online for finality.** Under checkpoints, member direct
   writes stay tentative until the publisher signs a checkpoint. Is that
   acceptable? Alternative: fully deterministic merge of all writer chains
   by signed `lamport` then `node_key`, with grant validity judged only by
   each write’s `grant_publisher_seq` (no checkpoint). That removes the
   publisher-online requirement but weakens revocation (a write can stay
   applyable without the publisher explicitly accepting a post-revocation
   head).
2. **Checkpoints vs “no member list”.** A checkpoint lists
   `writer key → seq + hash` heads. That is not a share/members document and
   has no list-revision, but it does name members. Confirm this is allowed,
   or name a different canonicalization device that stays within the rule.
3. **Freshness of the newest revocation.** Head comparison detects forks
   and permanent withhold gaps once peers meet; ExeMem can still delay the
   **tail** (newest revocation) indefinitely. Is People + multi-device head
   exchange enough, or do you want a minimal publisher-signed
   head/checkpoint with a time bound (and if so, should it be a cloud
   intent or People-only)?
4. **Fork resolution wire shape.** Require a publisher-signed resolution
   naming the chosen tip `entry_hash` — fold into `checkpoint`, or a
   dedicated reserved namespace / manage intent?
