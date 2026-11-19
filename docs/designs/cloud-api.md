# Cloud API (ExeMem): one contract

Status: design for review (2026-11-19). Not the live contract yet.
When this document and `docs/designs/cloud-api.openapi.json` differ, the
OpenAPI file wins for paths, auth, and error shapes.

Companion: `docs/designs/relay-protocol.md` (open relay). No dual paths,
fallback, migration, or unit tests.

Tom (2026-11-19): action-based interface. Node Ed25519 key is identity.
Every change carries a **writer signature**. Cloud/relay stays blind to
contents.

## Trust model

ExeMem and every relay are **untrusted storage and transport**. Honest
nodes reach the same canonical state from **signed data only**. Relays do
not authorize.

| Trusted for | Not trusted for |
|---|---|
| Availability | Order, completeness, acceptance, integrity |

**Logs are append-only and never trimmed.** Relays never delete log
entries. There are **no snapshots**. File blobs may be reclaimed only after
a publisher-signed `blob_delete` chain entry; readers treat a deleted blob
as intentionally gone.

### Acceptance (local node rule)

**Acceptance is a local node rule.** Relays neither enforce nor know about
it. Optional relay checks are anti-spam/quota only.

**Role changes live only in the chain.** Every grant and every role
reduction (including revoke), with its per-device cutoff heads, is a signed
`grant`-kind entry on a publisher device chain. Same merge order; covered
by publisher signed heads. **No side channel** — not on the relay surface,
not in ExeMem rows, not in People. People may carry **signed heads for
comparison only**. Any relay/ExeMem grant cache for anti-spam is **derived
from the chain**, optional, never authoritative. A reader that receives a
grant/revoke only via a side channel **must ignore it**.

`AcceptEntry(E, S) → accept | reject` — `S` is the node’s verified signed
history for `E.database_id`:

1. **Signature.** Verify `E.signature` over `signed_bytes(E)` with
   `E.node_key`.
2. **Database bind.** `E.database_id` matches `S`.
3. **Device chain.** `(E.writer_seq, E.prev_hash)` extends
   `(E.node_key, E.device_id)`, or identical `entry_hash` duplicate.
   Out-of-order delivery is buffered.
4. **Lamport.** Let `L_prev` be the lamport of this device’s previous entry
   (0 if `writer_seq = 1`). Let `L_heads` be the max lamport among the
   entries named by `E.seen_publisher_heads` (0 if empty). Require
   `E.lamport == 1 + max(L_prev, L_heads)`. Else reject.
5. **Publisher authorization / cutoffs.**
   - Owner (from birth) → authorized for publisher kinds.
   - Else `kind=data`: apply grant-window + **role-reduction cutoff** rules
     below. Direct writes past a cutoff are rejected **regardless of**
     `seen_publisher_heads`.
6. **On reject:** do not apply; keep evidence; stop that device chain at
   the gap.

**Canonical merge:** all accepted entries sorted by
`(lamport, node_key, device_id)`. At equal Lamport for grants to the same
grantee, **`revoked` wins**.

**Publisher-signed heads:** each device tip is signed (the tip entry).
Nodes compare publisher heads across own devices and via People. On
mismatch → stop advancing (`history.fork` / `history.withheld` —
**node-side signals**, not server HTTP errors).

**Guarantee:** after a reader has a publisher-signed role reduction with
cutoffs for a member **in the chain**, nothing past those cutoffs from
that member is accepted. Delayed delivery is liveness; head comparison
detects it.

**Limit:** relays can delay or stop delivery. They cannot make honest nodes
silently disagree once heads meet.

## Minimal signed history

1. Writer signature on every change.
2. One hash chain per `(node_key, device_id)`, gapless.
3. Grants / role reductions / revokes as signed entries on publisher device
   chains.
4. Local `AcceptEntry` + merge `(lamport, node_key, device_id)`; tie →
   revoke wins.
5. Publisher signs heads; nodes compare; stop on mismatch.
6. **No checkpoints. No snapshots.** Member direct write is final once it
   passes the grant-window against the publisher state it binds.
7. Proposals count only after publisher **incorporation**.
8. **Append-only, never-trimmed logs.**

### Device chains

- `writer_seq` ≥ 1, +1 each time; `prev_hash` within the device.
- `entry_hash = SHA-256(signed_bytes || signature_raw_64)`.
- Dedupe `(node_key, device_id, writer_seq)`. Multi-device same seq is fine.
  Equivocation = two hashes at one device position.
- Stolen node key may mint a new `device_id` (Decision 21).

### Signed bytes (common header)

Length-prefixed, big-endian. Domain + `0x00`, then:

`database_id`(32) · `node_key`(32) · `device_id`(16) · `writer_seq`(u64) ·
`prev_hash`(32) · `authored_at_ms`(u64) · `lamport`(u64) ·
`seen_publisher_heads` (u16 + (`device_id`, `writer_seq`, `entry_hash`)×
sorted by `device_id`).

Wire `signature`: base64url, 64 bytes.

| Kind | Domain | Notes |
|---|---|---|
| `birth` | birth material in chain | First publisher-device entry (`writer_seq=1`) |
| `data` | `lastdb-mutation-v1` | stream; payload hash; optional file / incorporation / grant_at |
| `grant` | `lastdb-grant-v1` | grantee, role; **cutoffs required on any role reduction** |
| `blob_delete` | `lastdb-blob-delete-v1` | publisher; names `sha256` of reclaimable file blob |
| `heads` | `lastdb-heads-v1` | optional compact publisher head vector |
| `relays` | `lastdb-relays-v1` | publisher-signed relay URL list |
| `resolve` | `lastdb-resolve-v1` | fork resolution tip |

**Data:** publisher direct needs no grant. Member direct binds `grant_at`
and `seen_publisher_heads`. Proposal: no grant_at; content only via
incorporation.

**Birth:** first chained entry on a publisher device. ExeMem
`manage`/`create` carries the full signed `BirthEntry` (ExeMem cannot
sign one).

### Grant window

Member direct write `W` with `grant_at = G` and `seen_publisher_heads = H`:

1. Fold grants in merge order through the publisher prefix implied by `H`.
   At `G`, effective role must be **`write`**.
2. No later grant in that prefix through `H` lowers the role below
   `write`. If `H` already covers a reduction/revoke, reject.
3. Apply **all role-reduction cutoffs** present in verified `S` (below).

### Role-reduction cutoffs

Every `grant` entry that **lowers** a member’s role carries per-device
cutoff heads for that member:

- `write → submit` or `write → read`
- `submit → read`
- any role → `revoked`

`cutoffs[]`: `(device_id, writer_seq, entry_hash)`. Unseen device →
cutoff 0.

- Apply only when the reduction entry is in verified chain `S`.
- Accept that member’s **direct** entries only up to those heads; reject
  everything after **regardless of** `seen_publisher_heads`.
- Per-member data inside that member’s own grant entry — not a member list.

`any` is read-only (`role` must be `read`).

### Blob delete

Publisher signs `blob_delete` naming blob `sha256`. Relays may then drop
those file bytes (anti-spam/quota). Readers treat the blob as
intentionally gone. **Log entries are never deleted.**

### Fork resolution

Publisher `resolve` entry names chosen `(node_key, device_id, entry_hash)`.
Fillable gaps buffer and resume without resolution.

## Restore, new device, and local state

**There are no snapshots and no `cursor_expired`.** Delivery cursors are
optional pagination aids only; they never expire as a restore path.

**Restore / new device / new member:**

1. Discover a relay (People bootstrap + publisher `relays` entry).
2. `log.read` every device chain from `writer_seq = 1` (birth first on a
   publisher device).
3. Run `AcceptEntry` on each entry; merge by `(lamport, node_key,
   device_id)`; replay into materialized state.
4. Fetch file blobs **lazily** by content hash via `blob.get` (presigned
   URL) when needed.

**Local nodes need not retain full history.** After replay they keep:

- materialized database state, and
- for each `(node_key, device_id)`: latest head `(entry_hash, writer_seq,
  lamport)`, and
- folded **grant state** (including cutoffs),

which is enough to validate the next entry on that chain. They may discard
older entry bodies locally; the relay retains the append-only log.

## Bootstrap (existing databases)

No migration and no dual path. To move an existing database onto this
contract:

1. A publisher device appends a `BirthEntry` (`writer_seq = 1`).
2. It then emits the current materialized state as ordinary signed
   `data` mutations (records and blob refs) on its device chain.
3. Members receive **fresh** `grant` entries on the publisher chain.
4. Old unsigned cloud data is **replaced**, not kept alongside.

One signed history from birth forward; readers ignore any pre-bootstrap
unsigned store.

## Shape

```
POST /api/cloud/v3/{fetch,push,manage,account,maintenance}
```

Product: `{database_id, intent, args|result}`. Account/maintenance: no
`database_id`. Batch 1–90; req ≤256 KiB; resp ≤8 MiB.

| Action | Intents |
|---|---|
| `fetch` | `mutations_since`, `file` |
| `push` | `mutations`, `file` |
| `manage` | `create`, `grant`, `revoke`, `access`, `delete` |
| `account` | `register`, `payment`, `deactivate`, `add_device`, `remove_device`, `databases`, `status` |
| `maintenance` | (none in this design — former snapshot_proof_* removed) |

`manage`/`grant` and `manage`/`revoke` append signed `grant` entries to
the chain (transport sugar). `manage`/`create` body is the full signed
`BirthEntry`. `manage`/`access` is non-authoritative UX only.
`manage`/`delete` is publisher reclaim of the ExeMem product record; relay
logs stay append-only.

`fetch`/`file` and relay `blob.get` return **exactly one** download path: a
**presigned URL** (plus headers / expiry / size / sha256).

Stream is in the signed entry; push result has **no** `landed` field.

## Auth (`nodeSignature`)

Headers: `X-LastDB-Node-Key`, `Device-Id`, `Issued-At`, `Nonce`,
`Signature`, `X-LastDB-Cloud-Protocol: 3` (not on account).

Signed lines: `POST\n<path>\n<protocol|->\n<node key>\n<device id>\n
<issued-at>\n<nonce>\n<body sha256 hex>`.

Skew ±60s; nonces ≥2× skew. Account keys need a registered device (except
`add_device`). Request signatures authorize HTTP only.

## Database keys (Decision 36)

`db_salt`; HKDF `lastdb-db-key-v1`; `database_id` = db public key; birth
signature by db key. Db key signs nothing else. Owner needs no grant.
Tombstones on ExeMem product index; no key rotation. Nodes verify birth
into `S` first.

## Requirements (Decision 38)

Publisher pays for storage and transfer. Lapsed: reads work;
push/grant/create → `payment_required`; delete always allowed.

## Decisions (explicit)

1. Protocol **v3**.
2. Version in header + signature material; no body `version`.
3. **426** `upgrade_required` + `minimum_version` (not on account).
4. **Superseded by 43.** Grants are publisher-signed chain entries.
5. No protocol header on **account**.
6. Live OpenAPI under `cloud_backup_protocol/` when shipped.
7. **Superseded by 45.** DEV snapshot-proof maintenance intents **removed**
   (no snapshots).
8. Actions: `fetch` / `push` / `manage` / `account` (maintenance empty here).
9. **Superseded by 42.** Stream is signed on the entry; no server `landed`.
10. `fetch`/`mutations_since`: publisher sees direct+proposal; receivers
    direct only; acceptance = publisher incorporation.
11. Silent rejections — no discard call.
12. Access under manage; no share-settings / `rules`.
13. **Superseded by 44–45.** Logs append-only and never trimmed; no log
    retention/GC; blob reclaim via `blob_delete` only.
14. **Superseded by 44.** Proposals are chain entries and are **never**
    deleted.
15. `manage`/`delete` — publisher product reclaim / tombstone on ExeMem.
16. Envelope `{database_id?, intent, args|result}`.
17. Batching — single or array; caps above; per-item errors.
18. Account: `register`, `payment`, `deactivate`, `add_device`,
    `remove_device`, `databases`, `status`. No login. No `em_` API keys.
19. Node public key + device id in signed headers.
20. Ed25519 `nodeSignature` for all node traffic.
21. No per-device keys. Stolen device + node key = full access; may mint
    new device chains.
22. App-registry publish requires a paid ExeMem account.
23. `payment` → short-lived single-use signed `payment_url`.
24. `register` rate-limited per IP.
25. Lose all devices + phrase → account gone.
26. `account`/`databases`: paginated discovery; `db_salt` on owned.
27. `account`/`status`: paid, storage, transfer, devices.
28. No node-key rotation.
29. Connect / invites / joins are People-app only.
30. No `rules` / share-settings.
31. **Superseded by 43.** Public read = signed `grant` `any` / `read` only.
32. Device rule + public `read.public` rate limit; publisher pays.
33. File upload `begin`/`commit`; downloads = presigned GET only.
34. Create = full signed `BirthEntry` (chained birth).
35. Levels: `read` | `submit` | `write` (+ signed reductions / `revoked`).
36. Self-certifying derived `database_id`. Tombstones. No db-key rotation.
37. **Superseded by 45.** No snapshots, no `advance_cutoff`, no
    restore-from-snapshot, no `cursor_expired`.
38. Publisher pays; lapsed: reads work; push/grant/create →
    `payment_required`; delete always allowed.
39. `deactivate`: cancel subs; tombstone owned DBs; retire key.
40. Devices: `remove_device` bookkeeping; cap 256.
41. Delivery cursors are pagination only; they do not expire as a restore
    mechanism.
42. Writer signatures + device chains; local `AcceptEntry`; checked
    Lamport; no checkpoints; no `landed`.
43. Signed grants on publisher chains; cutoffs on every role reduction;
    merge + revoke-wins; publisher heads via devices/People; no side
    channel.
44. Append-only logs; proposals never deleted; blob delete via chain entry.
45. **No snapshots.** Restore = full log replay from birth; local store =
    materialized state + per-device heads + grant state. Bootstrap replaces
    old unsigned cloud data with signed mutations (no dual path).

### Defaults

| Topic | Default |
|---|---|
| Freshness | Head comparison only |
| Reference relay | In `lastdb` for CI |
| Public reads | `grant#any` open on every conforming relay |
| Relay list | Publisher-signed `relays`; People bootstraps |
| Birth | First chained publisher-device entry |
| Fork resolution | Publisher `resolve` entry |
| Downloads | Presigned URL only |

## Server errors (HTTP)

| HTTP | code | fields |
|---|---|---|
| 400 | `invalid_request` | arg paths, `grants.any.role`, `birth_signature`, `cursor` |
| 400 | `batch_too_large` | `batch.response` |
| 401 | `unauthorized` | `request.*`, `device.*`, `account.retired` |
| 403 | `payment_required` | `payment.subscription` |
| 404 | `not_found` | `database.*`, `file_handle`, `blob.sha256` |
| 409 | `conflict` | `database.id`, `account.exists`, `device.*`, `entries.writer_seq`, `file.unverified` |
| 426 | `upgrade_required` | `request.protocol` |
| 429 | `rate_limited` | `account.register`, `read.public` |

No server `403` `grant.absent` / `grant.role`. No `cursor_expired`.

### Node-side signals (not server error fields)

`history.fork`, `history.withheld`, `history.equivocation`, cutoff /
Lamport / signature rejects — local only.

## CI plan (DEV E2E, not unit tests)

1. Tampered payload: node rejects even if relay stored it.
2. Cross-db / field tamper: reject.
3. Incorporation mismatch: reject.
4. One-device equivocation: `history.equivocation`.
5. Multi-device offline same seq: **not** equivocation.
6. Forged grant: cannot authorize.
7. Backdated write after revoke (cutoff in chain): reject.
8. Backdated write after downgrade: reject regardless of
   `seen_publisher_heads`.
9. Write that had seen a reduction in `seen_publisher_heads`: reject.
10. Side-channel grant/revoke ignored.
11. Inflated lamport: reject.
12. Bad signature: device chain stops.
13. Reordered delivery → same merge.
14. Divergent publisher heads → stop advance.
15. Withheld reduction detected by head compare.
16. **Bootstrap:** publisher BirthEntry + state mutations; replay equals
    source materialized state; members need fresh grants; no dual path with
    old unsigned data.
17. **Restore from start:** new device fetches every chain from seq 1,
    `AcceptEntry` + merge + replay; lazy blob fetch by hash.

## Build work

**Relay/ExeMem:** append-only log store; blob put/get (presigned);
optional anti-spam/quota; account service ExeMem-specific.

**Node:** `AcceptEntry`; full-log restore; keep heads + grant state;
bootstrap emit; head compare; birth; incorporation; blob_delete handling.
