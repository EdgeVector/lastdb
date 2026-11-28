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

**Logs are append-only in normal operation.** The one trim is `log.trim` of a
personal database, which drops the entries that a publisher-signed `snapshot`
entry covers, and `blob.drop` then drops the stored files that the newest snapshot
and the entries after it do not name (`relay-protocol.md`). The cloud makes no
snapshot of its own.
File blobs may be reclaimed after a publisher-signed
`blob_delete` chain entry; readers treat a deleted blob as intentionally
gone. Product erase is separate: `manage`/`delete` erases that database’s
relay logs and blobs (the `database_id` stays tombstoned);
`account`/`deactivate` erases everything that publisher has in ExeMem and
on the relay (logs, blobs, account), cancels subscriptions, and retires
the key.

### Acceptance (local node rule)

**Acceptance is a local node rule.** Relays do **not** run `AcceptEntry`.
They may apply a **spam/cost grant gate** from publisher-signed birth/grant
metadata (see `relay-protocol.md`): **401** bad/missing request signature,
**403** `forbidden` when the caller lacks a current grant. That gate cannot
make honest nodes disagree; wrong refuse/withhold is data loss, handled by
`relays` failover. Nodes still verify everything locally. ExeMem account
errors still include `payment_required`, quota/rate limits, `not_found`,
and bad request. There is no `manage`/`access` UX that invents grants.

**Role changes live only in the chain.** Every grant and every role
reduction (including revoke) is a signed `grant`-kind entry on a publisher
device chain. A grant that lowers a role or revokes still carries a
`cutoffs` field for the wire format; nothing reads it. **No side channel** — not
in ExeMem rows, not in People. People may carry **signed heads for
comparison only**. The relay’s grant index (and any ExeMem cache) is
**derived from verified publisher birth/grant entries**, never
authoritative for AcceptEntry. A grant/revoke seen only off-chain **must
be ignored** by nodes. Grantee node key and role are **signed
plaintext metadata** so the relay can gate without decrypting payloads.
Access for merge remains local (no `manage`/`access`).

`AcceptEntry(E, S) → accept | pending | reject` — `S` is the node’s
verified history for `E.database_id`:

1. **Signature.** Verify `E.signature` over `signed_bytes(E)` with
   `E.node_key`.
2. **Database bind.** `E.database_id` matches `S`.
3. **Device chain.** `(E.writer_seq, E.prev_hash)` extends stream
   `(E.node_key, E.device_id)`, or identical `entry_hash` duplicate.
   Missing predecessor → **pending** (buffer).
4. **Seen heads.** `E.seen_heads` is a set of **entry hashes** that may
   name entries from **any** stream (not only publisher devices). Every
   referenced entry must already be **held and accepted** in `S`. If any
   hash is missing → **pending** until fetched; if it never arrives the
   entry is **never applied**.
5. **Order value (`lamport`).** Let `L_prev` be the `lamport` of this
   stream’s previous entry (0 if `writer_seq = 1`), and let `L_seen` be the
   max `lamport` among the accepted entries named by `E.seen_heads` (0 if
   empty). `E.lamport` is a millisecond order value: require
   `E.lamport >= 1 + max(L_prev, L_seen)` and `E.lamport < LAMPORT_LIMIT`
   (`4_102_444_800_000`, `2100-01-01T00:00:00Z`). Else **reject**.
6. **Publisher and lease authorization.** Only the owner (from birth) signs
   the publisher kinds (`birth`, `grant`, `schema`, `blob_delete`, `heads`,
   `relays`, `snapshot`, `lease`). Direct `data` (`stream=direct`) and
   `bundle` entries are signed by the owner, or by a member holding `write`
   covered by an active publisher `lease` cited in `E.seen_heads`. Otherwise a
   non-publisher writer signs a proposal (`stream=proposal`); among
   `E.seen_heads`, the **publisher** entries fold the grant window, requiring
   `submit` or `write` there and in the current grant state.
7. **Bundle body (only if `kind=bundle`).** After steps 1–6 pass, fetch the
   referenced encrypted blob (`blob.get`), verify `blob_sha256` and
   `blob_size`, decrypt under the db key, verify `change_count`, then apply
   the inner **data** changes in order (see Signed bundles). A missing blob,
   hash/size/count mismatch, or decrypt failure → **pending** (not applied);
   the stream **blocks** on that entry until a valid blob is obtained — it
   does **not** freeze the stream (freeze is equivocation only).
8. **On reject:** do not apply; keep evidence. **On pending:** do not apply
   yet. A frozen stream (fork) never advances.

### Local node model

A device keeps **no full history**. It keeps:

| Piece | Purpose |
|---|---|
| Materialized state | Current records (and blob refs) |
| Per-stream heads | For each `(node_key, device_id)`: `(entry_hash, writer_seq, lamport)` |
| Grant state | Folded roles |
| Per-record position | Winning `(lamport, node_key, device_id)` for LWW |
| Log and cursor | Entries this node signed, and how many a relay has acknowledged |

**At WRITE time** the device:

1. Chooses `seen_heads` (hashes of accepted entries it depends on — any
   streams),
2. Assigns `writer_seq`, `prev_hash`, and millisecond order
   `lamport = max(authored_at_ms, 1 + max(own prev lamport, lamports of cited entries))`
   (`< LAMPORT_LIMIT`),
3. Seals `data` and `schema` payloads under the database read key with wire
   `namespace = "sealed"` (packing the real schema name inside the ciphertext
   for shared databases, with AAD bound to
   `database_id || node_key || device_id || writer_seq`), then signs the entry.
   A member's proposal is sealed to the owner's node key instead (see
   "Data / schema / bundle"),
4. **Applies locally** (same `AcceptEntry` + merge rules),
5. Appends the signed entry to the **log**; entries past the upload cursor
   are pending.

Upload sends those signed entries **unchanged**. Relays store/forward;
they do not re-author.

### Record merge (whole-record LWW)

Canonical record state is **whole-record last-writer-wins** by position
`(lamport, node_key, device_id)`. No field merging. A delete is a
**tombstone** that carries a position. Convergence holds for any arrival
order with **no rewind**: once a position wins, an earlier position never
replaces it. For grant entries at equal Lamport, **`revoked` wins**.
Wall-clock timestamps (`authored_at_ms`) are **display-only**.

**Tradeoff:** concurrent edits to different fields of the same record still
collide; one whole record wins. The fix is to **split into smaller
records**, not to field-merge.

### Publisher-signed `heads` (hidden-entry detection)

A publisher device may append a `heads` entry (`lastdb-heads-v1`) listing
tip hashes / `(device_id, writer_seq, entry_hash)` vectors it attests.
Nodes compare these (own devices + People). A tip that cites an entry this
node cannot obtain → **hidden/withheld** (`history.withheld`); stop
advancing past that dependency. This is consistent with `seen_heads`: both
are hash references that must resolve to accepted entries before progress.

### Forks

Two different valid entries at the same `(node_key, device_id, writer_seq)`
**freeze that stream permanently**. There is **no** publisher `resolve`
kind. The device continues under a **new `device_id`** (new stream from
seq 1). Keep both signed entries as evidence (`history.equivocation`).

**Guarantee:** a member's entries change no record until the publisher
signs them in. After a reader has a publisher revoke **in the chain**, the
member's proposals are not accepted. Delayed delivery is liveness; signed
`heads` / tip compare detects it.

**Limit:** relays can delay or stop delivery. They cannot make honest nodes
silently disagree once heads and dependencies meet.

## Minimal signed history

1. Writer signature on every change.
2. One hash chain per stream `(node_key, device_id)`, gapless.
3. Grants / role reductions / revokes on publisher device chains.
4. Local `AcceptEntry` (seen heads by hash from any stream; millisecond order
   `lamport < LAMPORT_LIMIT`) + whole-record LWW merge; grant ties → revoke
   wins.
5. Publisher-signed `heads` for hidden-entry detection (devices + People).
6. Shared databases have no snapshots and never trim logs. The personal
   database publishes native-file `snapshot` entries and trims covered log
   entries (`log.trim`) and unreferenced blobs (`blob.drop`). No `resolve`.
7. Proposals are **`data` entries with `stream = proposal`**; they count
   as content only after publisher **incorporation**; `write` members covered
   by an active publisher `lease` may sign direct `data` entries.
8. Append-only logs in normal operation (with personal snapshot `log.trim`).
9. Signed `bundle` entries for large batches / bootstrap / restore pushes
   (ordinary writes stay single `data` entries).

### Device chains (streams)

- `writer_seq` ≥ 1, +1 each time; `prev_hash` within the stream.
- `entry_hash = SHA-256(signed_bytes || signature_raw_64)`.
- Dedupe `(node_key, device_id, writer_seq)`. Same seq on different devices
  is fine. Equivocation at one stream position freezes that stream.
- Stolen node key may mint a new `device_id` (Decision 21).

### Signed bytes (common header)

Length-prefixed, big-endian. Domain + `0x00`, then:

`database_id`(32) · `node_key`(32) · `device_id`(32, Hex32) · `writer_seq`(u64) ·
`prev_hash`(32) · `authored_at_ms`(u64) · `lamport`(u64) ·
`seen_heads` (u16 count + `entry_hash`(32)×, sorted ascending).

Wire `signature`: base64url, 64 bytes.

| Kind | Domain | Notes |
|---|---|---|
| `birth` | `lastdb-birth-v1` | First publisher-device entry (`writer_seq=1`) |
| `data` | `lastdb-mutation-v1` | `stream` direct\|proposal; wire `namespace="sealed"`, sealed payload; optional file / incorporation |
| `schema` | `lastdb-schema-v1` | publisher; wire `namespace="sealed"`, sealed schema definition |
| `bundle` | `lastdb-bundle-v1` | `{blob_sha256, blob_size, change_count}`; publisher or leased `write` member |
| `grant` | `lastdb-grant-v1` | grantee, role; `cutoffs` carried for the wire format, never read |
| `lease` | `lastdb-lease-v1` | publisher; write lease window and closed stream positions |
| `snapshot` | `lastdb-snapshot-v1` | personal database; manifest digest of native store files and covered stream positions |
| `blob_delete` | `lastdb-blob-delete-v1` | publisher; names blob `sha256` |
| `heads` | `lastdb-heads-v1` | publisher tip vector for hidden-entry detection (and declined proposals) |
| `relays` | `lastdb-relays-v1` | publisher-signed relay URL list |

**Data / schema / bundle:** `data` and `schema` payloads are sealed under the
database read key at write time (`namespace = "sealed"` on the wire). Direct
`data` and bundles are signed by the publisher, or by a `write` member covered
by an active publisher `lease` cited in `seen_heads`. Otherwise a member signs
a proposal (`stream=proposal`), citing the publisher grants it holds in
`seen_heads`; a proposal changes nothing until the publisher signs a direct
entry citing it in `incorporated_from`, or a `heads` entry declining it.
Bundles never carry proposals or grant entries inside the blob.

**Proposals are sealed to the owner.** Every member holds the read key, so a
proposal sealed with it would be readable by every member before the owner
decides it. A member therefore seals a proposal payload to the owner's node key:
the Montgomery form of the owner's Ed25519 key is its X25519 key, the member makes
a fresh X25519 key for each proposal, and HKDF-SHA256 (salt
`lastdb:proposal-owner-seal:v1`, info `database_id || owner_key ||
fresh_public`) gives the AES-256-GCM key. The payload is
`fresh_public(32) || envelope`, with the same AAD as every sealed payload and the
real namespace packed inside. Every owner device holds the node key and opens it;
no member can. The owner declines a proposal that does not open this way
(`lastdb_node/src/owner_seal.rs`). A publisher entry that incorporates a proposal
is sealed with the read key as usual, so members read the change once the owner
accepts it.

**Birth:** first chained entry on a publisher device. ExeMem
`manage`/`create` carries the full signed `BirthEntry`.

### Grant window

A proposal `P` with `seen_heads = H`:

1. From the **publisher** entries among the accepted set named by `H`,
   fold grants in merge order. The proposer's effective role must be
   `submit` or `write`.
2. The proposer's role in the full verified `S` must also be `submit` or
   `write`, so a revoked member cannot propose by citing an older grant.

A role reduction or revoke carries a `cutoffs` field because the signed
bytes include it. Nothing reads it: no member entry changes a record, so
there is nothing to cut off.

### Blob delete

Publisher `blob_delete` names blob `sha256`. Relays may drop those bytes
when no accepted log entry still references that digest. A `blob_delete`
that names a digest still referenced by an accepted `bundle` (or other
accepted entry) is **invalid**; readers **ignore** it. Normal operation
never GC-trims logs; product `delete` / `deactivate` erase storage as
below.

### Signed bundles

A `bundle` is one ordinary chain entry (same `node_key`, `device_id`,
`writer_seq`, `prev_hash`, `lamport`, `seen_heads`, Ed25519 signature) whose
payload is:

| Field | Meaning |
|---|---|
| `blob_sha256` | SHA-256 of the encrypted blob bytes |
| `blob_size` | Encrypted blob length in bytes |
| `change_count` | Number of inner data changes (1…cap) |
| `grant_at_device` / `grant_at_seq` | Member grant binding (omit for publisher) |

The encrypted blob holds many **data** changes in a defined order. Upload
the blob with `blob.put` (presigned; parallel or multipart allowed)
**before** appending the `bundle` entry. Readers fetch with `blob.get`.
No new relay endpoints.

**Inner contents:** only record data changes (puts/tombstones). Grants,
revokes, role reductions, `heads`, `relays`, and `blob_delete` **must not**
appear inside a bundle.

**AcceptEntry (bundle):** run the common checks first (signature, db bind,
chain link, seen heads, lamport, write authority, fork). Then
download, verify sha256 + size, decrypt, verify `change_count`, and apply
inner changes. Every inner change shares the **entry’s** `lamport` (and
the entry’s `node_key` / `device_id`). A bundle **must not** hold two
changes to the same record key. Apply order inside a bundle is ascending
**record key** (byte-wise). Whole-record LWW against other entries still
uses position `(lamport, node_key, device_id)`; the in-bundle record-key
order is only the deterministic apply order among distinct keys. Hash,
size, or count mismatch, decrypt failure, or a missing blob → entry stays
**pending** / unavailable; the stream **blocks** until a valid blob is
obtained (same pending class as missing `seen_heads`; not a freeze). The
relay stays untrusted.

**Caps (protocol constants):** max encrypted blob size **64 MiB**; max
`change_count` **10_000**. Readers **reject** bundles over either cap.

**When to use:** ordinary writes stay single `data` entries. Large restore
pushes, large batch writes, and bootstrap current-state use bundles.
Bundles are append-only and **replace no history**.

**Blob identity / dedupe:** blob keys are scoped per `database_id`.
Encryption uses the db key plus a **per-blob nonce**, so identical
plaintext yields different ciphertext and different digests. There is
**no** cross-entry content dedupe and **no** refcount. A bundle’s blob
must not be removed by `blob_delete` while any accepted log entry
references that digest; such a `blob_delete` is invalid and ignored.

**Deferred (not designed here):** a publisher-signed rebase that replaces
covered history to bound storage remains a future option.

## Restore, new device, and local state

Shared databases have no snapshots. No `cursor_expired`. Delivery cursors are
pagination only.

**Restore / new device / new member (shared database):**

1. Discover a relay (People + `relays` entry).
2. `log.streams` + `log.read` every stream from `writer_seq = 1`.
3. `AcceptEntry` (pending until seen-head deps and bundle blobs arrive);
   LWW merge; replay. Bundle bodies download via `blob.get` when the entry
   is reached (stream blocks on that entry until the blob verifies).
4. Other file blobs **lazily** by hash via `blob.get` (presigned URL).

**Personal database restore / new device:** installs the newest
publisher-signed `snapshot` (native store files named by digest, already
encrypted and never sealed again) when one exists, reads each device stream
after its snapshot position, opens the sealed payloads, and signs on a new
stream under a new device id. See
[personal-database-signed-log.md](personal-database-signed-log.md).

After replay, keep only materialized state, per-stream heads, grant state,
per-record positions, and the outbox — enough to validate and author the
next entry. Relays retain the full log for shared databases, and the newest
snapshot plus trailing entries and referenced blobs for a personal database.
With current-state bundles, bootstrap/restore of a large home is expected to
finish well under a minute for a few hundred MB of synthetic data (see CI plan).

## Bootstrap (existing databases)

No migration, no dual path:

1. Publisher device appends `BirthEntry` (`writer_seq = 1`).
2. Emits current state as one or more signed `bundle` entries (encrypted
   current-state blobs uploaded first), not as a long run of single `data`
   entries.
3. Members get **fresh** `grant` entries (ordinary chain entries, never
   inside a bundle).
4. Old unsigned cloud data is **replaced**, not kept alongside.

## Shape

```
POST /api/cloud/v3/{fetch,push,manage,account}
```

Product: `{database_id, intent, args|result}`. Account: no `database_id`.
Batch 1–90; req ≤256 KiB; resp ≤8 MiB.

| Action | Intents |
|---|---|
| `fetch` | `mutations_since`, `file` |
| `push` | `mutations`, `file` |
| `manage` | `create`, `grant`, `revoke`, `delete` |
| `account` | `register`, `payment`, `deactivate`, `add_device`, `remove_device`, `databases`, `status` |

`manage`/`grant` and `revoke` append signed `grant` entries (transport
sugar). `manage`/`create` = full signed `BirthEntry`. There is **no**
`manage`/`access` — access is derived locally from the chain.
`manage`/`delete` erases that database’s relay logs and blobs; the
`database_id` stays tombstoned.

### Account service

`account`/`deactivate` erases everything that publisher has in ExeMem and
on the relay (logs, blobs, account), cancels subscriptions, and retires
the key. The **app registry** is a separate ExeMem service outside this
API (not designed here).

`fetch`/`file` and relay `blob.get` return **exactly one** download path:
a **presigned URL**. Push result has **no** `landed` field.

## Auth (`nodeSignature`)

Headers: `X-LastDB-Node-Key`, `X-LastDB-Database-Id`, `Issued-At` (unix
seconds), `Nonce`, `Signature`, `X-LastDB-Relay-Protocol: 1`.
`X-LastDB-Device-Id` (Hex32 stream id) is a companion header only; it is not
signed.

Signed lines: `POST\n<path>\n<protocol>\n<node_key>\n<database_id>\n
<body_sha256 hex>\n<issued_at>\n<nonce>`. `relay-protocol.openapi.json`
is authoritative.

Skew ±60s; nonces ≥2× skew. Request signatures authorize HTTP only.

## Database keys (Decision 36)

`db_salt`; HKDF `lastdb-db-key-v1`; `database_id` = db public key; birth
by db key. Owner needs no grant. Tombstones on ExeMem index; no rotation.

## Requirements (Decision 38)

Publisher pays. Lapsed: reads work; push/grant/create →
`payment_required`; delete always allowed.

## Decisions (explicit)

1. Protocol **v3**.
2. Version in header + signature material; no body `version`.
3. **426** `upgrade_required` + `minimum_version` (not on account).
4. **Superseded by 43.** Grants are publisher-signed chain entries.
5. No protocol header on **account**.
6. Live OpenAPI under `cloud_backup_protocol/` when shipped.
7. **Superseded by 45.** snapshot_proof maintenance **removed**.
8. Actions: `fetch` / `push` / `manage` / `account`.
9. **Superseded by 42.** Stream signed on the entry; no server `landed`.
10. Publisher sees direct+proposal; receivers direct only; acceptance =
    publisher incorporation, and a proposal not accepted is cited by a
    publisher `heads` entry. Proposals are flagged `data` entries.
11. Silent rejections — no discard call.
12. **Superseded by 46.** No `manage`/`access`; access from the chain;
    no share-settings / `rules`.
13. **Superseded by 44–45.** No GC trim; blob reclaim via `blob_delete`;
    product erase via `delete` / `deactivate`.
14. **Superseded by 44.** Proposals never deleted by GC.
15. **Superseded by 46.** `manage`/`delete` erases that DB’s relay logs
    and blobs; `database_id` stays tombstoned.
16. Envelope `{database_id?, intent, args|result}`.
17. Batching; caps above; per-item errors.
18. Account surface as listed; no login; no `em_` keys.
19. Node public key + database id in signed material; device id (Hex32) as a companion stream header.
20. Ed25519 `nodeSignature`.
21. No per-device keys; new `device_id` starts a new stream (also the fork
    recovery path).
22. **Superseded by 46.** App registry is a separate ExeMem service
    outside this API.
23. `payment` → short-lived signed `payment_url`.
24. `register` rate-limited per IP.
25. Lose all devices + phrase → account gone.
26. `account`/`databases` discovery; `db_salt` on owned.
27. `account`/`status`: paid, storage, transfer, devices.
28. No node-key rotation.
29. Connect / invites / joins are People-only.
30. No `rules` / share-settings.
31. **Superseded by 43.** Public read = `grant` `any` / `read` only.
32. Device rule + `read.public` rate limit; publisher pays.
33. File upload begin/commit; downloads = presigned GET only.
34. Create = full signed `BirthEntry`.
35. Levels: `read` | `submit` | `write` (+ reductions / `revoked`).
36. Self-certifying `database_id`; tombstones; no db-key rotation.
37. **Superseded by 45.** No shared snapshots; no `cursor_expired`.
38. Publisher pays; lapsed rules as above.
39. **Superseded by 46.** `deactivate`: erase all publisher ExeMem/relay
    data; cancel subs; retire key.
40. Devices: `remove_device` bookkeeping; cap 256.
41. Delivery cursors are pagination only.
42. Writer signatures + device streams; write-time `"sealed"` payloads under
    the db read key; local write model (sign, apply, log + cursor);
    `AcceptEntry` with any-stream `seen_heads` by hash; millisecond order
    `lamport < LAMPORT_LIMIT`; whole-record LWW; no `landed`; no `resolve`.
43. Signed grants; the `cutoffs` field is carried but never read;
    revoke-wins ties; publisher `heads` for hidden-entry
    detection; no side channel.
44. Append-only logs in normal operation (with personal snapshot `log.trim`);
    proposals not GC-deleted; blob delete via chain; product erase via `delete`
    / `deactivate`.
45. Shared DBs have no snapshots (restore = full log replay). Personal DB
    publishes native-file `snapshot` entries, trims covered history via
    `log.trim`, and drops unreferenced blobs via `blob.drop`. Fork freezes the
    stream; continue on a new `device_id`.
46. **Superseded by 48 for relays.** No `manage`/`access`. `delete` erases
    that DB’s relay logs/blobs (id tombstoned). `deactivate` erases all
    publisher ExeMem/relay data, cancels subscriptions, retires the key.
    App registry is outside this API.
47. Signed `bundle` entries (encrypted multi-change blobs via existing
    `blob.put`/`blob.get`); caps 64 MiB / 10_000 changes; missing or bad
    blob → pending/block (not freeze); no in-bundle grants/heads/relays/
    blob_delete; per-db keyed blobs with per-blob nonce (no digest
    dedupe/refcount). Bootstrap = birth + current-state bundles + grants.
48. Relay spam/cost gate from publisher-signed birth/grant entries: every
    request signed; append is the publisher's, or `data` (`proposal` or
    `direct` under a publisher `lease`) from a `submit` or `write` member; put
    needs `write`; read/streams/get need `read` or `grant#any`; revoke cuts off
    new appends (403); already-stored entries stay. Quota billed to the
    publisher’s ExeMem account; grantees need no account. Rejected: shared db
    secret for append auth.

### Defaults

| Topic | Default |
|---|---|
| Freshness | Head / `heads` comparison only |
| Reference relay | In `lastdb` for CI |
| Public reads | `grant#any`/`read`: any **signed** caller may read |
| Relay list | Publisher-signed `relays`; People bootstraps |
| Birth | First chained publisher-device entry |
| Fork recovery | New `device_id` (stream frozen; no `resolve`) |
| Downloads | Presigned URL only |
| Concurrent field edits | Split into smaller records (whole-record LWW) |
| Bundle blob max | 64 MiB encrypted (protocol constant) |
| Bundle change_count max | 10_000 (protocol constant) |
| Bundle blob identity | Per-`database_id` + per-blob nonce (no digest dedupe) |

## Server errors (HTTP)

ExeMem/relay cover auth/signature, payment/quota, `not_found`, and bad
request. Relays additionally return **403** `forbidden` when the caller’s
`node_key` lacks a current grant (spam/cost only — not AcceptEntry).

| HTTP | code | fields |
|---|---|---|
| 400 | `invalid_request` | arg paths, `grants.any.role` (any must be read), `birth_signature`, `cursor` |
| 400 | `batch_too_large` | `batch.response` |
| 401 | `unauthorized` | `request.*`, `device.*`, `account.retired` |
| 403 | `payment_required` | `payment.subscription` (ExeMem) |
| 403 | `forbidden` | relay: no current grant for `node_key` / role |
| 404 | `not_found` | `database.*`, `file_handle`, `blob.sha256` |
| 409 | `conflict` | `database.id`, `account.exists`, `device.*`, `entries.writer_seq`, `file.unverified` |
| 426 | `upgrade_required` | `request.protocol` |
| 429 | `rate_limited` | `account.register`, `read.public`, relay quota (publisher-billed) |
| 500 | `unavailable` | unknown or service Dynamo errors (`provider.dynamo`); retry, same as 429 |

### Node-side signals

`history.fork` / `history.equivocation` (frozen stream), `history.withheld`
(hidden entry via `heads`), Lamport / signature / authorization rejects, **pending**
until `seen_heads` deps arrive — local only.

## CI plan (DEV E2E, not unit tests)

1. Tampered payload: node rejects even if relay stored it.
2. Cross-db / field tamper: reject.
3. Incorporation mismatch: reject.
4. One-stream equivocation: stream **frozen**; continue only under new
   `device_id`.
5. Multi-device offline same seq: **not** equivocation.
6. Forged grant: cannot authorize.
7. Unleased member direct entry or bundle: reject. Proposal after revoke: reject.
8. Side-channel grant/revoke ignored.
9. **Unknown `seen_heads` hash:** entry stays **pending**; never applied if
   the hash never arrives.
10. **Invalid `lamport`** (not above prev and cited entries, or `>= LAMPORT_LIMIT`): **reject**.
11. Reordered delivery → same LWW state; no rewind.
12. Publisher `heads` cites missing tip → withheld detection.
13. Bootstrap replay equals source state; fresh member grants.
14. Restore from start: full replay + lazy blobs (bundle blobs required
    when a `bundle` entry is accepted).
15. **Bundle E2E (DEV):** fresh upload of a few hundred MB of synthetic
    data via one or more `bundle` entries, and restore of that upload,
    each finish in **under one minute**. Docs-only here; implement in CI
    later — not a unit test.
16. **Relay grant gate (DEV):** a non-grantee’s `log.append` / `blob.put`
    is refused (**403**); a revoked member’s new append is refused
    (**403**); previously accepted entries remain stored.

## Build work

**Relay/ExeMem:** append-only log; blob put/get; optional anti-spam;
account service ExeMem-specific.

**Node:** local write path (sign/apply/log); `AcceptEntry` + pending
deps (incl. bundle blobs); LWW; frozen-stream handling; `heads` compare;
bootstrap via bundles; blob_delete (ignore deletes of live bundle digests).
