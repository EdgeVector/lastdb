# LastDB relay protocol (open)

Status: built. The reference relay in this repository and the Exemem relay
(DEV and PROD) serve it. First written as a design on 2026-11-19; `log.streams`,
the `snapshot` entry and `log.trim` were added on 2026-11-22 and 2026-11-23.
Companion to `docs/designs/cloud-api.md`. Wire:
`docs/designs/relay-protocol.openapi.json`. What a node keeps on disk is in
`docs/home-format.md`.

**Goal:** smallest open protocol so anyone can stand up a relay if ExeMem
goes down. Signed data is relay-independent. ExeMem = conforming relay +
account/billing service. No dual paths, fallback, or unit tests.

## Trust

Canonical acceptance is **local** (`AcceptEntry`). Relays store, serve, and
apply a **spam/cost gate** from publisher-signed grant metadata. That gate
is **not** AcceptEntry: the relay does **not** decrypt payloads, check
the order value or `seen_heads`, or run full merge. Nodes still verify everything
locally, so the trust model is unchanged — a relay that lets junk through,
or wrongly refuses, **cannot** make honest nodes disagree. It can only
withhold, which is the same as data loss, already handled by failover
through the publisher-signed `relays` entry.

**Logs are append-only and never trimmed**, except product erase and the one trim
below: the owner of a personal database asks the relay to drop the entries that a
snapshot covers (`log.trim`).
Grants/revokes exist only as signed chain entries. The relay’s grant index
is **derived** from publisher-signed birth/grant entries it has verified
for gating — never a side channel. People carries signed heads for
comparison only.

**The relay makes no snapshot.** It holds entries and blobs, and never a
record. A snapshot is the publisher's: the native files of its
store as blobs, named by a publisher-signed `snapshot` entry (see "The
`snapshot` entry"). The relay stores both like any entry and any blob, and
reads the entry only when a `log.trim` names it.

**What the relay can read.** In a personal database every payload is sealed,
and the relay sees the count, sizes and times of entries. In a shared database
the `data` entries are plain record key and field JSON today, so the relay can
read them (`signed-history-sharing.md`, "What is not built").

File blobs may be dropped after a publisher
`blob_delete` chain entry that does not name a digest still referenced by
an accepted log entry (including `bundle` blobs). Product erase: ExeMem
`manage`/`delete` erases that database’s logs and blobs on the relay
(`database_id` stays tombstoned); `account`/`deactivate` erases all of
that publisher’s relay data.

## Order value

The wire field `lamport` is the order value of an entry: a time in
milliseconds, set when the write was made. A node accepts an entry only if its
value is above the previous entry of the same stream, above every entry it
cites in `seen_heads`, and below `4102444800000` (2100-01-01 in milliseconds).
Nothing else ties the value to the entries before it. An author signs the
larger of its clock and one more than the highest value it has seen, so a
write made after another is always above it. Records resolve by
`(lamport, node_key, device_id)`: the higher position wins. The relay checks
none of this.

## Surface

```
POST /relay/v1/log/append
POST /relay/v1/log/read
POST /relay/v1/log/streams
POST /relay/v1/log/trim
POST /relay/v1/blob/put
POST /relay/v1/blob/get
```

Batching/caps match cloud-api. **Every** relay request is signed
(`nodeSignature` + `X-LastDB-Relay-Protocol: 1`), including reads.

### Request signature (`nodeSignature`)

Minimal signed request (Ed25519 over the node key). Companion headers
carry the fields; the signature covers:

| Field | Role |
|---|---|
| `method` + path | e.g. `POST` and `/relay/v1/log/append` |
| `node_key` | caller’s Ed25519 public key (hex) |
| `database_id` | target database |
| `body_sha256` | SHA-256 of the raw request body |
| `issued_at` | unix seconds (skew ±60s) |
| `nonce` | replay protection (≥2× skew retention) |

Wire lines (big-endian / hex as elsewhere):

```
POST\n<path>\n<protocol>\n<node_key>\n<database_id>\n<body_sha256 hex>\n<issued_at>\n<nonce>
```

Bad or missing signature → **401** `unauthorized`. Device id may still
appear as a companion header for append stream identity; it is **not**
part of the grant gate (grants are by `node_key`).

### Relay-side access gating (from grant entries)

The relay keeps a **per-`database_id` grant index**, updated as it accepts
publisher-signed `birth` / `grant` entries (including role reductions and
revokes) in **chain order**.

**Publisher.** Always allowed. The relay learns the publisher node key from
the `birth` entry, verified against `database_id` (the db public key) and
the birth signature.

**`log.append`.** Accepted only when the signing `node_key` is the
publisher, **or** a publisher-signed grant in that database’s chain
currently gives that key **`submit`** or **`write`**, and no later revoke /
role reduction has removed that authority. A member holding `submit` or
`write` may append proposal `data` (`stream = proposal`) to its own stream. A
member holding `write` may also append direct `data` and `bundle` entries to its
own stream: the relay does not know whether a lease covers them, and the nodes
decide (see "The `lease` entry"). Everything else, the publisher kinds and
`lease` among them, is the publisher’s alone (403 `forbidden`). A **revoke cuts off appends immediately** (new requests →
**403** `forbidden`). Entries the relay already stored before the revoke
**stay**; the relay does **not** retroactively delete them.

**`blob.put`.** Accepted for the publisher, or for a key whose current
grant is **`write`**.

**`log.read` / `log.streams` / `blob.get`.** Require a current **read** grant
for the signing `node_key`, or an active **`grant#any` / `read`**. Meaning of
`grant#any`: **any caller with a valid request signature** may read — not
unsigned/anonymous access. There is no unsigned public path.

**What the relay verifies.** Two things, and no more:

- For gating: the publisher signatures on `birth` and `grant` (including
  revoke / role-reduction), enough to update the grant index.
- For a `log.trim`: the publisher signature on the one `snapshot` entry the
  request names, and that the entry it stores at each position of that
  snapshot has the hash the snapshot names (see `log.trim`).

It does **not** decrypt record payloads or run full `AcceptEntry`. Both checks
decide what the relay stores or drops. Neither decides what counts.

**Readable grant metadata.** Because the relay must gate on them, grant /
revoke / role-reduction entries expose **grantee node key and role**
(and the `cutoffs` field, which nothing reads) as signed plaintext metadata the relay can see (not encrypted
inside the payload).

**Rejected alternative:** a shared database secret for append auth —
revoked members would keep it.

**Bootstrap / failover.** A new or empty relay builds the grant index by
reading the database’s logs from the `birth` entry onward and verifying
publisher-signed birth/grant entries in order. A failover relay rebuilds
the same way from the logs it is given (or fetches from another relay);
until birth+grants are verified, it refuses non-publisher writes and
non-`grant#any` reads.

### `log.append` / `log.read` / `log.streams`

Store key `(database_id, node_key, device_id, writer_seq)`; `device_id` is
Hex32 (64 lowercase hex, 32 bytes in `signed_bytes`), never a UUID. Entries:
`birth`|`data`|`bundle`|`grant`|`schema`|`blob_delete`|`heads`|`relays`|`snapshot`|`lease`.

Read by `writer_key` from `from_seq` (default 1) or opaque delivery cursor
(pagination only; never expires). A read of one device stream from a seq that a
trim dropped (see `log.trim`) answers 410 `trimmed`. Omit `device_id` to receive that writer's
entries across every device; the cursor carries the device. With `device_id`,
read that one device chain. Nodes run `AcceptEntry` on read (pending until
`seen_heads` deps — and for `bundle`, a valid blob — arrive).

**`log.streams`** answers "which device streams does this writer have, and how
far does each reach?" so a node can ask once, then read each stream only from
the entry after the last one it already holds, instead of rereading every
stream from seq 1. An idle sync costs one listing and reads no entries.

Request `{ "database_id", "writer_key" }`. Response
`{ "streams": [ { "device_id", "head_seq", "head_hash" } ] }`, sorted by
`device_id` (ascending, lowercase hex). `head_seq` is the **contiguous head**:
the highest `writer_seq` such that every sequence number from 1 up to it is
stored on that stream. **No relay refuses an entry beyond a gap**: this is the
one rule about gaps, for the reference relay and for Exemem. A stream
can hold seq 1, 2 and 5; its head is 2, and seq 5 counts only once 3 and 4 are
stored. A stream is therefore gapless up to its head by the definition of the
head, and an entry that continues the head advances it. `head_hash` is the entry hash (`mutation_log::entry_hash`, 64 lowercase
hex) of the entry at `head_seq`. A stream whose seq 1 is not stored is not
listed. A writer with no stream gets `{ "streams": [] }`, not an error.

**Access is exactly the access of an all-devices `log.read` of the same
`writer_key`**, decided by the same check: the publisher, a current grant, or an
active `grant#any`/`read` may list; a caller with no grant gets 403
`forbidden`; a revoked member gets 403 `forbidden` (a revoked member may still
read the publisher's single device stream, but never lists). The relay is still
a mailbox: the listing is derived from what it stores and says nothing about
whether the entries are valid.

Contract for another relay (ExeMem): same route, same signed request, same
body and response; errors as in the table below (401 bad signature, 403 no
current read grant or revoked, 400 `invalid_request` for a malformed
`database_id` or `writer_key`, 426 protocol); the heads must be contiguous
heads as defined here, and answering must not require reading every entry.

### `log.trim`

The owner of a **personal database** (every data and schema entry carries the
namespace `sealed`, one owner, no grants) asks the relay to drop what a snapshot
covers, so the relay does not keep a history that a snapshot already holds.

Request `{ "database_id", "device_id", "writer_seq" }`: the snapshot stream and the
sequence number of one `snapshot` entry on it. Response
`{ "done", "entries_deleted" }` (Exemem adds `released_bytes`).
A call does a bounded amount of work and answers `done: false` when more is left;
the caller repeats. A repeat is safe at any point.

- **Who.** Only the publisher's node key (403 `forbidden`, field `publisher`).
- **What the relay trusts.** Nothing in the request but the name of the entry. It reads
  that stored entry, checks that it is a `snapshot` entry of the publisher at that
  position, verifies its signature, and takes every position from its `through`
  list. The entry stored for a position must hash to the `entry_hash` that the
  snapshot names, or the call fails with 400 `through` and drops nothing from that
  stream. A snapshot that names a position beyond what the relay holds fails the
  same way.
- **What goes.** For each stream in `through`: the entries from the first stored one
  up to the position. The snapshot entries before the named one. **Nothing else: a
  trim never drops a blob.** The manifests and files of a dropped snapshot entry stay
  on the relay (and stay billed); collecting them is a later step.
- **What stays.** The per-stream head (so `log.streams` is unchanged and the next
  entry in order still advances the head; the relay keeps the last dropped
  sequence number and its hash as a floor), the `birth` entry, the named snapshot and the newer ones with their
  files, and every entry after a position.
- **Only a personal database.** An entry that would go and is not a sealed `data` or
  `schema` entry (or, on the snapshot stream, a `snapshot` entry) stops the call
  with 403 `forbidden`, field `namespace`. A shared database, whose members read
  the publisher's streams from the start, is never trimmed.
- **A reader behind the trim.** A `log.read` of one device stream that starts at or
  below the floor answers 410 `trimmed` (field
  `from_seq`). The node does not read around it: it reports that it is behind the
  history the relay dropped and that the database must be restored into a new
  home. A read that starts above the floor, a restore from a retained snapshot, and a
  device that is already past the positions see no difference. A read of all devices
  of a writer returns the entries that are stored.
- **A late repeat of an append.** `log.append` of an entry at or below a stream's floor is
  answered as a duplicate and stored nowhere, and not charged: the floor is part of append
  admission (Exemem: a condition in the same transaction as the write; the reference relay:
  the entry is taken back when the floor is found at or above it). The entry was on the
  relay, because a floor is only set from stored entries. A read there is 410 whether or not
  an item exists.
- **Billing (Exemem).** The stored-bytes counter of the publisher goes down by the
  stored length of exactly the entries deleted, in the same transaction
  as each delete, so a repeated or concurrent call subtracts nothing twice.
- **Old relays and old nodes.** A relay without the route answers 404, which the node
  logs and ignores. A node that never calls it leaves the relay as it was.

The node calls it once after each snapshot it publishes, naming the **second newest**
snapshot, so the relay always keeps the two newest snapshots with everything after
the older one's positions. See `signed-log-on-local-machinery.md`, section 14.

### The `snapshot` entry

A `snapshot` entry names a checkpoint of a database: the native files of the
publisher's store at one write boundary, and the stream positions they hold. A
node that installs the files reads each stream after its position instead of
from the first entry. The relay stores the entry like any publisher entry and
reads nothing in it. The files and the manifest are blobs (`blob.put` before
the entry is appended).

- **Kind string:** `snapshot` (JSON `"kind":"snapshot"`, `"namespace":"snapshot"`).
- **Who may sign it:** the publisher only (`is_publisher_kind`), like `heads` and
  `relays`. The personal database puts its snapshot entries on a stream of their
  own, whose `device_id` is derived from the node seed
  (HKDF-SHA256, `info` = `lastdb:personal-database:v1:snapshot-device`), so every
  device of the owner signs snapshots on the one stream and no data entry shares it.
- **JSON body fields** (beside the common header: `database_id`, `node_key`,
  `device_id`, `writer_seq`, `prev_hash`, `authored_at_ms`, `lamport`,
  `seen_heads`, `signature`): `manifest_sha256` (64 hex, the SHA-256 of the
  manifest blob), `manifest_bytes` (integer, its length, at most 16 MiB),
  `through` (array, at most 1024, strictly ascending by `device_id`) of
  `{ "device_id", "writer_seq", "entry_hash", "order" }`: the last entry of each
  stream that the snapshot holds, its hash, and its order value (`lamport`).
- **Signed bytes:** the common header (domain `lastdb-snapshot-v1` + `0x00`, then
  `database_id`(32) `node_key`(32) `device_id`(32) `writer_seq`(u64)
  `prev_hash`(32) `authored_at_ms`(u64) `lamport`(u64) `seen_heads`(u16 + hashes
  ascending)), then `manifest_sha256`(32) `manifest_bytes`(u64) `through` count
  (u16) and for each position `device_id`(32) `writer_seq`(u64) `entry_hash`(32)
  `order`(u64). All integers big-endian. `entry_hash = SHA-256(signed_bytes ‖
  signature_raw_64)` as for every kind.
- **Order value:** the normal rule, and above every `order` in `through`. A
  personal snapshot entry cites only the birth in `seen_heads`.
- **The manifest** is JSON, `{ "upload_id": "<uuid>", "files": [ { "path",
  "bytes", "sha256" } ] }`: the files under the data directory
  (`laststore-pack-node-v1`, and `disk-run/` and `disk-cas/` with `CURRENT`,
  `laststore-run-home-v2` and `runs/<uuid>.ldrun`). Each file is a blob named by
  its own SHA-256. The files are encrypted at rest by the store, so they travel
  as they are. The relay sees the paths, lengths and digests in the manifest.
- **A relay must:** accept the kind from the publisher, derive the stream head
  from it like any entry, and accept `blob.put` of the files. An entry parser that
  refuses unknown kinds (a 400 on `entries.kind`) must learn this kind, and a
  parser that deserializes each entry into `mutation_log::SignedEntry` must be
  built against a `mutation_log` that has `SignedEntry::Snapshot`.
- **Blob size.** One native file is one blob. The protocol sets no cap. Run files
  grow as the store compacts, and the node moves each file in one `blob.put` and
  one `blob.get`, streamed, with no resume: a failed transfer restarts that file.

### The `lease` entry

A `lease` entry is the publisher's statement that every key holding `write` in
the grant entries before it may sign direct entries until a time. The design is
`write-leases.md`. A relay stores it like any publisher entry and reads nothing in
it; only the publisher may append it (403 `forbidden` for anyone else).

- **Kind string:** `lease` (JSON `"kind":"lease"`, `"namespace":"lease"`).
- **JSON body fields** (beside the common header): `expires_at_ms` (a covered
  member signs direct entries only before it, by its own clock), `closes_at_ms`
  (above `expires_at_ms`; a lease entry signed at or after it closes the lease),
  `closed` (array, at most 65535, strictly ascending by `node_key` then
  `device_id`) of `{ "node_key", "device_id", "writer_seq" }`: for the earlier
  leases that this entry closes, per device stream that wrote under them, the last
  entry the publisher had accepted.
- **Signed bytes:** the common header (domain `lastdb-lease-v1` + `0x00`), then
  `expires_at_ms`(u64) `closes_at_ms`(u64) `closed` count (u16) and for each
  stream `node_key`(32) `device_id`(32) `writer_seq`(u64). All integers
  big-endian.
- **Order value:** the normal rule. The publisher cites the lease it renews in
  `seen_heads` where it can; a node reads the grants before the lease in position
  order `(lamport, node_key, device_id)`.
- **A member's direct entry** names its lease by citing exactly one `lease` entry in
  `seen_heads`. A node accepts it into the stream when the member held `write`
  as of that lease. It counts unless the lease is closed and the entry lies past
  the closing point; the rule is in `write-leases.md` section 5 and lives in
  `mutation_log` only. `grant_at_device` and `grant_at_seq` stay absent on such an
  entry.
- **A relay must:** accept the kind from the publisher only, derive the stream head
  from it like any entry, and accept a `write` member's direct data and bundle
  entries on its own stream. A parser that refuses unknown kinds must learn this kind.
- **Bundles:** a node takes a bundle from the publisher only. A member's
  bundle entry is stored by the relay and ignored by nodes.

### `blob.put` / `blob.get`

Blob keys are scoped per `database_id`, content-addressed by sha256 of
the stored (encrypted) bytes. Bundle writers `blob.put` the encrypted
bundle body (presigned; parallel or multipart allowed) **before**
`log.append` of the `bundle` entry. **`blob.get` returns exactly one
download path: a presigned URL** (plus headers, expiry, bytes, sha256).
After a verified, valid `blob_delete` for that digest, get may return
`not_found`. No new endpoints for bundles.

**Large files.** The protocol sets no blob size cap (the 64 MiB bundle cap is
node-enforced), and a blob moves through the presigned URLs, so neither side
needs the whole file in memory. A client streams the file from disk to the PUT
URL and streams the GET response to disk, hashing as the bytes pass; the
reference relay does the same on its side (it hashes the upload as it writes
the staging file, and `blob.put` commit is a rename). Timeouts for such
transfers must scale with the size, not use the 30-second request timeout.

**Who verifies the upload.** The relay does not read a large upload to check it. The
`blob.put` begin answer names the presigned PUT in `upload` with `headers` that include
`x-amz-checksum-sha256`, the base64 (standard alphabet, padded) of the 32 bytes of the blob's
SHA-256. The client sends exactly the headers returned, except that it sets `content-length`
itself (a returned `content-length` equals the `bytes` it declared and must not be sent twice).
The object store, or the reference relay, refuses a body of another length or digest with HTTP
400 and refuses a request that leaves the header out. The commit step confirms the length and
the recorded checksum and reads no object bytes. The signed log entry still names the blob by
its SHA-256 only, never by a URL, and a blob the relay already holds answers begin with HTTP 200
and no `upload`. Exemem's store is Backblaze B2, which enforces the signed header on a presigned
single PUT and returns it on HeadObject (probed on the DEV bucket, 2026-11-23).

**A relay's limit on one blob.** A relay may refuse a blob that is larger than it accepts. It
answers the `blob.put` begin (before any upload) with HTTP 400, code `invalid_request`, field
`bytes`. The Exemem relay does this for a blob over 5 000 000 000 bytes (one presigned single PUT;
a larger object would need multipart, which the protocol does not define). A relay that sets a limit does not
have to name it. The reference relay has no limit by default; `reference_relay --max-blob-bytes N`
sets one and gives the same answer, so a client can be proved against it. A client that meets
this answer for a file with a length above zero must not retry in a tight loop, and must say
which file and how large it is. The node does this for snapshot files (see
`signed-log-on-local-machinery.md`, section 12) and does not split a file.

**Dedupe:** encryption with the db key plus a per-blob nonce means
identical plaintext yields different digests — no cross-entry digest
dedupe and no refcount. A `blob_delete` for a digest still referenced by
an accepted `bundle` (or other entry) is invalid; nodes ignore it.

## v3 map

| v3 | Relay |
|---|---|
| `push`/`mutations`, `fetch`/`mutations_since` | `log.*` |
| `push`/`file`, `fetch`/`file` | `blob.*` |
| `manage`/`grant`, `revoke` | out — `log.append` of signed grants |
| `manage`/`create` | `log.append` of signed `BirthEntry` |
| `manage`/`delete` | Erase that DB’s relay logs + blobs; id tombstoned |
| `account`/`deactivate` | Erase all publisher relay data (ExeMem-driven) |
| `account/*` (other) | not protocol |
| former `manage`/`access`, `retention` | **removed** |
| former `snapshot` | the `snapshot` entry and the blobs it names |

## Discovery and failover

Publisher-signed `relays` entry; People bootstraps first URL. Nodes may
append and read across several relays; compare publisher-signed `heads`
(and tip hashes); switch with no migration. A relay that withholds an
entry cited by `seen_heads` or `heads` is detected when the dependency
never arrives. The `snapshot` entry plays no part in that. Equivocation freezes that stream;
recovery is a new `device_id`, not a relay feature. Failover relays rebuild
the grant index from verified publisher birth/grant entries in the logs.

## Defaults

| Topic | Default |
|---|---|
| Reference relay | In `lastdb` for CI; writes one stderr line per request (route, status, entries returned by `log/read`) |
| Public `grant#any` reads | Any **signed** caller may read when `grant#any`/`read` is active |
| Relay list | Publisher-signed `relays` + People |
| Log GC | Never, except product erase via delete/deactivate and `log.trim` of a personal database |
| Blob GC | After valid chain `blob_delete`, or product erase |
| Bundle blob max | 64 MiB / 10_000 changes (node-enforced) |
| Download | Presigned URL only |
| Fork recovery | New `device_id` (frozen stream; no resolve) |
| Accounts | ExeMem: publisher account for quota/billing only; grantees need none. Pure relays need no accounts. |
| Grant checks | Per-db grant index from publisher birth/grant entries (spam/cost only) |
| Quota | Charged to the database’s **publisher** (publisher pays) |

## Errors

| HTTP | code | notes |
|---|---|---|
| 400 | `invalid_request` | arg paths; `grants.any.role` (any must be read); signatures |
| 400 | `batch_too_large` | batch caps |
| 401 | `unauthorized` | bad or missing request signature |
| 403 | `forbidden` | no current grant for this `node_key` / role |
| 404 | `not_found` | missing log/blob (incl. after `blob_delete`); `log.trim` names no stored snapshot entry |
| 410 | `trimmed` | `log.read` starts at an entry that `log.trim` dropped |
| 426 | `upgrade_required` | `request.protocol` |
| 429 | `rate_limited` | quota / rate limits (publisher-billed) |
| 500 | `unavailable` | unknown or service Dynamo errors; retry (same as 429) |

`payment_required` when ExeMem billing refuses the publisher. Relay grant
refusals use **403** `forbidden` (spam/cost gate), not AcceptEntry. A 500
`unavailable` is not a chain conflict and is not final.

**What a client does with a refusal.** A 429 carries a `field`. A field that names a provider
(`provider.*`: `provider.dynamo` throttled, `provider.dynamo.conflict` the transaction of this
request collided with another), `log.streams.head` and `log.streams.backfill` (progress is stored,
ask again), or a read that failed (`mutation_log.read`, `quota.read`, `payment.read`,
`payment.clock`) means the same request can succeed a moment later. A field that ends in
`.configuration` and every other field, among them `quota.bytes`, is final: the client reports the
relay's message at once. A 500 `unavailable` and a 502, 503 or 504 from the gateway are retried the
same way. The node retries at most three times, after 250, 500 and 1000 ms, each cut to a random
value between half and all of it, so a call adds at most 1.75 s of waiting. The relay consumes the
`nonce` of a request it refuses, so every retry is signed again with a new nonce and time. A retried
`log.append` is a duplicate the relay already counts, and a second `blob.put` commit of the same
digest and length answers like the first.

## CI conformance

Same scenarios as `cloud-api.md` against ExeMem and the in-repo reference
relay, including failover, fork/withhold, bootstrap via bundles, restore-
from-start, the bundle few-hundred-MB under-a-minute upload/restore check,
**non-grantee append refused (403)**, and **revoked member append refused
(403)**. Account paths ExeMem-only.
