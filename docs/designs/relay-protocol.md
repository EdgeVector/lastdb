# LastDB relay protocol (open)

Status: built. The reference relay in this repository and the Exemem relay
(DEV and PROD) serve it. First written as a design on 2026-11-19; `log.streams`,
the `snapshot` entry and `log.trim` were added on 2026-11-22 and 2026-11-23, `blob.list`
and `blob.drop` on 2026-11-24.
Companion to `docs/designs/cloud-api.md`. What a node keeps on disk is in
`docs/home-format.md`.

**The wire has one definition: the `relay_protocol` crate** (Tom, 2026-11-26). Its types
are every route, request, answer, header, error code and the signed message. The node, the
reference relay and the Exemem relay use only those types; none of them writes its own copy
of a request or answer shape. `docs/designs/relay-protocol.openapi.json` is generated from
the crate (`cargo run -p relay_protocol --features schema --bin relay-protocol-openapi`)
and is never edited by hand. This document says what the routes do; the crate says what
they carry. See "Contract rule" below.

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
POST /relay/v1/blob/list
POST /relay/v1/blob/drop
```

Batching/caps match cloud-api. **Every** relay request is signed
(`nodeSignature` + `X-LastDB-Relay-Protocol: 1`), including reads.
A node also sends `User-Agent: lastdb/<build version>` (`relay_protocol::user_agent`). It is not
signed and grants nothing; a relay logs it with every refusal and failure, so a failure names the
version that caused it.

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
`write` may append `data` entries (`stream = proposal` or `stream = direct`) to
its own stream. The relay is a mailbox: it does not know whether a lease covers a
direct entry, and the nodes decide (see "The `lease` entry"). A direct entry that
no lease covers, such as one from a writer later reduced to `submit`, is stored
and ignored, and it does not stop the stream. A member holding `write` may also
append `bundle` entries to its own stream. Everything else, the publisher kinds and
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
`{ "done", "entries_deleted", "released_bytes" }`: how many entries this call dropped and the length of their stored copies. The node keeps both in its account of the relay (`log-sync/<database>/cloud-usage.json`); a trim also drops the entries of the owner's other devices, so the sum is the relay's own account of what it released. The account has a start (`since_ms`) and says `complete: false` on a home that uploaded before it began: nothing is backfilled, and its totals are then only what it put since.
A call does a bounded amount of work and answers `done: false` when more is left;
the caller repeats. A repeat is safe at any point. A call that dropped entries always
says so: a failure after some entries went answers 200 with `done: false` and the counts
so far, never an error that loses them. The caller adds the counts of every 200 answer.

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
  trim never drops a blob.** The manifest and the files of a dropped snapshot entry,
  and the files of dropped entries, go with `blob.drop`, which the node calls after
  the trim (see `blob.list` / `blob.drop`).
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
- **Old nodes.** A node that never calls it leaves the relay as it was.

The node calls it after each snapshot it publishes, naming **that snapshot**, so the
relay keeps one snapshot with everything after its positions. A device that has not
read up to those positions when the snapshot is named is behind the trim. See
`signed-log-on-local-machinery.md`, sections 14 and 16.

### Log segments

A personal database's node sends the entries of its device stream as **segments**: the
bytes of a run of consecutive entries live in one blob, and the relay keeps one small
item that names the blob. The entries are the same signed entries, one per write, signed
when the write commits; only where their bytes are stored changes. A segment keeps entry
bytes out of the relay's item store (Exemem: one DynamoDB partition per database, 1,000
write units a second, one unit per KB, doubled in a transaction). Tom approved it on
2026-11-25 (`design-lastdb-relay-log-segments-20261125`). The publisher may send
segments on any stream of its own; the node sends them for a personal database only.
Shared databases keep inline entries.

**The blob.** The signed entries `first_seq` through `last_seq` of one device stream, in
order, each as its JSON (the form `log.append` takes) followed by one `\n`. Nothing else.
It is not sealed again: each body is already sealed. It goes up through `blob.put` like
any file, before the append that names it, and is charged like any blob.

**`log.append` of a segment.** The body carries `segment` in place of `entries`:

```json
{
  "database_id": "<hex64>",
  "segment": {
    "device_id": "<hex64>",
    "first_seq": 101,
    "last_seq": 190,
    "last_hash": "<hex64 entry hash of entry 190>",
    "sha256": "<hex64 of the blob>",
    "bytes": 3964021
  }
}
```

The writer is the caller's node key. One of `entries` and `segment`, never both (400
`invalid_request`, field `segment`). The answer is the answer of an entry append, counted
in entries: `{ "stored": last_seq - first_seq + 1 }`, with `"duplicate": true` for a
repeat.

- **Who.** Only the publisher (403 `forbidden`, field `publisher`).
- **Fields.** `first_seq` at least 1, `last_seq` at least `first_seq`, `last_hash` and
  `sha256` 64 lowercase hex, `bytes` at least 1 and at most the relay's largest blob (400
  `invalid_request`, field `segment.<name>`). An unknown field is 400, field `segment`.
- **The blob.** `sha256` must be committed with exactly `bytes`, and not dropped (404
  `not_found`, field `segment.sha256`). The node then uploads it again and repeats the
  append. On Exemem the check is a condition in the transaction that stores the item, so a
  `blob.drop` cannot remove the blob between the check and the item.
- **Snapshot entries stay inline.** `log.trim` and `blob.drop` read the named snapshot
  entry by its key, so the snapshot stream holds entries only. A segment there is refused
  by the trim (403, field `namespace`).
- **A repeat.** When the head of the stream is at or past `last_seq`, the append is a
  repeat: answered as a duplicate, nothing stored, nothing charged. The same holds at or
  below the floor.
- **In order only.** Otherwise the head must be exactly `first_seq - 1` (no head and
  `first_seq` 1 counts), and the floor below `first_seq`. Anything else is 409
  `conflict`, field `segment.first_seq`. A segment is never stored beyond a gap, and never
  over an entry stored beyond the gap it would fill (the same 409).
- **An inline repeat.** An inline `log.append` of an entry that a stored segment holds (at
  or below the head, or at the segment's `last_seq`) is a duplicate: nothing stored.
- **What it stores.** One item for the stream at `last_seq`, holding `first_seq`,
  `last_seq`, `last_hash`, `sha256` and `bytes`, and the head moves to `last_seq` and
  `last_hash` in the same step. The item itself is not charged; its blob was.

The relay cannot hash entries it does not hold, so `last_hash` is the publisher's
statement, as the blob's content is. A reader checks both (below). A false statement can
only damage the publisher's own database.

**`log.read`.** A stored segment appears in `entries` in its place in the stream, as an
object with one field:

```json
{
  "segment": {
    "node_key": "<hex64>", "device_id": "<hex64>",
    "first_seq": 101, "last_seq": 190, "last_hash": "<hex64>",
    "sha256": "<hex64>", "bytes": 3964021,
    "url": "<presigned GET>", "headers": {}, "expires_at_ms": 1764000000000
  }
}
```

`url`, `headers` and `expires_at_ms` are the answer `blob.get` gives for that digest. A
segment counts as one item toward `limit`. A read from `from_seq` starts with the segment
that holds `from_seq` when one does, so the first segment of a page can start below
`from_seq`; later items continue it without a gap. A cursor after a segment names
`last_seq + 1`.

The reader downloads the blob, checks its SHA-256 and length, and checks that it holds
exactly the entries `first_seq` through `last_seq` of that stream, in order, and that the
hash of the last is `last_hash`. A mismatch is an error, as a bad entry is. It then takes
the entries from `from_seq` on and checks them like entries read inline (signature,
chain).

**`log.trim`.** A segment is dropped whole. For a stream position `p`, the trim drops every
stored item whose last sequence number is at or below `p`: entries, and segments whose
`last_seq` is at or below `p`. The floor becomes the last sequence number and hash of the
last item it dropped. The hash the snapshot names is checked against the item that ends at
`p`; when `p` falls inside a segment, that segment stays, no hash is checked for it, and the
floor stops at the item before it. A segment is a sealed item: it may go wherever a sealed
`data` entry may. `entries_deleted` counts the entries the dropped segments held;
`released_bytes` counts only inline entries, because a segment's bytes are its blob's.

**`blob.drop`.** A segment's blob is needed while its item is stored. The node reads the
items after the newest snapshot and names their blobs as needed, like the files their
entries name. After a trim drops an item, its blob is unnamed and the next sweep drops it.

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
  `seen_heads`. A node accepts it into the stream whatever the
  member's role, so a stream never stops at it. It counts only when the member held
  `write` as of that lease and the lease is not closed before the entry; the rule is in `write-leases.md` section 5 and lives in
  `mutation_log` only. `grant_at_device` and `grant_at_seq` stay absent on such an
  entry.
- **A relay must:** accept the kind from the publisher only, derive the stream head
  from it like any entry, and accept a `write` or `submit` member's data entries (proposal and
  direct) and a `write` member's bundle entries on its own stream. A parser that refuses unknown kinds must learn this kind.
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

### `blob.list` / `blob.drop`

After a snapshot is named and trimmed, the relay of a personal database holds the
files and the manifest of the newest snapshot, the entries after its positions, and
the sealed files those entries name. Every other stored file is dropped and its bytes
are released. The relay owns what is stored and what is charged. The publisher's
node owns what is needed. The relay reads no manifest and no file: it drops only the
digests the publisher names, and only while the streams it was told about are as it
was told.

**`blob.list`.** Request `{ "database_id", "after"? }`, from any node that may read
the database. Response `{ "blobs": [{ "sha256", "bytes" }], "next"? }`: the committed
blobs, ascending by digest, at most 1,000 in a page. `next` is present when more
follow; pass it as `after`. This listing is the truth for "is this file on the
relay": the node uploads a snapshot file only when the listing lacks its digest and
length, and asks again after it appended the `snapshot` entry.

**`blob.drop`.** Request

```json
{
  "database_id": "<hex64>",
  "snapshot": {"device_id": "<hex64>", "writer_seq": 7},
  "heads": [{"device_id": "<hex64>", "writer_seq": 99987}],
  "sha256": ["<hex64>", "..."]
}
```

- **Who.** Only the publisher of a personal database, like `log.trim` (403
  `forbidden`).
- **`snapshot`** names the head of the publisher's snapshot stream. The relay reads
  that stored entry, checks its signature and kind, and refuses the call when a listed
  digest is the manifest that entry names (400 `invalid_request`, field `sha256`).
- **`heads`** are the stream heads the node saw when it computed the list: every
  stream of the publisher that `log.streams` listed, the snapshot stream not among
  them. At most 32, each stream once, each at an entry (400 field `heads` otherwise).
  The list is empty only when the publisher has no other stream. A stream of the
  publisher that the call names neither as the snapshot stream nor in `heads` is
  refused like a head that moved (409 `heads`, field `heads`).
- **`sha256`**: 1 to 25 digests, each once (400 field `sha256` otherwise).
- **One step.** The relay reads the head of the snapshot stream and of each listed
  stream once. No head, another head than named, or an entry stored above the head
  (an entry beyond a gap, which a reader of the stream cannot see and which may name a
  file) answers **HTTP 409, code `heads`**, field `snapshot` for the snapshot stream
  and `heads` for a listed one, and nothing is dropped. The same answer, with field
  `heads`, goes to a call while the publisher has a stream in the database that the
  call does not name: a device that sent its first entry after the caller listed the
  streams. Then, in one atomic step:
  every one of those streams is still as read (its head, and a count of the entries
  stored beyond a gap, which changes when such an entry is stored), each blob record
  that exists is deleted with its pending record, and the summed bytes are released
  from the publisher's stored bytes once. A digest with no committed record is counted
  as `absent`, is not an error, and its pending record is left alone.
- **After the step** the relay deletes the stored objects for good (every version; a
  hide marker is not a delete). An object that could not be deleted is logged; it is no
  longer charged.
- Response `{ "dropped", "released_bytes", "absent" }`.

**Why the heads are a condition.** The one danger of a drop is a file that is
committed and not yet named: a device uploads the sealed file, then appends the
entry. Two rules close it without a clock. The device that names a file asks for it
again after it named it (`blob.put` begin answers at once for a committed digest) and
uploads it when it is gone; the same holds for the files of a snapshot after its
`snapshot` entry is appended. And the drop is one step with the heads: an entry
appended before the drop moved a head, so the drop does nothing, and an entry appended
after it finds its file gone and puts it back. A stream that did not exist when the
node listed the streams has no head in the call. Its first entry makes it a stream the
call does not name, so a drop after that entry is refused, and a drop before it is
covered by the first rule.

**Counting what a drop released.** One call is one step, so a client that asks again
after a lost answer gets `absent` for the digests its first try dropped. The node
does not count from such an answer: it counts a digest it named as released when the
next `blob.list` no longer has it, with the length the earlier listing gave.

**`blob.put` and the pending record.** A relay that keeps a record of an upload that
began keeps it from begin to commit only: begin writes it only while the digest is not
committed, and the commit deletes it in the same step that records the blob.

**The reference relay** serves both routes with the same answers. It keeps no stored
bytes counter. A drop holds the lock that `log.append` holds for its whole call, so
the comparison of the heads and the deletes are one step. It deletes the stored file
of a digest when no other database owns that digest.

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

## Sizes

The protocol sets no size limit of its own. The hosted relay (Exemem) runs on AWS
Lambda, so its platform limits are the protocol's: one request and one response are
each at most 6 MB (6 × 1024 × 1024 bytes), and one stored entry is one DynamoDB item,
at most 400 KB. The reference relay holds requests and responses to the same 6 MB, so
CI sees what the hosted relay does.

- A sender sizes each `log.append` call to fit the request limit (the node sends at
  most 3 MiB per call).
- A `log.read` page stops before its response would pass 6 MB and returns a cursor at
  the next entry, even when it holds fewer entries than `limit`. A reader follows the
  cursor (or the next `from_seq`) and never takes a short page as the end of a stream.
- Blob bytes move through presigned URLs and are not bound by these limits (see
  "Large files").

Until 2026-11-25 the Exemem relay refused any request body over 350,000 bytes. No
platform set that number. Once brain text moved into ~44 KB entries, every full
`log.append` was refused and personal sync stopped. Tom: "Don't set arbitrary limits."

## Defaults

| Topic | Default |
|---|---|
| Reference relay | In `lastdb` for CI; writes one stderr line per request (route, status, entries returned by `log/read`) |
| Public `grant#any` reads | Any **signed** caller may read when `grant#any`/`read` is active |
| Relay list | Publisher-signed `relays` + People |
| Log GC | Never, except product erase via delete/deactivate and `log.trim` of a personal database |
| Blob GC | After valid chain `blob_delete`, product erase, or `blob.drop` of a personal database (the files the newest snapshot and the entries after it do not name) |
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
| 403 | `payment_required` | ExeMem billing refuses the publisher (field `payment.subscription`) |
| 404 | `not_found` | missing log/blob (incl. after `blob_delete`); `blob.put` commit of a blob not uploaded; `log.read` of a segment whose blob is gone (`segment.sha256`); `log.trim` names no stored snapshot entry |
| 409 | `conflict` | a segment or entry out of order (`segment.first_seq`, `entries.writer_seq`); `blob.put` commit that differs from the stored blob |
| 409 | `heads` | `blob.drop`: a stream is not as the caller named it, or the caller did not name it (field `snapshot` or `heads`); nothing was dropped, compute the list again |
| 410 | `trimmed` | `log.read` starts at an entry that `log.trim` dropped |
| 426 | `upgrade_required` | `request.protocol` |
| 429 | `rate_limited` | quota / rate limits (publisher-billed) |
| 500 | `unavailable` | unknown or service Dynamo errors; retry (same as 429) |

Relay grant refusals use **403** `forbidden` (spam/cost gate), not AcceptEntry. A 500
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

## Contract rule

Within protocol `1` (`/relay/v1/`) a change may only add: a new route, a new optional
request field, a new answer field, a new error code. A removed or renamed field, a field
made required, a changed type or a removed route is a breaking change and needs protocol
`2` under `/relay/v2/`, served beside v1 until no client sends v1.

- Answer types accept fields they do not know, so an older node reads a newer relay's
  answer. Request types refuse fields they do not know (`deny_unknown_fields`), so a relay
  never silently drops what a newer node asked for.
- Every change to the contract raises its revision, `relay_protocol::REVISION`. The OpenAPI
  `info.version` is `<protocol>.<revision>` (for example `1.1`), and every answer of a relay,
  a refusal included, names the revision it serves in `X-LastDB-Relay-Revision`. A relay from
  before revisions sends no header; a node reads that as revision 0.
- A node never assumes the relay is new enough. An addition a node uses only where the relay
  serves it is a `relay_protocol::Feature`, named once with the revision it arrived in; the
  node asks `relay_protocol::supports(revision, feature)` per relay. When the relay is too old,
  the node leaves out the optional field, or holds that one operation with a status that says
  the relay must be upgraded. There is no older fallback path and no release-time check that a
  LastDB release is not ahead of the deployed relay. Nothing chooses on a `User-Agent`.
- An addition that every relay in use already serves needs no `Feature`: the relay adds first,
  as before.
- `ci-required` job `relay_contract` regenerates the OpenAPI file and fails when the
  committed file differs, fails when the contract changed but its revision did not rise, then
  runs a pinned `oasdiff breaking` against the base commit's
  file and fails on any WARN or ERR. The label `relay-contract-break-approved` lets a
  reviewed break through (removing v1 once v2 serves every client).
- Exemem builds its relay from the same crate through its `fold/` submodule, so the relay
  it deploys carries the contract of that `fold/` commit.

## CI conformance

Same scenarios as `cloud-api.md` against ExeMem and the in-repo reference
relay, including failover, fork/withhold, bootstrap via bundles, restore-
from-start, the bundle few-hundred-MB under-a-minute upload/restore check,
**non-grantee append refused (403)**, and **revoked member append refused
(403)**. Account paths ExeMem-only.
