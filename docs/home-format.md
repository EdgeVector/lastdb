# The format 15 home

This page describes the home of a LastDB node as it is on disk: the files, the
rows inside the store, and which of them travel in a snapshot. It describes
what is built. It is not a design for work in progress.

Written 2026-11-24 from main `870f485`. The code is the authority:
`fold_db/crates/core/src/storage/format.rs` declares the format, and
`vendor/laststore/README.md` describes the run files. When this page and the
code differ, the code wins. Fix the page.

How a write leaves the home and reaches the relay is in
`designs/signed-log-on-local-machinery.md`. The relay is in
`designs/relay-protocol.md`.

## The model in four sentences

- The store is the database.
- The spool is the outbox.
- The relay is a mailbox for signed entries and files.
- A snapshot is the store's own files plus a signed entry that says which
  entries those files already hold.

## The one rule about sync state

**Rows in the store travel in a snapshot. Files beside the store do not.**

A restored home must know exactly which entries its files already hold, so the
position of every stream is a row in the store, written in the same
transaction as the record it belongs to. The spool's records also live in
`disk-run` so each commit writes its entry atomically with the record; a
restore deletes those inherited spool records before opening its own device
stream, while `accepted-files/` and `log-sync/` stay outside the snapshot.

## The layout

`[S]` marks what a snapshot carries.

```text
<home>/
  identity.key               the node seed, 32 bytes. Never leaves the device.
  cloud-credentials.json     the saved relay connection (Exemem)
  databases/
    registry.json            the owned databases on this node: id, name, kind, spec, lease length
    <id>/data/               each owned sibling database's store (same run/cas layout, no sockets)
  managed-shares/
    <id>/                    a received database registration, snapshot-absent, and log/ store
  data/
    laststore-pack-node-v1   [S] 37 bytes: format=15, home=disk-run, cas=disk-cas
    disk-run/                    the record store, one laststore run home
      laststore-run-home-v2  [S] 64-byte clear descriptor: magic, home id, CRC
      CURRENT                [S] sealed list of the live runs, and the frontier
      runs/<run id>.ldrun    [S] immutable run files
      LOCK                       owner lock
      accepted-files/            sealed files of spool entries not yet sent, one `<seq>.bin` each. Not in a snapshot.
    disk-cas/                [S] the file blob store, the same shape as disk-run
    local-file-refs.json         files that records hold by path
    app-grants.json              local app grants
    folddb.sock, apps.sock       the daemon's sockets
  log-sync/
    device-id                  the device id of this installation
    wake                       wakes the daemon's sync loop from the `lastdb` command
    <database id>/
      birth.json               birth salt, signature, and author time for an owned shareable database
      cloud-usage.json         cumulative relay request and byte counts for the database
      segment.out              transient: the log segment of one upload, removed after it
      incoming/                sealed blobs downloaded while applying another device's stream
      snapshot-last.json       the last snapshot this device published
      snapshot-known.json      what `lastdb status` prints
      snapshot-manifest, .snapshot-manifest
                               transient manifest upload / restore scratch files
      snapshot-request, -result, -error
      snapshot-unverified      a snapshot entry is on the relay and its files are not checked yet
      reclaim-due.json         a sweep of the relay is due for one snapshot
      reclaim-last.json        what the relay held after the last sweep
      reclaim-named.json       `named` digest-to-length map plus multi-turn sweep `files` and `bytes` totals (legacy bare digest maps still accepted); retained while a multi-turn sweep continues and deleted when the sweep completes or stops with no unsettled digest
      reclaim-request, -result, -error
      authored.json, uploaded.json, received.json, proposals.json
                               shared databases only
```

`accepted-files/` sits inside `disk-run/` beside `runs/` and is not part of a
snapshot; the spool's `accepted_*` collections live in `disk-run` runs and are
cleared on restore before the new device signs. A snapshot names the marker,
and the descriptor, `CURRENT` and run files of both run homes. It names nothing
else.

Every key comes from the node seed in `identity.key`, which is also what the
recovery phrase encodes: the node key, the personal database id, the read key
of each database, and the keys that seal the run files.

## The rows

A run home holds sorted key and value rows in namespaces. All of them travel
in a snapshot.

### Records (`main`)

| Key | Value | Purpose |
|---|---|---|
| `rk:` | One whole record. At most 64 KiB. | The record. Read by key. |
| `ak:` | One alternate entry: an alternate key and the primary key it points to. | Finds the same record by another key. Holds no copy of the record. |
| `ad:` | The stamp of an applied delete. | Lets a later purge check the deleted record's tombstone revision. Sync ordering uses the `signed-replay-*` rows in `metadata`, not `ad:`. |
| `fi:` | An inline file, at most 64 KiB. | A small file travels with its record. |
| `dellog` | The delete audit ledger. | Who erased what, by key fingerprint. |
| `layout:keys` | `typed-ranges` | The key encoding of this home. |

One record value holds the key, the alternate key values, the fields, a
stamp, optional metadata (`created_by`, `updated_by`), and the marker of its
file or inline file. A record has at most one live file.

### File blobs (`disk-cas`)

| Key | Value |
|---|---|
| `fb:<space>:<sha256 of the record key>:<stream>:<sequence>` | The sealed bytes of one file version. |

The directory name says content-addressed. A record's file is addressed by its
record and by the write that made it, not by its content. The blobs on the
relay are the ones named by content (the SHA-256 of the stored bytes).

### Sync positions and last writer (`metadata`)

| Key | Value | Purpose |
|---|---|---|
| `signed-replay-winner:record:<hash>` | `ordering_ms`, `writer_key`, `writer_device`, `writer_seq` | The stamp of the newest write of a record's fields. An incoming entry replaces the fields only if its stamp is higher. |
| `signed-replay-fields:record:<hash>` | `fields`, `alternates`, `writer`, `created_by`, `meta`, `ordering_ms` | Winning record payload held while a `file_required` record awaits its file. |
| `signed-replay-file-winner:record:<hash>`, `-inline-winner:record:<hash>` | `ordering_ms`, `writer_key`, `writer_device`, `writer_seq` | The stamp of the newest write that set or cleared a record's file, or its inline file. A delete is a write of both. |
| `signed-replay-winner:schema:<hash>` | `ordering_ms`, `writer_key`, `writer_device`, `writer_seq` | The stamp of the newest schema put or drop. |
| `signed-replay-drop-winner:schema:<hash>` | `ordering_ms`, `writer_key`, `writer_device`, `writer_seq` | The stamp of the newest schema drop, so an entry older than that drop stays superseded even after the schema is recreated. |
| `signed-replay-drop:schema:<hash>` | `<receipt id>:<digest>` | Marks an in-progress paged schema drop so an interrupted drop resumes under the same receipt and blocks conflicting edits until removal finishes. |
| `signed-replay-chain:<device id>` | `seq`, `hash`, `order` | The newest entry of this device's stream, written by the commit that signed it. The next commit signs the entry after it. It can be ahead of this device's `signed-replay-head` row by the entries the relay has not acknowledged. |
| `signed-replay-head:<node key>:<device id>` | `seq`, `hash`, `order`, `frozen` | The last entry of one device stream that this node applied, and for its own stream the last entry the relay acknowledged. |
| `signed-replay-receipt:<receipt id>` | The digest of a completed schema drop. | Makes the store's paged schema removal idempotent once finished. |
| `mutation_author_clock:<writer id>` | Physical nanosecond floor and logical counter. | Stamps local writes and rises past every remote entry applied here. |
| `changes:<stream>:<seq>` | One changed key. The newest 250,000. | The local changes list for apps. It is not the cloud log. |
| `portable-local-author` | The local author stream. | Numbers local writes. The changes list reuses its numbers. |
| `__content_key_proof_v1` | A sealed constant. | Proves the content key at open. |

The head row and the record are one store transaction. "Accepted" and
"applied" are therefore one fact, and a restart replays nothing.

A write made on this device becomes the next entry of the device's stream in
the transaction that commits it. Its order value is the millisecond of its
author time (the clock gives each captured write a millisecond of its own), and
its sequence number is the one after the `signed-replay-chain` row, which is
also its number in the spool. The commit seals the body with the database read
key, signs the entry, writes the stamp rows and moves the chain row. Nothing
about a write changes when it is sent. A home whose spool holds writes that an
older build left unsigned does not open: the node refuses to start and names
the remedy, which is to run the older build until it has sent them. An
entry of another device is compared with the stamp rows when it arrives, part
by part: the fields, the file and the inline file each take the value of their
newest write, so the result does not depend on the order of arrival. A write
that names no file does not change the file. In a schema that requires a file,
when winning fields arrive before the record's file, `signed-replay-fields:record:<hash>`
holds that payload until a winning file arrives (materializing the record and
clearing the saved row) or a newer write or delete supersedes it; a record
whose file is cleared is removed while its winning fields stay saved for any
later winning file. Record entries older than `signed-replay-drop-winner:schema:<hash>`
stay superseded even if a later schema put recreates the schema.

A snapshot is refused while the spool holds an entry the relay did not
acknowledge.

### Schemas (`schemas`, `schema_states`)

The schema definitions the database uses, and their retention (TTL) state.

## A run file

- A run is one immutable file of rows in ascending key order. A row is a key
  and a value, or a key and a tombstone.
- Each block is compressed with zstd when that makes it smaller, then sealed
  with AES-256-GCM. The block key comes from the home key and the run id. So a
  run file goes to the relay as it is, with no second sealing.
- A commit writes one new run, makes it durable, then publishes a new
  `CURRENT` by rename. A read holds one `CURRENT` for its whole call. Nothing
  replays at open.
- Eight runs of one level merge into one. Run files grow over time, and a
  snapshot uploads only the files that the relay does not hold (it asks the
  relay with `blob.list`). After the snapshot is named, the relay drops the
  files that only an older snapshot named.
- A lookup reads the footer and the top block, one index block and one data
  block. A home keeps at most 32 run readers open. A reader holds no record.

## The spool

- Every commit of a captured home puts one entry in the spool before the
  caller is told. One write is one entry. A schema put or drop is an entry.
- The spool is records in the main home, written in the same commit as the
  write they describe, so a write and its entry become durable together:
  - `accepted_entries/<seq>`: the signed entry of the device stream, as the
    bytes the relay stores, after a short note (mutation id, plaintext SHA-256,
    sealed file SHA-256 and length). The key is the sequence as 20 digits.
  - `accepted_ids/<mutation id>` and `accepted_id_order/<seq>`: the newest
    10,000 mutation ids with the plaintext SHA-256 of each body, plus every id
    not yet acknowledged. A commit that reuses a recorded id, or repeats one in
    the same batch, is refused before the stream moves.
  - `accepted_state/state`: the acknowledged position, the upload position
    that confirms it (one record, so the two are one write), `snapshot_required`
    and the frontier when the stream started. The head is the last entry, or the
    acknowledged position when no entry is left.
- The sealed file that an entry names stays a file, `accepted-files/<seq>.bin`,
  because it can be larger than a record. It is written in order before the
  commit of its entry. A file that a write links by its path is read once, at
  the commit. Sealed files count toward the size limit of the spool (1 GiB).
- The store makes the entry in the commit: the host gives it an author that
  seals with the database read key and signs with the node key. The sender
  flushes the drive to the medium, reads a page of entries, sends the files and
  then the entries, and when the relay acknowledges, one commit moves the
  acknowledged position, records the upload position and deletes the sent
  entries. Their files are removed after that commit; an open removes any file
  outside the spool. The sender signs nothing, and a retry sends the same bytes.
- A home captures exactly when something sends the spool: it has a relay, it
  already has a spool, or it is a restore.
- A spool that has recorded no write starts at the head of its device's chain
  (`signed-replay-chain:<device>`): the entries up to it were sent before.
- `snapshot_required` means the home held data before it began to capture:
  nothing is read for upload until a first snapshot covers that data.
- A snapshot carries the spool records of the home that took it. A restore
  deletes them in its first commits and starts an empty spool for its own
  device.
- One commit captures at most 1024 mutations.
- The sequence number of the signed entry is the number of the spool entry. A
  commit refuses a note signed with another number.
  Each captured schema put or drop derives its mutation id from the schema
  name, definition identity (or `drop`), and `authored_at_ms`, so repeated
  `put -> drop -> put` or `drop` transitions each receive their own spool entry
  and stream sequence, while a duplicate schema mutation id is refused.
- A build from before this layout kept the spool in `disk-run/accepted-v1/`.
  This build refuses a home that still has that folder. The one manual step:
  run the older build until `lastdb status` shows every write acknowledged,
  stop it, check that `accepted-v1/state.json` has head equal to acked and that
  there is no `pending.json`, move `accepted-v1/` to the Trash, start this
  build.

## What an acknowledgement means

- A commit writes its run file, then `CURRENT`, and syncs each one and its
  folder in that order. On macOS each sync is an ordered write
  (`F_BARRIERFSYNC`): the bytes are on the drive and stay in order with later
  writes, and the drive's cache is not emptied. On Linux each sync is
  `fdatasync`.
- So a crash of LastDB or of the operating system loses no acknowledged write.
  A sudden power cut on macOS can lose the newest acknowledged writes that are
  still in the drive's cache, in order: the store is never torn, and a write
  and its spool entry are lost together.
- Nothing that a power cut can still take leaves the machine. Before the sender
  reads a page of entries, before a snapshot is cut, and before a flush receipt,
  the drive is flushed to the medium (`F_FULLFSYNC` on macOS). The sender skips
  the flush when an earlier flush already covered every entry up to the spool's
  head.
- The files under `log-sync/` that only signal or count wait for less. A
  write wakes the sync loop inside the daemon and touches no file. `wake` is an
  empty marker with no flush that only the `lastdb` command creates. The
  segment a sender stages before its upload has no flush, because the next turn
  builds it again from the spool. `cloud-usage.json` is an ordered write and a
  rename, so a power cut can lose its newest count but never tear it. A shared
  database's upload cursor (`uploaded.json`) is an ordered write too: a cursor
  set back only sends entries again, and the relay answers `duplicate`.

## What a snapshot is

- The files marked `[S]` above, pinned at one instant, each uploaded as one
  blob named by its SHA-256, and a manifest that lists them.
- A signed `snapshot` entry that names the manifest and, for each device
  stream, the last entry the files hold. Those positions are the head rows as
  they were at the same instant.
- A file that a record holds by path is sealed into a private copy of the file
  store for the snapshot, as one value. A value holds at most 32 MiB
  (`MAX_RUN_VALUE_BYTES`), so a record file is at most `MAX_RECORD_FILE_BYTES`
  (32 MiB less 29 bytes of sealing). `/v1/write` refuses a larger file with a 400
  that states both sizes: a file the snapshot cannot carry is never accepted.
- A restore installs the files with no store open, checks that the head rows
  equal the positions in the entry, and reads each stream after its position.
- The relay keeps one snapshot. After a snapshot is named, the node asks the
  relay to drop the entries it covers and the older snapshot entries, and then
  to drop every stored file that this snapshot and the entries after it do not
  name. The relay then holds what the store holds.

## Names

The code and the wire keep some names from earlier designs. This is what each
one is.

| Name in code or on the wire | What it is |
|---|---|
| `accepted_*` collections, `accepted-files`, "accepted stream", "upload queue" | The spool. |
| `node_key` | The owner's key. It comes from the recovery phrase and is the same on every device of the owner. |
| `device_id` | The id of one stream. One per installation, plus two that are no device: the birth stream and the snapshot stream of a personal database, both derived from the seed. |
| `lamport` | The order value: the commit time in milliseconds, raised to one more than the highest value the device has seen. Not a Lamport counter. |
| `signed-replay-*` | The rows for sync positions and the last-writer rule. Nothing replays at restart. |
| `disk-cas`, `cas_blobs` | The file blob store. Keyed by record and write. |
| `portable-local-author` | The local author stream. The other `portable-*` keys are retired and the store refuses them. |
| `folddb.sock` | The owner socket of the daemon. |
| "cloud sync", `lastdb backup`, "relay", Exemem | One connection. The copy of the streams and snapshots on the relay is the backup, and reading it is the sync. |

## Version numbers

Only the first one is the disk format.

| Number | Where | What it versions |
|---|---|---|
| 15 | `HOME_FORMAT`, the marker file, `lastdb status` | The home as a whole |
| v2 | `laststore-run-home-v2`, `.ldrun` | The run file engine (laststore) |
| v1 | `laststore-pack-node-v1` | The name of the marker file |
| 3 | `RECORD_VALUE_VERSION` | The encoding of one record value |
| 14 | `RECORD_PROFILE` | An internal profile number of the run home |
| 1 | `X-LastDB-Relay-Protocol` | The relay wire protocol |

## Older descriptions

These describe earlier designs and are history:

- The unified mutation log design, removed from this repository by #1674
  (`git show f43398cc7^:docs/designs/unified-mutation-log.md`): one cloud log
  for each database, with the cloud assigning order.
- The disk and storage format proposals of 2026-11-05 to 2026-11-07, kept
  outside this repository: a journal inside the store with `portable-entry`
  rows, `applied_through` and `uploaded_through` cursors, and log files named
  by sequence range. The store refuses those keys as retired.
