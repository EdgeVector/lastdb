# The format 15 home

This page describes the home of a LastDB node as it is on disk: the files, the
rows inside the store, and which of them travel in a snapshot. It describes
what is built. It is not a design for work in progress.

Written 2026-11-23 from main `0dd6be839`. The code is the authority:
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
transaction as the record it belongs to. A restored home must not carry
anything that belongs to the old device's stream, so the things that wait to
be sent are files outside the store.

## The layout

`[S]` marks what a snapshot carries.

```text
<home>/
  identity.key               the node seed, 32 bytes. Never leaves the device.
  cloud-credentials.json     the saved relay connection (Exemem)
  databases/registry.json    the databases on this node: id, name, kind
  data/
    laststore-pack-node-v1   [S] 37 bytes: format=15, home=disk-run, cas=disk-cas
    disk-run/                    the record store, one laststore run home
      laststore-run-home-v2  [S] 64-byte clear descriptor: magic, home id, CRC
      CURRENT                [S] sealed list of the live runs, and the frontier
      runs/<run id>.ldrun    [S] immutable run files
      LOCK                       owner lock
      accepted-v1/               the spool. Not in a snapshot.
        state.json                 head, acknowledged position, flags
        cloud-upload.json          upload progress paired with the spool
        entries/                   signed entries the relay has not acknowledged, with their sealed files
        ids/                       one file per recent mutation id
        staging/
    disk-cas/                [S] the file blob store, the same shape as disk-run
    local-file-refs.json         files that records hold by path
    app-grants.json              local app grants
    folddb.sock, apps.sock       the daemon's sockets
  log-sync/
    device-id                  the device id of this installation
    wake                       wakes the sync loop
    <database id>/
      snapshot-last.json       the last snapshot this device published
      snapshot-known.json      what `lastdb status` prints
      snapshot-request, -result, -error
      authored.json, uploaded.json, received.json, proposals.json
                               shared databases only
```

The spool sits inside `disk-run/` and is still not part of the store. A
snapshot names the marker, and the descriptor, `CURRENT` and run files of both
run homes. It names nothing else.

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
| `ad:` | The stamp of a delete that won. | An older edit that arrives later does not bring the record back. |
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
| `signed-replay-file-winner:record:<hash>`, `-inline-winner:record:<hash>` | `ordering_ms`, `writer_key`, `writer_device`, `writer_seq` | The stamp of the newest write that set or cleared a record's file, or its inline file. A delete is a write of both. |
| `signed-replay-winner:schema:<hash>` | `ordering_ms`, `writer_key`, `writer_device`, `writer_seq` | The stamp of the newest schema put or drop. |
| `signed-replay-chain:<device id>` | `seq`, `hash`, `order` | The newest entry of this device's stream, written by the commit that signed it. The next commit signs the entry after it. It can be ahead of this device's `signed-replay-head` row by the entries the relay has not acknowledged. |
| `signed-replay-head:<node key>:<device id>` | `seq`, `hash`, `order`, `frozen` | The last entry of one device stream that this node applied, and for its own stream the last entry the relay acknowledged. |
| `signed-replay-receipt:` | The digest of an applied schema drop. | The store's paged removal needs it. |
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
a record whose file is cleared this way is removed.

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
  snapshot uploads only the files that the previous snapshot of the device did
  not hold.
- A lookup reads the footer and the top block, one index block and one data
  block. A home keeps at most 32 run readers open. A reader holds no record.

## The spool

- Every commit of a captured home puts one entry in the spool before the
  caller is told. One write is one entry. A schema put or drop is an entry.
- A spool entry is the signed entry of the device stream, as the bytes the
  relay stores (`body.bin`), and the sealed file that the entry names
  (`file.bin`). The store makes both in the commit: the host gives it an author
  that seals with the database read key and signs with the node key. The sender
  reads the entries, sends the files and then the entries, and retires them
  when the relay acknowledges. It signs nothing, and a retry sends the same
  bytes.
- A file that a write links by its path is read once, at the commit, and its
  sealed copy stays in the spool until it is sent. It counts toward the size
  limit of the spool (1 GiB).
- A home captures exactly when something sends the spool: it has a relay, it
  already has a spool, or it is a restore.
- `state.json` holds the head and the acknowledged position. An acknowledgement
  trims the bodies. `gap` blocks the next commit. `snapshot_required` means the
  home held data before it began to capture: nothing is read for upload until
  a first snapshot covers that data.
- One commit captures at most 1024 mutations.
- `ids/` keeps the newest 10,000 mutation ids with the hash of each body, and
  the id of every entry not yet acknowledged. A repeated write with the same id
  and body is not entered twice. The same id with another body is refused.
- The sequence number of the signed entry is the number of the spool entry.
  A schema change that the spool already holds under its id gets no second
  entry and no place in the stream.

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

## Names

The code and the wire keep some names from earlier designs. This is what each
one is.

| Name in code or on the wire | What it is |
|---|---|
| `accepted-v1`, "accepted stream", "upload queue" | The spool. |
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
| v1 | `accepted-v1` | The spool directory |
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
