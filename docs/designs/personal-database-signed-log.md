# Design: a personal database on the signed log (backup and multi-device)

Date: 2026-11-22. Author: Claude, for Tom. Status: built on the spool and the store's replay (section 13; `signed-log-on-local-machinery.md` has the details) ("Yes, build it", then "I think you probably have the right idea ... go ahead and start building it", Tom, 2026-11-22).

## 1. The idea

A personal database is the same signed log as a shared one, with no grants. The owner's node key is the publisher. Every device of the owner has its own stream. The copy of those streams on the relay is the backup, and reading them is how a second device catches up.

Nothing new is added to the relay protocol. Every device signs with the same node key, and the accept rule already says the publisher's direct entries count.

## 2. Why it is needed

On lastdb main (9f642e0f) the node signs entries only for a shareable database. A personal database gets no birth, its writes are not signed, and nothing reaches the relay. The old snapshot backup is removed. So main has no cloud backup for personal data, and the primary cannot move to it.

## 3. Identity

| Thing | Comes from | Same on every device |
|---|---|---|
| Node key | The recovery phrase (the node seed) | Yes |
| Personal database key and id | Derived from the node seed with a fixed label | Yes |
| Read key of a database | Derived from the node seed and the database id | Yes |
| Device id | Random, made once per installation | No |

A restore needs only the recovery phrase and the relay. The phrase gives the node key, the personal database id and the read key.

## 4. A write

1. The app writes. The store validates and commits, and puts the write in the spool of accepted writes before the caller is told. The spool is the upload queue.
2. The sync loop takes the spool entries past the acknowledged position, opens each canonical body, seals it with the database read key for the position it will have, and signs it as the next entry of this device's stream. One write is one `data` entry. A schema put or drop is an entry of the same kind. Every entry cites the birth.
3. The loop saves the exact signed entries before it sends them (`log-sync/<database>/outgoing.json`), so a retry sends the same bytes and never signs again. It sends up to 90 entries in one append.
4. When the relay acknowledges, the loop confirms the position on the spool, which trims it, writes the own head row, and deletes the saved entries.

The commit already put the alternate keys of a record, its file or inline file and a schema change in the body, so nothing is added for them. A home with no relay captures nothing, and the owned and received stores of shared databases capture nothing, so no spool fills that nothing sends.

## 5. Sealing: the relay cannot read a personal database

Every payload of a non-shareable owned database is sealed before it is signed.

- Key: the database read key (section 3).
- Cipher: the AEAD the repository already uses for stored data. A fresh random nonce is stored in front of the ciphertext.
- Bound data: the database id, node key, device id and `writer_seq` of the entry, so a ciphertext cannot be moved to another position.
- What is sealed: the whole accepted body of the write (schema name, record key, fields, alternate keys, file marker, or the schema definition), and file blobs.
- The schema name is sealed too. A sealed entry carries the fixed namespace `sealed`, and the real schema name travels inside the sealed payload.

The relay sees how many entries there are, their sizes and their times. A shareable database stays as it is today (payloads in clear) until the read key delivery for members is designed. That is a separate open question.

## 6. Several devices

Each sync turn, a device asks the relay for the heads of the owner's device streams (one call, no entries). For each stream other than its own it reads from the entry after the head row it holds, merges the streams by order value, verifies each entry (signature, chain, order rule), opens it, and applies it with the stamp of the entry: the order value, the node key, the device id and the sequence number. The record, its winner row and the head row of the stream are one store transaction.

Two devices that change one record while apart: the later edit wins, because the order value is the time of the commit. A device that has seen an entry writes later entries above it. The order is the same rule as everywhere else: the higher position wins.

A device never needs another device to be online. The relay holds the streams. An idle turn reads no entry, and a restart replays nothing.

## 7. A new device, and a restore

They are the same operation.

1. Enter the recovery phrase. The node derives the node key, the personal database id and the read key.
2. List the streams of that node key and read each from the first entry. Verify, unseal, apply.
3. Make a new device id and start a new stream. The old device's stream stays as it is.

The personal database holds the list of the owner's other databases (id, name, kind) in a reserved schema, sealed like everything else. A restore reads that list and then restores each of those databases the same way.

## 8. Files

A file blob is sealed and uploaded before the entry that names it. A device that applies the entry fetches the blob, checks it against the marker in the body, and commits the file with the record.

## 9. Existing records and the primary

No conversion code. The primary moves once, by a manual copy: a fresh home on the new build, then every schema's keys are listed from the old node and its records are read in batches and written to the new node through the normal write call. The new node signs and seals them as it would any write, one entry for each write.

## 10. Costs and limits

- A restore installs the newest snapshot, then reads only the entries after it. With no snapshot it replays the whole history.
- The relay learns sizes and timing.
- Losing the recovery phrase loses the backup.

## 11. Proof in CI (real nodes, reference relay)

1. Device A writes records in two schemas, in single writes and in batches.
2. Device B starts from the recovery phrase alone and holds every record.
3. A and B change the same record while apart, A having made more writes than B and B having edited later. After both sync, in either order, both hold the value of B. A record that A deleted does not come back when an older edit of B arrives after.
4. A fresh home restored from the phrase equals A and B, key by key.
5. The bytes the relay stored contain none of the record keys, values or schema names.
6. A restarted node holds its records without the relay, signs its next entry on the right sequence, and replays nothing.
7. A file written on A is readable on B and on the restored home.

## 12. Order of work

1. Core: derived personal id, sign and seal personal writes, read the owner's other devices, restore and join from the phrase, CI proof items 1 to 6.
2. Files and the list of owned databases, CI proof item 7.
3. The copy of the primary and the move, with a restore proof of the new home before the old one is retired.

## 13. What is built

Sections 3 to 8 are built. The code is in `lastdb_node/src/log_sync/personal/` and `personal.rs`, `lastdb_node/src/log_sync/sealed.rs`, `lastdb_node/src/personal_identity.rs`, and `mutation_log/src/sealed.rs`. The proof is `scripts/operations/personal_database_proof.py`, job `personal_nodes` in `signed-history-dev.yml`. `docs/designs/signed-log-on-local-machinery.md` section 9 lists the rows, the crash windows and what is deferred. Sealing was built in the first step and is unchanged.

### Keys

All keys are HKDF-SHA256 outputs of the node seed (the 32 bytes behind the recovery phrase). The seed is uniformly random, so HKDF takes no salt, and each key has its own `info`.

| Key | `info` |
|---|---|
| Personal database key. Its verifying key is the database id. | `lastdb:personal-database:v1:database-key` |
| Birth salt | `lastdb:personal-database:v1:birth-salt` |
| Device id of the birth stream | `lastdb:personal-database:v1:birth-device` |
| Read key of one database | `lastdb:database-read-key:v1:` followed by the 64 hex characters of the database id |

A home whose personal database is not the derived one is refused at boot. An old home is never converted.

### One birth, on its own stream

The birth is the same bytes on every device: the salt and the signature come from the seed, and the entry is authored at time 0 on the stream whose device id is the birth device id. That stream holds only the birth. A device uploads it when the relay does not list the birth stream, before any blob or entry, and the relay and every reader get an identical entry, so two devices that both write first cannot disagree about the publisher. Every personal entry cites the birth and nothing else.

### Sealing

Every payload of the personal database is sealed with the AES-256-GCM envelope that LastDB uses for stored data: `0x01 ‖ nonce(12) ‖ ciphertext ‖ tag(16)`, a fresh random nonce for each payload. The bound data is 104 bytes: database id (32), node key (32), device id (32) and `writer_seq` (8, big-endian). A sealed entry carries the namespace `sealed`. The plaintext of an entry is the canonical JSON body that the spool records for the write. A file blob is the file bytes sealed the same way, at the position of the entry that names it, and its SHA-256 is the digest the entry names.

### A sync turn

`one_turn` runs the personal turn between the shared pull and the shared upload: list the streams, send the spool, apply the other streams. The relay answers 403 to a list of the streams of a database whose birth it does not hold. For the personal database that is an owner who has not written on any device, and a device that has never uploaded treats it as an empty relay. A device that has uploaded gets the 403 as an error.

### Restore and a new device

`lastdb backup restore --recovery-phrase-stdin [--relay-url URL] --into DIR [--json]` makes a home from the phrase, installs the newest snapshot of the relay when there is one (the native files, checked against the digests of a signed `snapshot` entry), runs the incoming turn from the position after the snapshot on each stream (from the first entry when there is no snapshot), and signs on a new stream under a new device id from then on. It needs no database id and no device ids. It prints `ok`, `database_id`, `devices` (device streams read, not counting the birth stream), `through`, `replayed`, `snapshot` (null, or `snapshot_seq`, `files`, `bytes`, `streams`, `download_ms`) and `peak_memory_bytes`. `lastdb backup snapshot [--wait]` takes a snapshot now; a device also takes one after 25,000 entries on its own stream.

### Open items

- The relay does not yet drop the entries a snapshot covers (a later step). A home that held data before it began to sync publishes its existing files as its first snapshot and sends nothing else until that snapshot is named (`signed-log-on-local-machinery.md` section 11).
- Shared databases keep the in-memory history and their log files.
- The list of owned databases (section 7) is not built. A database of kind personal that is not the owner row stays unsynced.
