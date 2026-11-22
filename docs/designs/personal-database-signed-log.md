# Design: a personal database on the signed log (backup and multi-device)

Date: 2026-11-22. Author: Claude, for Tom. Status: approved to build; step 1 built (section 13) ("Yes, build it", then "I think you probably have the right idea ... go ahead and start building it", Tom, 2026-11-22).

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

1. The app writes. The store validates and commits.
2. The node signs the change on this device's stream and appends it to `authored.json`. One write call is one entry: `data` for one record, `bundle` for several.
3. If the entry cannot be signed, the record is reverted (the existing rule).
4. The sync loop uploads entries past the cursor in `uploaded.json`.

## 5. Sealing: the relay cannot read a personal database

Every payload of a non-shareable owned database is sealed before it is signed.

- Key: the database read key (section 3).
- Cipher: the AEAD the repository already uses for stored data. A fresh random nonce is stored in front of the ciphertext.
- Bound data: the database id, node key, device id and `writer_seq` of the entry, so a ciphertext cannot be moved to another position.
- What is sealed: the record key and value of a `data` entry, the definition of a `schema` entry, the blob of a `bundle`, and file blobs.
- The schema name is sealed too. A sealed entry carries the fixed namespace `sealed`, and the real schema name travels inside the sealed payload.

The relay sees how many entries there are, their sizes and their times. A shareable database stays as it is today (payloads in clear) until the read key delivery for members is designed. That is a separate open question.

## 6. Several devices

Each sync turn, a device reads every stream signed by its own node key for the database, skips its own stream, and accepts the entries (signature, sequence, Lamport). It unseals them and applies them. Two devices that change one record: the higher position wins (Lamport number, then device id), the same rule as everywhere else.

A device never needs another device to be online. The relay holds the streams.

## 7. A new device, and a restore

They are the same operation.

1. Enter the recovery phrase. The node derives the node key, the personal database id and the read key.
2. Read every stream of that node key from the first entry. Verify, unseal, apply.
3. Make a new device id and start a new stream. The old device's stream stays as it is.

The personal database holds the list of the owner's other databases (id, name, kind) in a reserved schema, sealed like everything else. A restore reads that list and then restores each of those databases the same way.

## 8. Files

A file blob is sealed and uploaded before the entry that names it. A restore fetches a blob when a record that names it is read, or all of them when asked.

## 9. Existing records and the primary

No conversion code. The primary moves once, by a manual copy: a fresh home on the new build, then every schema's keys are listed from the old node and its records are read in batches and written to the new node through the normal write call. The new node signs and seals them as it would any write. Batches become bundles, so the first upload is a modest number of entries.

## 10. Costs and limits

- A restore replays the whole history. There are no snapshots. A checkpoint bundle can shorten this later if it becomes slow.
- The relay learns sizes and timing.
- Losing the recovery phrase loses the backup.

## 11. Proof in CI (real nodes, reference relay)

1. Device A writes records in two schemas, in single writes and in batches.
2. Device B starts from the recovery phrase alone and holds every record.
3. A and B change the same record while apart. After both sync, both hold the same value, the one with the higher position.
4. A fresh home restored from the phrase equals A and B, key by key.
5. The bytes the relay stored contain none of the record keys, values or schema names.
6. A restarted node rebuilds from its files without the relay.
7. A file written on A is readable on B and on the restored home.

## 12. Order of work

1. Core: derived personal id, sign and seal personal writes, read the owner's other devices, restore and join from the phrase, CI proof items 1 to 6.
2. Files and the list of owned databases, CI proof item 7.
3. The copy of the primary and the move, with a restore proof of the new home before the old one is retired.

## 13. What step 1 built

Step 1 is the core: sections 3 to 7, without files (section 8), the list of other owned databases, and the move of the primary (section 9). The code is in `lastdb_node/src/log_sync` (`sealed.rs`, `mirror_sealed.rs`, `personal.rs`, `blobs.rs`), `lastdb_node/src/personal_identity.rs`, and `mutation_log/src/sealed.rs`. The proof is `scripts/operations/personal_database_proof.py`, job `personal_nodes` in `signed-history-dev.yml`.

### Keys

All keys are HKDF-SHA256 outputs of the node seed (the 32 bytes behind the recovery phrase).

| Key | Salt | Info |
|---|---|---|
| Personal database key. Its verifying key is the database id. | `lastdb:personal-database:v1` | `database-key` |
| Birth salt | `lastdb:personal-database:v1` | `birth-salt` |
| Device id of the birth stream | `lastdb:personal-database:v1` | `birth-device` |
| Read key of one database | `lastdb:database-read-key:v1` | the 64 hex characters of the database id |

A home whose personal database is not the derived one is refused at boot. An old home is never converted.

### One birth, on its own stream

The birth is the same bytes on every device: the salt and the signature come from the seed, and the entry is authored at time 0 on the stream whose device id is the birth device id. That stream holds only the birth. A device that never saw the birth signs it on its first write, and the relay and every reader get an identical entry, so two devices that both write first cannot disagree about the publisher.

### Sealing

Every payload of the personal database is sealed with the AES-256-GCM envelope that LastDB uses for stored data: `0x01 ‖ nonce(12) ‖ ciphertext ‖ tag(16)`, a fresh random nonce for each payload. The bound data is 104 bytes: database id (32), node key (32), device id (32) and `writer_seq` (8, big-endian). A sealed `data` or `schema` entry carries the namespace `sealed`. A history that has an opener refuses a data or schema entry that is not sealed or does not open. Plaintext layouts:

- `data`: `u16 name_len ‖ schema name ‖ u16 key_len ‖ record key ‖ value`;
- `schema`: `u16 name_len ‖ schema name ‖ JSON definition`;
- `bundle` blob: `u32 count`, then per change `u16 name_len ‖ schema name ‖ u16 key_len ‖ record key ‖ u32 value_len ‖ value`, sorted and unique by schema name and record key.

A bundle blob is sealed the same way, at the position of its bundle entry, and its SHA-256 is the digest the entry names. The node keeps the blob under `log-sync/<database>/blobs/<digest>`, uploads it before the entry, and reads it back on a restart.

### A write

One write call is one entry. A write with one record is a `data` entry. A write with several records is a `bundle` entry; two mutations of one record in one call keep the last. A schema that the history does not carry yet is signed first as its own `schema` entry. If an entry cannot be signed, the store reverts the records to the history, as it does for a shareable database.

Every entry of the personal database cites the tip of every other stream of the owner in `seen_heads`, the birth stream included. Its Lamport number is therefore above everything its device has seen, so a write made after a sync wins over the writes it saw, and a reader that has not seen the birth holds the entry until it does.

### A sync turn

`one_turn` uploads the logs first, so the birth is on the relay before anything is read. Then, for the personal database, it reads every stream of the node key with one all-devices read, drops its own stream, sorts the entries by Lamport number so dependencies come first, accepts them (a bundle needs its blob, which is fetched and kept), and applies the records to the store. The relay answers 403 to a read of a database whose birth it does not hold. For the personal database that is an owner who has not written on any device, and a device with no birth in its history treats it as an empty relay. A device that holds a birth gets the 403 as an error.

### Restore and a new device

`lastdb backup restore --recovery-phrase-stdin [--relay-url URL] --into DIR [--json]` makes a home from the phrase, runs the same read, and starts a new stream under a new device id. It needs no database id and no device ids. It prints `ok`, `database_id`, `devices` (device streams read, not counting the birth stream), `through` and `replayed`.

### Open items

- A restore and every sync turn read all streams from the first entry. A per-device read from a sequence needs the device ids, and the all-devices read starts at sequence 1 for every device but the first. A checkpoint bundle or an incremental read should come before the primary moves.
- `authored.json` is one JSON array with a 16 MiB read limit, rewritten on every write.
- A `data` entry carries no alternate keys of the record. The materializer writes records without them, as it does for shared databases.
- Files (section 8) and the list of owned databases (section 7) are not built. A database of kind personal that is not the owner row stays unsynced.

