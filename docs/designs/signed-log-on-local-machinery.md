# Design: the signed log on the previous local machinery

Date: 2026-11-22. Author: Claude, for Tom. Status: approved. Steps 1 to 3 are built: step 1 earlier, steps 2 and 3 (outgoing and incoming, the order rule) together in one change, because neither can be proven without the other. Section 9 says what was built and what was deferred.

Basis: Tom, 2026-11-22: "Can we reuse as much of the old machineries as possible? ... logs can be in any form. And then just reuse the current snapshot ... The only major change is the cloud portion", and "keep the database format as much as you can, especially the snapshot. All of that should be exactly the same."

Read from lastdb main `fb90abaf` and from `d06054159`, the build the primary ran. Claims marked (inferred) were not checked in code. Section 9 follows the code as built.

## 1. The finding

Most of the previous machinery is still on main. Phase 1 deleted the snapshot producer and the node code that drove the spool and the replay. It left the store side in place:

- the spool of accepted writes, which trims on acknowledgement;
- the whole-record replay with its last-writer stamp, kept in a row beside the record;
- the protection of a local write that is still pending.

(The spool is captured by every home that has a relay, and by no other home. See section 9.)

The disk format is 15 at both commits. So the work is mostly to bring back what was deleted, and to connect it to signed streams and the relay.

One hazard existed on main before this change: a home with cloud credentials wrote every commit into the spool, and nothing drained it. The spool now has a sender (section 9), and a store that has no sender captures nothing.

## 2. What stays exactly the same

| Piece | State |
|---|---|
| Database format 15: records on disk, file blobs content-addressed | Same |
| Memory as a byte-limited cache | Same |
| Spool of accepted writes; trim on acknowledgement | Same (on main) |
| Last writer wins by a stamp in a row beside the record; a delete keeps its position | Same (on main) |
| Snapshot: the native files, the manifest by digest, the producer, the staged install with the store closed | Same (restored from `d06054159`) |
| Snapshot files encrypted with the database's own keys; disk copy and cloud copy use the same keys | Same. No extra sealing for a snapshot |
| A file goes up before the entry that names it | Same |

## 3. What changes: the cloud portion only

| Before | Now |
|---|---|
| One cloud log per database; the cloud assigns delivery order | One signed, hash-chained stream per device; no cloud order |
| Order by time, capped by cloud acceptance time | Order by a hybrid time set when the write is made, then node key, then device (section 4a) |
| The cloud verifies and stores entries | The relay is a mailbox. It checks grants only |
| Entries encrypted for the cloud | Entries sealed with the database read key (built in #1843) |
| A snapshot is bound to a cloud delivery number | A snapshot is bound to signed stream positions, and named by a signed `snapshot` entry |
| Snapshot files travel by presigned cloud grants | Snapshot files travel by the relay's file routes |

## 4. How it works

### A write

1. The store commits the write and puts it in the spool. This is unchanged.
2. At upload, the node takes the spool entries past the acknowledged position, seals each body, and signs each as the next entry of this device's stream.
3. It saves the exact signed entries before sending. A retry sends the same bytes.
4. The relay acknowledges. The node trims the spool and deletes the saved entries.

The spool body already carries a record's alternate keys, its file, inline files and schema changes. Nothing is added for those.

### An entry from another device

1. The node reads each stream from the entry after the last one it holds.
2. It verifies the entry and opens it.
3. In one store transaction it writes the record if the entry's position is higher than the record's stamp, and advances that stream's head.

A local write that waits for its entry to be signed is protected by the pending marker and the baseline row of the store. It is settled when its entry is signed (section 9).

"Accepted" and "applied" are one fact. A restart replays nothing.

### What a node keeps

- The database.
- Per stream, the head: sequence number, hash, order value.
- Grants and undecided proposals, for shared databases.
- Spool entries not yet acknowledged, and the signed entries of the batch that waits for the relay.

A personal database needs no row per accepted entry. Its entries cite only the birth, and the order does not depend on citations (section 4a).

## 4a. Order (checked against the code on 2026-11-22)

One rule for every database:

- **The value.** When a write is made, the device stamps it with the larger of its clock (milliseconds) and one more than the highest value it has seen. The store already keeps that high mark: the author clock, raised by `observe_replay_time` when a remote entry is applied.
- **The order.** The value, then node key, then device id. This is the existing stamp row's order (`ReplayStamp`: `ordering_ms`, `writer_key`, `writer_device`, `writer_seq`), so it fits with no format change, and "written at" stays a real time.
- **Valid when** the value is higher than the previous entry on the same stream, higher than every entry it cites, and below a fixed far-future limit. The old "exactly one more than the highest seen" check goes.

What it gives: the later edit wins across devices; an edit made after seeing another always wins; an offline edit keeps the time it was made. The relay takes no part.

What it gives up: the cloud's cap on a fast clock. A device whose clock runs ahead can win a conflict against edits made before the other devices saw its entries. The far-future limit stops a wild value from breaking the clock.

Details the check found:

- The spool records the write time beside the entry, not the writer sequence. The sequence is assigned at upload, as in the previous build.
- The commit clock is in nanoseconds and the stamp in milliseconds, so two writes can tie. The signer raises a tied value by one millisecond.
- Schema changes are stamped from a different clock today. They move to the same one.
- Only entries that count raise a device's high mark. A member's proposal does not, so a member cannot push the publisher's clock forward.
- The wire field keeps the name `lamport` in this step. Renaming it, removing the unused `cutoffs` field, and citing entries by stream and sequence number are one later format change. That change is not needed for the personal database.

### Citations

- **Personal database.** An entry cites only the birth. A node reads every stream to its head each sync and applies entries in order, so a schema change arrives before the data that uses it.
- **Shared databases.** Unchanged in this step: entries cite by hash, and the node holds the grants and proposals it needs.
- **Later.** Citing by node key, device, sequence number and hash lets a shared database check a citation against stream heads. The check found it needs the node key in the citation, three small tables (grants, open proposals, decided proposals), and a choice about detecting a fork at an old position.

All of it lives in rows of the store itself, so a snapshot carries it. There is no `authored.json` or `received.json`, and no copy of the history in memory.

### Snapshot and restore

1. The node freezes writes when the spool is empty, and takes the snapshot as before.
2. It uploads the files and the manifest through the relay, then signs a `snapshot` entry that names the manifest and the stream positions.
3. A restore reads the newest `snapshot` entry, checks the signature and the digests, installs the files with the store closed, then reads each stream after the snapshot's positions.

A restoring node trusts the owner's signature on the snapshot. For a personal database the owner is the only writer.

### Finding streams

A new relay call lists a writer's device streams with each head. A node then reads only what is new. An idle sync reads nothing.

## 5. What is deleted

- From #1803 and #1843: the in-memory history of records and entries for the personal database, the JSON log files, and the code that signed at commit and rebuilt records from the history.
- Kept from #1843: the identity and key derivation, the sealing, the restore from the recovery phrase alone, the reference relay, the CI proof.
- Not brought back: the old journal that wrote one file per applied change. The row per entry replaces it.

Shared databases keep the in-memory history for now. They are small. They move to the same rows later.

## 6. Order of work

Each step has a real-node proof in CI.

| Step | Content | Proof | Risk |
|---|---|---|---|
| 0 | Larger data and a memory reading in the proof | A baseline | Low |
| 1 | Relay: list streams; stream large files | An idle sync reads nothing; a large file moves without filling memory | Low to medium |
| 2 | Outgoing: sign at upload from the spool; saved retry entry | A kill under write load loses nothing and repeats nothing; spool size stays flat | High |
| 3 | Incoming: apply with the stamp in one transaction | Two devices converge; a deleted record does not come back; memory stays bounded; a restart replays nothing | High |
| 4 | Snapshot as the checkpoint; restore from snapshot and tails | A restore from a snapshot equals a full replay | High |
| 5 | The move of an existing home | A home with data, made in CI, moves and matches key by key | Medium |
| 6 | The relay drops entries a snapshot covers | A trimmed relay still restores | Medium |

Steps 1 to 5 are required before the primary moves. Step 6 can wait.

## 7. Differences to accept, and risks

1. **Order is a hybrid time, not the cloud-capped time.** Section 4a. A fast clock is no longer capped by the cloud.
2. **The relay holds the only copy of old entries.** If it lost them after a trim, the snapshot still restores the state.
3. **One signed entry per write.** A bulk import would make many entries. The move does not, because it publishes the existing files as the first snapshot.
4. **The primary's home.** #1843 refuses a home whose personal database id is not derived from the recovery phrase. The move maps the registry onto the derived id while the store files stay as they are (inferred that they do not embed the id). So the primary can move in place by a snapshot of its existing files, without copying every record through the write call.
5. **Exemem.** The relay needs the stream listing, and later the trim call. Its file routes use presigned uploads (inferred).

## 8. Decisions

Tom approved the plan on 2026-11-22 ("All right, it's approved. Let's do it."), after it was put to him as: keep the cloud log as designed, keep the disk storage as it is, join them in one thin layer.

Settled by the code check after that, within the plan: the hybrid order of section 4a, and no per-entry row for a personal database.

Status: steps 1 to 3 are built. Steps 4 to 6 are not.

## 9. What was built for steps 2 and 3

This section is the record of the code. It replaces the plan where the two differ.

### The layers

- The spool of accepted writes and the store's replay functions are used as they are. They gained one row family, one author-clock call for schema changes, and four functions on the mutation manager (below). The format of the database, the record layout and every existing row are unchanged. The home format stays 15.
- Two defects of the spool were found by the proof and fixed, with no change to any stored format. A commit of about 200 captured writes made the pending record larger than the 64 KiB bound of the spool's control files, which failed the commit queue and stopped the daemon: the bound is now 1 MiB and a captured commit is capped at 1024 mutations, with a refusal the caller can read. A kill during the move of a commit's entries into the spool left entries above the head, which the recovery at open called a hole although the pending record names them: recovery now accepts exactly those entries and finishes the move.
- `lastdb_node/src/log_sync/personal/` is the thin layer that joins them to the signed streams: `outgoing.rs` signs and sends, `incoming.rs` reads and merges, `verify.rs` checks an entry with `AcceptEntry`, `apply.rs` applies a change with its stamp, `body.rs` reads the accepted body, `saved.rs` keeps the signed batch that waits, `files.rs` moves file bytes.
- Brought back from `d06054159` and adapted: the sender loop (`accepted.rs` and `accepted/source.rs`: read a page of the spool, seal each body, sign, keep the signed bytes, send, confirm the position), the saved retry store (`session/retry`), and the apply of a downloaded entry (`database_replay/{apply,payload,accepted_record,chain,page,types}.rs`: the body types, the file preparation before the commit, the last-writer rule of `RecordReplay::attempt`). Not brought back: `journal.rs`, and the signing at commit.
- Deleted for the personal database: the sealed mirror and the sealed bundle path (`mirror_sealed.rs`, `blobs.rs`), `read_streams` (every stream from entry 1 at every turn), the use of `authored.json`, `uploaded.json` and `received.json`, and the in-memory history. Shared databases keep `VerifiedHistory` and their files and behave as before.

### A write

Every commit lands in the spool, in order, before the caller is told. The sender takes the entries past the confirmed position (at most 90 per append and about 8 MiB of payload), opens each canonical body with the content key, seals it with the database read key bound to the position it will have, and signs it as a `data` entry that cites the birth. One write is one entry. A schema put or drop is an entry of the same kind. A file a record names goes up through `blob.put`, sealed, before the entry that names it.

### Rows added

One family in the metadata namespace of the store, beside the `signed-replay-*` rows:

`signed-replay-head:<node key hex>:<device id hex>` holds JSON `{"seq": n, "hash": "<entry hash hex>", "order": <order value>, "frozen": false}`. There is one row for each device stream that this node has applied an entry of, and one for its own stream (the last entry the relay acknowledged). There is no row for an accepted entry. A schema drop of another device keeps a receipt row, as the store's paged removal requires.

The signed entries that wait for the relay are a file, `log-sync/<database>/outgoing.json`, with their sealed blobs in `outgoing-blobs/`. A file and not rows, because the entries are needed only until the relay acknowledges them, are never read by key, and must not travel in a snapshot of the store (a restored home has its own stream and nothing of the old one to send). The file is written whole with a rename, as the old retry store was.

### Crash windows

- Before the batch is saved: nothing was sent. The next turn signs the same spool entries at the same sequence numbers.
- After the batch is saved, before the relay acknowledges: the next turn settles the entries again (a no-op if done) and sends the saved bytes. The relay counts identical entries as duplicates.
- After the relay acknowledges, before the spool confirms: the same.
- After the spool confirms, before the own head row: the spool position is past the batch. The next turn writes the head row from the saved entries and deletes the file.
- After the head row, before the file is deleted: the next turn deletes the file.
- Applying an entry of another device: the record, its winner row and the head row of the stream are one store transaction. A crash before it commits applies nothing, and the entry is read again. A crash after it replays nothing.

### An entry from another device

Each turn the node lists the owner's device streams (`log/streams`, no entries). A stream whose relay head is above its head row is read from the next entry, one page at a time. Streams are merged by order value, then node key, device id and sequence number, so a schema arrives before the records that use it. An entry is checked with `AcceptEntry` (signature, chain to the head row, order rule), its payload is opened, a file it names is fetched and checked, and the change is applied with the stamp `ReplayStamp {ordering_ms: order value, writer_key: node key, writer_device: device id, writer_seq}`. A stream that carries an invalid entry is frozen and never read again. An idle turn reads no entry.

### A local write that waits

A commit sets the store's pending marker and baseline for each record it changes. A remote entry that arrives while a local write waits is kept as the baseline and does not replace the visible record. When the local entry is signed, the sender settles it with the same last-writer rule: if no newer write won, the local write becomes the winner and the marker is cleared. If a newer write won while it waited, the winner replaces it. The settle repeats safely, so a crash in the middle of a batch is repaired by running it again.

### The order rule

The order value (the wire field `lamport`) is the time of the commit in milliseconds, from the store's author clock. The author clock is raised past every remote entry that is applied, so a write made after an entry has a higher value. A schema put or drop takes its time from the same clock. The sender signs the larger of that time and one more than the previous entry of the stream, so the value rises on every stream.

`AcceptEntry` accepts an order value that is above the previous entry of the stream and above every cited entry, and below `LAMPORT_LIMIT = 4_102_444_800_000` (2100-01-01 in milliseconds, far below the point at about the year 2540 where the author clock would overflow). The rule is the same for every database. A shared-database author signs the larger of its clock and one more than the highest value it has seen, so its values stay causal. Only an applied entry raises the author clock, so a member's proposal does not.

### Capture

A home captures every write exactly when something sends the spool: it has a relay (`LASTDB_RELAY_URL` or saved cloud credentials), it already has a spool, or it is a restore. The owned and received stores of shared databases capture nothing, because nothing sends a spool for them.

### What CI proves

`scripts/operations/personal_database_proof.py` (job `personal_nodes`): one entry on the relay for each write; a second device and restored homes from the phrase alone; the later edit wins whichever device syncs first, with the earlier writer holding more writes; a deleted record stays deleted when an older edit arrives after it; alternate keys and a file read on three devices with the relay stopped; no key, value or schema name on the relay; an idle turn reads nothing and a restart replays nothing; kill -9 under write load loses and repeats nothing; and tens of thousands of writes with bounded peak memory and an emptied spool.

### Deferred

Steps 4 to 6: the snapshot as checkpoint (a restore still replays every stream from the first entry), the move of an existing home, and the relay dropping entries a snapshot covers. Shared databases still use the in-memory history. The pure `mutation_log` crate still carries the `bundle` entry kind and the opener of sealed payloads, which only the relay end-to-end tool uses now. The relay's contiguous head (`log/streams`) is used for the listing only; a read that returns fewer entries than the head reports is an error.
