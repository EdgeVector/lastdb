# Design: the signed log on the previous local machinery

Date: 2026-11-22. Author: Claude, for Tom. Status: approved. Steps 1 to 6 are built: step 1 earlier, steps 2 and 3 (outgoing and incoming, the order rule) together in one change, because neither can be proven without the other, step 4 (the snapshot as the checkpoint), step 5 (the move of an existing home), and step 6 (the relay drops what a snapshot covers). Sections 9, 10, 11 and 14 say what was built and what was deferred.

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
| 5 | A home that holds data gets a relay | A home written with no relay publishes its files as the first snapshot and matches key by key | Medium |
| 6 | The relay drops entries a snapshot covers | A trimmed relay still restores | Medium |

Steps 1 to 5 are required before the primary moves. Step 6 can wait.

## 7. Differences to accept, and risks

1. **Order is a hybrid time, not the cloud-capped time.** Section 4a. A fast clock is no longer capped by the cloud.
2. **The relay holds the only copy of old entries.** If it lost them after a trim, the snapshot still restores the state. The relay keeps one snapshot, the newest (Tom, 2026-11-24, section 16): if its files were damaged on the provider, the local database is the only other copy until the next snapshot. A device that has not read the entries a trim dropped cannot read them from the relay any more; it is told so and restores into a new home (section 14).
3. **One signed entry per write.** A bulk import would make many entries. The move does not, because it publishes the existing files as the first snapshot.
4. **The primary's home.** #1843 refuses a home whose personal database id is not derived from the recovery phrase. The move maps the registry onto the derived id while the store files stay as they are (inferred that they do not embed the id). So the primary can move in place by a snapshot of its existing files, without copying every record through the write call.
5. **Exemem.** The relay needs the stream listing, and the trim call (section 14). Its file routes use presigned uploads. Section 13 says what is proven against the deployed DEV relay.

## 8. Decisions

Tom approved the plan on 2026-11-22 ("All right, it's approved. Let's do it."), after it was put to him as: keep the cloud log as designed, keep the disk storage as it is, join them in one thin layer.

Settled by the code check after that, within the plan: the hybrid order of section 4a, and no per-entry row for a personal database.

Status: steps 1 to 6 are built.

## 9. What was built for steps 2 and 3

This section is the record of the code. It replaces the plan where the two differ.

### The layers

- The spool of accepted writes and the store's replay functions are used as they are. They gained one row family, one author-clock call for schema changes, and four functions on the mutation manager (below). The format of the database, the record layout and every existing row are unchanged. The home format stays 15.
- Two defects of the spool were found by the proof and fixed, with no change to any stored format. A commit of about 200 captured writes made the pending record larger than the 64 KiB bound of the spool's control files, which failed the commit queue and stopped the daemon: the bound is now 1 MiB and a captured commit is capped at 1024 mutations, with a refusal the caller can read. A kill during the move of a commit's entries into the spool left entries above the head, which the recovery at open called a hole although the pending record names them: recovery now accepts exactly those entries and finishes the move.
- `lastdb_node/src/log_sync/personal/` is the thin layer that joins them to the signed streams: `outgoing.rs` signs and sends, `incoming.rs` reads and merges, `verify.rs` checks an entry with `AcceptEntry`, `apply.rs` applies a change with its stamp, `body.rs` reads the accepted body, `saved.rs` keeps the signed batch that waits, `files.rs` moves file bytes.
- Brought back from `d06054159` and adapted: the sender loop (`accepted.rs` and `accepted/source.rs`: read a page of the spool, seal each body, sign, keep the signed bytes, send, confirm the position), the saved retry store (`session/retry`), and the apply of a downloaded entry (`database_replay/{apply,payload,accepted_record,chain,page,types}.rs`: the body types, the file preparation before the commit, the last-writer rule of `RecordReplay::attempt`). Not brought back: `journal.rs`, and the signing at commit.
- Deleted for the personal database: the sealed mirror and the sealed bundle path (`mirror_sealed.rs`, `blobs.rs`), `read_streams` (every stream from entry 1 at every turn), the use of `authored.json`, `uploaded.json` and `received.json`, and the in-memory history. Shared databases keep `VerifiedHistory` and their files and behave as before.

### A write

**Replaced on 23 November 2026.** The store now signs a write, as the next entry of the device stream, in the transaction that commits it. The spool holds the signed entry and its sealed file, the sender only sends, and `outgoing.json` and `outgoing-blobs/` are gone. [The home on disk](../home-format.md) describes it as built. What follows is the earlier design. Every commit lands in the spool, in order, before the caller is told. The sender takes the entries past the confirmed position (at most 90 per append and about 8 MiB of payload), opens each canonical body with the content key, seals it with the database read key bound to the position it will have, and signs it as a `data` entry that cites the birth. One write is one entry. A schema put or drop is an entry of the same kind. A file a record names goes up through `blob.put`, sealed, before the entry that names it.

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

**Replaced on 23 November 2026.** A commit now writes the stamp rows of each record it changes, in its own transaction: the order value is the millisecond of the author time, which the clock gives to one captured write only, and the sequence number is the write's number in the spool. Nothing in the store changes when the write is sent, and there is no pending marker, baseline or settle. A remote entry is compared with the stamp rows part by part (fields, file, inline file) when it arrives. The rest of this paragraph is the earlier rule. A commit sets the store's pending marker and baseline for each record it changes. A remote entry that arrives while a local write waits is kept as the baseline and does not replace the visible record. When the local entry is signed, the sender settles it with the same last-writer rule: if no newer write won, the local write becomes the winner and the marker is cleared. If a newer write won while it waited, the winner replaces it. The settle repeats safely, so a crash in the middle of a batch is repaired by running it again.

### The order rule

The order value (the wire field `lamport`) is the time of the commit in milliseconds, from the store's author clock. The author clock is raised past every remote entry that is applied, so a write made after an entry has a higher value. A schema put or drop takes its time from the same clock. The sender signs the larger of that time and one more than the previous entry of the stream, so the value rises on every stream.

`AcceptEntry` accepts an order value that is above the previous entry of the stream and above every cited entry, and below `LAMPORT_LIMIT = 4_102_444_800_000` (2100-01-01 in milliseconds, far below the point at about the year 2540 where the author clock would overflow). The rule is the same for every database. A shared-database author signs the larger of its clock and one more than the highest value it has seen, so its values stay causal. Only an applied entry raises the author clock, so a member's proposal does not.

### Capture

A home captures every write exactly when something sends the spool: it has a relay (`LASTDB_RELAY_URL` or saved cloud credentials), it already has a spool, or it is a restore. The owned and received stores of shared databases capture nothing, because nothing sends a spool for them.

### What CI proves

`scripts/operations/personal_database_proof.py` (job `personal_nodes`): one entry on the relay for each write; a second device and restored homes from the phrase alone; the later edit wins whichever device syncs first, with the earlier writer holding more writes; a deleted record stays deleted when an older edit arrives after it; alternate keys and a file read on three devices with the relay stopped; no key, value or schema name on the relay; an idle turn reads nothing and a restart replays nothing; kill -9 under write load loses and repeats nothing; and tens of thousands of writes with bounded peak memory and an emptied spool.

### Deferred

Steps 5 and 6: the move of an existing home (section 11), and the relay dropping entries a snapshot covers (section 14). Shared databases still use the in-memory history. The pure `mutation_log` crate still carries the `bundle` entry kind and the opener of sealed payloads, which only the relay end-to-end tool uses now. The relay's contiguous head (`log/streams`) is used for the listing only; a read that returns fewer entries than the head reports is an error.

## 10. What was built for step 4

### The producer, brought back

- `fold_db/.../storage/disk_backup/snapshot.rs` and `files.rs`, and `local_file_ref::{snapshot_budget, snapshot_rows}`, come back from `d06054159`. `DatabaseSnapshot` freezes the main and file roots with the node marker and lists the native files; a local file reference is sealed into a private copy of the files store, as before. `DbOperations::capture_synced_snapshot` and `Metadata::fork_restored_author` come back too. `adopt_restored_snapshot` only forks the author stream now.
- Changed: the capture also reads the head rows (`signed-replay-head:*`) of the store, in the same section that pins the files. It refuses (returns `None`) unless the spool's acknowledged position equals its head and no `signed-replay-pending:` or `signed-replay-deferred:` row exists. The write gate (`enter_write`) holds while the roots are pinned and the rows are read, so no commit starts or ends in between, and the cut is exact: the files hold exactly the writes that the head rows name.
- Not brought back in this step: `prearm.rs`, `Status.snapshot_required`, `note_accepted_snapshot`, `Error::Snapshot`, the `capture_initial_snapshot` option, `clear_restored_bootstrap_pending`, `release_bootstrap_replay_pins` and `live_file_paths`. They pin the exact files of a home that had data before capture began, and refuse to acknowledge before a first snapshot covers that history. That is the move of an existing home. The snapshot of this step covers the whole store at the freeze, so it needed none of them. Section 11 brings them back.

### The `snapshot` entry

A new entry kind in `mutation_log`, signed by the owner. The kind string is `snapshot`, the domain `lastdb-snapshot-v1`. The body is `manifest_sha256`, `manifest_bytes` and `through`, a list of `{device_id, writer_seq, entry_hash, order}` for each stream the snapshot holds. The exact bytes are in `relay-protocol.md`. It sits on a stream of its own, whose device id is `HKDF-SHA256(node seed, info = "lastdb:personal-database:v1:snapshot-device")`, like the birth device. Devices skip that stream when they apply entries. The reference relay needed no change: it stores a publisher entry of any kind it can deserialize.

### Publishing

After a clean turn, when a snapshot is requested or due, the node captures, hashes each file, asks the relay what it holds (`blob.list`), uploads each file the relay lacks (by digest and length) with the file-streaming `blob_put`, uploads the manifest, appends the entry, and asks the relay again that every file is there (section 16). A crash before the entry leaves unused blobs, which the next sweep drops. A second device that signs on the snapshot stream at the same time gets a refused append and tries again on the next turn.

When: on request (`lastdb backup snapshot [--wait]`, route `POST /api/backup/snapshot`), and automatically when 25,000 entries (`AUTO_SNAPSHOT_ENTRIES`) have been signed on this device's stream since its last snapshot. The old build took a snapshot on request and for the first one; the retention signal of the cloud no longer exists.

### Restore

`lastdb backup restore` lists the owner's streams. When the snapshot stream has an entry it reads the newest, checks the signature, database, stream, cited birth and the order limit, fetches the manifest (digest and length checked), fetches each file (digest and length checked) into `data/` of the staged home while no store is open, boots on the files, forks the author stream, checks that the head rows are exactly the positions the entry names, and reads each stream after its position; the first entry of each stream is checked to chain to the head hash by the ordinary incoming check. The staged home is published by one rename, so a kill leaves no home or a complete one. The staged install of `native_reset` (exchange of a live home) is not needed for a new home and was not brought back.

### What the relay can see

The files are the store's own: AES-256-GCM frames with keys derived from the node seed (`laststore` run frames, `CURRENT` sealed, the node marker is a fixed constant). They are uploaded as they are. The relay sees file names (run uuids), lengths, digests, and the stream positions in the entry.

## 11. What was built for step 5

A home that already holds data and starts syncing publishes its existing files as the first snapshot, and treats no earlier write as uploaded until that snapshot is named on the relay. The owner's home, written by build `d06054159`, was the first such home. It moved once, on 2026-11-23, and the tooling for that move was deleted. The database format does not change (`HOME_FORMAT` stays 15), and the product holds no code that reads an old layout.

### The problem

Step 4 takes a snapshot at a freeze with the spool empty and acknowledged. A home that has data and no spool starts capturing at its first write after the relay is configured. Two things go wrong with that. The data from before is in no entry and in no snapshot, so a restore from the relay would silently lack it. And while the first snapshot waits for the spool to drain, the spool would have to be sent first, which puts entries on the relay with no base under them.

### The pieces that came back, and how they were adapted

| Piece (from `d06054159`) | Now |
|---|---|
| `prearm.rs`: stage a private copy of the two native roots and pin it | Back, in `disk_backup/prearm.rs`. The copy is hard links to the immutable run files and copies of the few small files, so it is quick, it holds no extra bytes for the runs, and it survives a restart. It is taken by `PackRuntime::begin_first_snapshot` at boot, and not by a hook while the store opens, because the markers of the earlier upload path must be cleared first and that needs the running store |
| `State.snapshot_required`, `Status.snapshot_required`, `Error::Snapshot`, `note_accepted_snapshot` | Back in the spool. The spool starts with the flag set when the home held data (`history`) and refuses to confirm an upload, and the node does not read it, until the flag is cleared. The field is read as false when absent, which is every state written since step 4 |
| `capture_initial_snapshot` | Back as a method that opens the preserved copy. The store option is not needed: the host decides by whether the home existed before this start |
| `clear_restored_bootstrap_pending`, `release_bootstrap_replay_pins`, `live_file_paths` | Back, as `Metadata::clear_unsettled_replay` and its helpers. See "Rows of the earlier path" |
| `bootstrap.rs` of the node (the saved bootstrap record, `resume`, `complete_or_await_upload`) | Not back. The saved `snapshot-last.json` record of step 4 makes the publish idempotent, which is what `resume` did |
| The first-backup intent file | Not back. Nothing asks for the first snapshot: the home has data and no spool, and it has a relay |

### What happens

1. Boot of a home that existed before and has no spool, with a relay configured. Before any write is accepted, `begin_first_snapshot` clears the markers of the earlier path, flushes, takes the write gate, checks that the spool still holds no entry, stages the copy of both roots, and writes the spool state with `snapshot_required` and the frontier of the copy. A failure stops the boot with a message, because a capture that began without the copy could never cover the data.
2. Every later write enters the spool, as in step 4. The spool is not read for upload.
3. The sync turn sees the flag and runs `run_first` before anything else. It signs the birth if the relay lacks it, hashes the files of the copy, uploads the ones the relay lacks, uploads the manifest, and appends a `snapshot` entry whose `through` is empty: the copy holds no stream position, because no entry existed when it was cut.
4. The node writes `snapshot-last.json`, records the snapshot in the spool (`note_accepted_snapshot`, which clears the flag), and removes the copy. From then on the spool is the upload queue, and the turn goes on to the sender in the same turn.

Where a write is: a write made before the copy is in the snapshot and not in the spool, because the spool was empty at that moment. A write made after the copy is in the spool and not in the snapshot, because the files of the copy are immutable. The gate makes the moment sharp, and the check of `head == 0` under the gate proves that nothing slipped in. So no write is lost, and none is sent twice.

A restore from this snapshot installs the files, forks the author stream, and reads every stream from its first entry, because `through` is empty. That is exactly the writes since the copy. The restored home records the snapshot as covering its history (`adopt_restored_snapshot` calls `note_accepted_snapshot`), or its own spool would wait for a snapshot that nothing will take.

### Crash windows

| Killed | The next start |
|---|---|
| Before the copy is staged | No spool yet. `begin_first_snapshot` runs again from the same files |
| After the copy is staged, before the spool state is written | The same. A copy that matches the frontier is kept, and any other is cut again, because no entry exists yet |
| While the upload runs | The copy and the flag are there. The turn runs `run_first` again and uploads again. Blobs are named by digest, so a second put of the same file stores the same bytes again (the reference relay renames over the old file), and nothing is named until the entry |
| After the entry, before `snapshot-last.json` | The turn publishes again. A second `snapshot` entry with the same empty `through` is on the relay. It is harmless: both name the same state, and the newest wins |
| After `snapshot-last.json`, before the flag is cleared | The turn finds the saved record, names nothing, clears the flag and removes the copy |
| After the flag is cleared, before the copy is removed | `begin_first_snapshot` removes the leftover at the next boot |

### Rows of the earlier path

**Removed on 23 November 2026.** No build writes these rows any more, a snapshot no longer looks for them, and `Metadata::clear_unsettled_replay` is gone. The rest of this paragraph is history. A home that was connected to the earlier cloud backup can hold `signed-replay-pending`, `-baseline`, `-pending-attachment` and `-deferred` rows, and the file pins that go with them. They belong to writes that the old cloud never acknowledged and to remote changes that waited behind them. No entry of the signed log will ever settle them, and they do real harm: a snapshot is refused while any pending or deferred row exists, so the home would never take a second snapshot, and a remote change to a record with a pending row would wait for ever. The old function that cleared them for a restored home fits, and it is used as it was: it keeps every record value and the `winner` stamps and drops only that state. It runs at the one moment when it is safe, when the spool is empty, because a pending row of a write that is in the spool must stay until that write is signed. The copy is cut after it, so the snapshot carries none of them and a restore needs no special path. A record written before keeps its winner stamp, whose time is a real millisecond time, so every entry the new build signs, stamped with a later time, still wins over it.

### The id records of the spool

The spool keeps one small file for each accepted write in `ids/`, named by the mutation id and holding the SHA-256 of the body. They have one use: a write presented again with the same mutation id and the same body is not added to the spool twice, and the same id with another body is refused. Nothing else reads them (`accepted_mutation_seen` has no caller). The spool never removed them, so after 20,000 writes a synced spool was only id records, and the owner's old spool held more than 100,000.

They are now kept for the newest 10,000 writes (`IDS_KEPT`) and for every entry the relay has not acknowledged, and the directory is trimmed every 2,500 acknowledged entries and at open. No format changes: a record is still a file with the same name and content. A client that retries a write older than the newest 10,000 accepted writes is taken to make a new write: it is applied, entered into the spool and signed again with the same body, which is the meaning of a retry that arrives that late. The old record would only have saved one duplicate entry. The scale step of `personal_database_proof.py` asserts that the directory stays below the bound.

### The move of the owner's home

The owner's home was written by the old build and moved to this build once, on 2026-11-23. A one-off script, a CI job that ran the old build, a runbook and the command `lastdb admin identity` served that move only. They were deleted after it. What stays is the general path above: any home that holds data and gets a relay publishes its first snapshot.

### What CI proves

Job `existing_home` of `signed-history-dev.yml` (`scripts/operations/existing_home_proof.py`) starts the current build on a home with no relay, writes records in three schemas (an alternate key and a file in one), stops it, and starts it again with the reference relay. It proves: the first snapshot is named once; every record reads by primary key and alternate key and the file's bytes match; the relay holds one entry for each write after the snapshot and none for the earlier records; a restore from the recovery phrase installs the snapshot, replays only the later entries and equals the home key by key; the relay holds no marker. The kill windows before the snapshot is named, a second device, and the large-file upload are no longer proven by CI: they were proven on the old home with the old build, and no other job reaches them.

### A limit found on the way

A record can hold a file by path (`data/local-file-refs.json`), and a snapshot seals each such file into its private copy of the files store as one value. A run holds a value of at most 32 MiB (`MAX_RUN_VALUE_BYTES`), so a home with a referenced file larger than that cannot be snapshotted. This is true of the snapshot of step 4 as much as of the first snapshot. The first snapshot makes it visible at boot, where it stops the node with a message, which is the safe answer: a capture that began without the copy could never cover the data. The product still lacks a way to snapshot such a file. Chunking the sealed value would change the files store, so it is not done here.

### What CI cannot cover

The size of a real home, any `pending` row an earlier cloud backup left, the apps installed on it, and the real relay (Exemem).

### Deferred

A second snapshot entry after a kill at one window is left on the relay, harmless and small. Step 6 (the relay drops the entries a snapshot covers) is section 14.

## 12. Where the snapshot is: status, and a file the relay refuses

### What `lastdb status` says

After the `Cloud sync:` line, `lastdb status` prints where the newest snapshot of the personal database is. The node does not call the relay for it. It reads `snapshot-known.json` (beside `snapshot-last.json`, outside the store), which the node writes when it publishes a snapshot and when a restore installs one. The daemon reads it at boot, so the line is right after a restart.

| State | Line |
|---|---|
| A snapshot is known | `Snapshot: 2026-11-22 14:03:11 +0000, 7 files, 1.4 MiB, on https://relay.example, covers this device through entry 311` |
| None yet | `Snapshot: none yet` |
| The first snapshot of an existing home is being published | `Snapshot: none yet, the first snapshot of this home is in progress` |
| A snapshot cannot be published | the line above for the newest known snapshot, then `Snapshot blocked: <message>` |

The time is local, from the signed time of the `snapshot` entry. The relay is its origin. "Through entry N" is the last entry of this device's own stream that the snapshot covers. It is 0 for a snapshot that holds no entry of this device: the first snapshot of a home that held data before it began to sync, and a snapshot that a restore installed for a new device. The line is always printed, because a node can sync through a relay address in its environment and then has no saved cloud connection, so no `Cloud sync:` line.

`/api/status` carries the same facts as a new object, `cloud_sync.snapshot`. Existing fields of `cloud_sync` are unchanged.

| Field | Meaning |
|---|---|
| `state` | `none`, `in_progress`, `published`, `installed` or `blocked` |
| `taken_at_ms` | Signed time of the `snapshot` entry (Unix ms) |
| `files`, `bytes` | Files in the manifest and their total length |
| `relay` | Origin of the relay that holds it |
| `manifest_sha256` | Digest of the manifest the entry names |
| `through_seq` | Last entry of this device's own stream that the snapshot covers (0 if none) |
| `snapshot_seq` | Sequence number of the `snapshot` entry |
| `streams` | Device streams the snapshot covers |
| `message` | Why a snapshot is blocked, else null |

A field is null while the state is `none` or `in_progress`.

### A file larger than the relay accepts

The Exemem relay takes at most 5 GB for one blob (5,000,000,000 bytes, the documented limit of one upload to its object store; it was 64 MiB until 2026-11-23, when the relay stopped reading each upload and the object store began to verify the length and SHA-256 itself) and answers a larger `blob.put` begin with HTTP 400, code `invalid_request`, field `bytes`. An upload near that size has not been run against Exemem; CI moves a 512 MiB file through the reference relay. The reference relay has no limit unless it starts with `--max-blob-bytes`, and then answers the same way (`relay-protocol.md`). The node knows this answer when it comes from the begin of an upload of a file with a length above zero. It does not know the limit, because the relay does not say it.

The snapshot then stops. The report is the one in the status (`state` `blocked`, `message`), in the daemon log as one WARN event (the log of the daemon carries severity and category only, never message text, so the words are in the status), and, for a request, in the error of `lastdb backup snapshot --wait`. It names the run file (the file name only, no path and no content), its length, the relay, says that the relay's limit on one file is smaller, and that the snapshot can be published when the limit is raised or the node can send large files in parts. Large files are not split in this step.

- A refusal is not a failed sync turn. Entries upload and ordinary writes go on. The next automatic attempt waits ten minutes (in memory; a restart clears it), because each attempt freezes and hashes every file. `lastdb backup snapshot` always tries.
- The first snapshot of a home that held data before it began to sync is the one exception to "entries go on": until it is named on the relay the spool is not read for upload (section 11), so entries wait. The turn returns the message as its error and does not start a new attempt for ten minutes.
- A success clears the state.

### What CI proves

`personal_database_proof.py`, step 15: a fresh home says `Snapshot: none yet`; after a snapshot, the line and the object equal the report and the signed entry on the relay (time, file count, bytes, relay, digest, own sequence); a home restored from it reports the same snapshot as `installed` with through entry 0; against a reference relay started with a 64 KiB limit, a home with larger data files gets one refusal, the message above in the status, one new WARN event in the daemon log, no second request in twelve seconds of sync turns, and its entries still upload. CI cannot cover the real Exemem relay's answer.

## 13. What is proven against the Exemem DEV relay

Job `deployed_relay` of `signed-history-dev.yml` runs `personal_database_proof.py --relay-url` against the deployed DEV relay. The URL is the `exemem_relay_url` input, the variable `EXEMEM_RELAY_URL`, or `environments.dev.exemem_api`. It runs on pushes, on the daily run and on dispatch, and on a pull request only when the PR changes the proof, its node helpers, the blob transfer or the workflow.

**A fresh identity needs no account.** The relay checks a request by its node-key signature alone. The first `birth` entry of a database names its publisher, and the relay bills the account that the publisher's node key derives (`derive_user_hash`), creating the billing row on the first charge with the default quota. In DEV `REQUIRE_PAID_STORAGE` is false, so no subscription is read. `lastdb admin backup init` makes a seed offline, so every run has a new node key, a new personal database id and a new account row, and restores nothing of an earlier run. No API key, account or secret is used. (In PROD the relay refuses a publisher with no active subscription.)

Proven against the DEV relay, with real nodes:

- Device A writes singles and batches in three schemas, an alternate key and a file. The relay confirms every entry (spool head equals the confirmed position, spool empty).
- Device B restores from the recovery phrase alone, reads one device stream, replays every entry, and holds every record, alternate key and the file.
- A and B, each cut off from the relay, edit one record apart. The later edit wins whichever device syncs first.
- A snapshot (`backup snapshot --wait`) names a checkpoint. After more writes, device C restores from the phrase alone, installs the snapshot, replays exactly the entries after it, finds the device streams of A and B, and equals A and B. C and A then exchange writes.
- A snapshot by a device that lost its record of the last one (`snapshot-last.json`) uploads no file: the node asks the relay what it holds (`blob.list`) and uploads only what is missing. Device D restores from that snapshot and replays nothing. The relay also answers `blob.put` begin for a digest it already holds with no `upload`, and the node treats that as stored.
- A streamed file upload carries one `content-length`. The Exemem relay signs `content-length` and returns it among the upload headers, and the node used to add it a second time, so the object store answered HTTP 400 and the entry that names a file stayed unconfirmed. The reference relay returns no `content-length`, so only the DEV relay showed it.

Not proven against it, because the relay's process, request log and disk are not readable: that the relay holds no key, value, schema name or marker, one entry per write by listing, a restore by full replay from a relay with no snapshot, idle turns reading no entry, reads of a restored home with the relay stopped, the kill before the first snapshot is named, the kill of a restore, kill -9 under load, and the scale run. These stay on the reference relay. The reference relay differs from Exemem in: a repeated `blob.put` begin (reference issues a new upload, Exemem answers with no `upload`), a 5 GB limit on one blob, billing of the publisher's account with a quota, and the presigned upload and download URLs of the object store. Neither relay refuses an entry beyond a gap (`relay-protocol.md`, `log.streams`). Of the storage a run adds to the DEV relay, the trim of section 14 drops the log entries a snapshot covers and the sweep of section 16 drops the files that nothing names. What the last snapshot of a run names stays there.

## 14. What was built for step 6: the relay drops what a snapshot covers

Basis: Tom, 2026-11-22: reuse the previous accepted machinery. The build before signed history (`d06054159`) already kept a covered log prefix and dropped it later, and this step keeps its shape and removes its separate state.

### What is reused, and what is not

| Previous machinery (`d06054159`, `log_sync/retention.rs`, `automatic_retention.rs`, `blob_retention.rs`) | Here |
|---|---|
| The relay holds a reclaimed position (`reclaimed_through`) and moves it only from the value the caller expects (`expected_floor`) | The relay holds a floor for each stream (the last dropped seq and its hash) and moves it in the same transaction as the delete, only from the floor it just read |
| A snapshot names what is covered (`snapshot_id`, `snapshot_through`); the relay checks the request against the snapshot it holds | The request names one `snapshot` entry. The relay reads that stored entry, verifies its signature and takes every position from its `through` list. No position is in the request |
| The relay reclaims a bounded number of steps for each request and the caller repeats (`max_steps`, `RECLAIM_BATCH`) | The relay drops at most 1,000 entries in one call and answers `done: false`; the node repeats |
| Blobs are reclaimed after the log they belong to, in batches (`reclaim-blobs`) | A trim drops log entries only and never a blob. The files that the newest snapshot and the entries after it do not name go in the sweep of section 16, in batches of 25 (`blob.drop`), after the log they belonged to |
| A device behind the snapshot cannot read the reclaimed entries and must upgrade (a clear failure) | A device behind gets 410 `trimmed` and must restore into a new home (a clear failure) |
| Not reused: the local work files (`snapshot-reclaim.json`), the timers that resume cleanup, the `backup reclaim` command and its enable flag, the per-device cursor that held a reclaim until the device had read through | For the trim there is no state on the node, no timer and no flag: the relay's floor is the only state, and one call after a snapshot is the only trigger. The sweep of section 16 keeps one small mark (`reclaim-due.json`) from the snapshot to the turn that finishes it, and `backup reclaim` asks for a sweep now |

### The rule

- **When.** After the node has published a snapshot. A snapshot entry is `n`, and the node calls `log.trim` naming entry `n`. The relay therefore keeps one snapshot, with every entry after its positions (Tom, 2026-11-24: "just keep the latest one"; it was the two newest until then). A snapshot that finds nothing changed calls nothing. A call that fails is logged and is not an error of the snapshot: nothing was lost, and the next snapshot asks again with a newer entry.
- **What one snapshot costs.** A restore uses the newest snapshot, and there is no older one to fall back to. A device that has not read up to the positions of a snapshot when it is named is behind the trim and restores into a new home. It had the time between two snapshots to catch up before.
- **Who.** Only the owner's node key. The personal database only: every entry that would go must be a sealed `data` or `schema` entry (or a `snapshot` entry on the snapshot stream), else the call stops with 403 `namespace`. A shared database is never trimmed, because its members read the publisher's streams from the start.
- **What the relay checks.** The named entry is stored, is a `snapshot` entry of the publisher on that stream, and its signature verifies. For each stream in `through`, the entry stored at the position hashes to the `entry_hash` the snapshot names, and the relay holds at least that many entries. Otherwise the call fails (400 `through`) and drops nothing from that stream.
- **What goes.** The entries of each stream up to its position, and the snapshot entries older than `n`. The `birth` entry is on its own stream and never goes. **No blob goes in the trim.** The manifest and the files of a dropped snapshot entry, and the sealed files of dropped entries, are then named by nothing, and the sweep of section 16 drops them. A snapshot cannot come to name a file that the sweep just removed: the node asks the relay again after it appended the `snapshot` entry and uploads what is gone, and the relay drops nothing once the head of the snapshot stream moved.
- **What stays.** The head of each stream, so `log/streams` works as before and the next entry in order still advances the head. A stream that was trimmed is listed with its real head. A device that is past the positions reads from its own head and does not notice.

### A device behind the trim

A device whose head on some stream is below the floor of that stream cannot read the entries after its head, because the entries in between are gone. The relay answers a `log/read` of one device stream that starts at or below the floor with 410 `trimmed`. The node reports `this device is behind history that the relay dropped after a snapshot; restore the database into a new home with lastdb backup restore` as the error of its read of the other devices' streams. That error is in the status (`cloud_sync.databases.<id>.download.last_error`, and the top-level `last_error`), not only in a log that carries no text, and it clears after a clean read. The device applies nothing. This is the simpler choice: the other, installing the snapshot into a running home, needs the staged install with the store closed that section 10 chose not to bring back for a new home. The restore it asks for is the ordinary restore: the newest snapshot, then each stream after its positions, which the trimmed relay still holds.

### Billing (Exemem)

The stored-bytes counter of the publisher (`storage_used_bytes`) goes down by exactly the stored length of the log entries that were deleted. Each batch of entries goes in one DynamoDB transaction with the floor write and one counter update. A trim neither deletes nor releases a blob; the sweep of section 16 does, and the counter goes down by the stored length of each blob it drops, in the transaction that deletes its record. A delete is conditional on the item existing, and the counter update on the counter being what the call read. A transaction that loses a race changes nothing, so nothing is subtracted twice, and a repeat finds the floor already moved. The counter does not go below zero.

### Races, for entries (the races of the files are in section 16)

The question: can any ordering lose a log entry, or make a restore of a retained snapshot fail? Four orderings, with the code that decides each.

1. **What a position means.** A snapshot is cut only when the spool's acknowledged position equals its head (`capture_synced_snapshot` returns `None` otherwise), and the head row of another device's stream moves only when `incoming` applies an entry that it read from the relay. So every entry at or before a position in `through` was already stored on the relay when the snapshot was taken, whatever stream it is on. The relay also checks, before it drops anything, that its contiguous head is at least the position and that the stored entry at the position hashes to the `entry_hash` the signed snapshot names.
2. **An entry of device B at or before a trimmed position that reaches the relay after the trim** (B was offline and uploads late). By 1 it cannot be a first arrival: the snapshot holder could only have B's entry from the relay, so the relay already took it. What can arrive is a repeat of an append whose answer B did not see. The relay answers it as a duplicate and stores nothing and charges nothing. So the relay does not refuse it and the node does not treat it as behind. The floor is part of admission, atomically, not a read followed by a write:
   - **Exemem.** Every append transaction carries a `ConditionCheck` that the stream's floor item is absent or `floor_seq < :seq` (`trim::floor_admission`, pushed in `log.rs` `append_entries`). The entry put, the head move, the charge and the check commit together or not at all. A trim and an append of the same sequence number also touch the same entry item, so DynamoDB serializes them: either the append commits first and the trim then finds and drops it (it queries the entries after the floor), or the trim commits first and the check fails. When the transaction fails and the entry is absent, the relay re-reads the floor and answers a duplicate if it is at or above the number (`is_dropped`), else it is the existing retryable conflict. The cheaper head check that runs before the transaction only avoids the work; it is not what makes it safe.
   - **Reference relay.** `append_one` writes the entry file and then looks at the floor; if the floor is at or above the number it takes its own file back and answers a duplicate. A trim sets the floor first and then lists and removes the files at or below it (`trim.rs` `apply` lists again after setting the floor). If the append looked before the floor was set, the floor was set after the file was written, so the trim's listing finds the file. No order leaves an entry stored below a floor.
   - **Why a duplicate cannot mislead a caller.** A floor is only ever set from stored entries, and a head only counts stored entries, so an entry at or below the floor was taken by the relay. The only honest caller of that number is repeating an append already taken, and a duplicate is what the relay gives for any stored entry. A different entry that a forked B signs at that number is also answered as a duplicate and not stored; the relay cannot compare it, and nothing reads it, because a restore trusts the owner's signed snapshot and every reader stops at the floor. An entry above the position is accepted as before: the head row stays, so the gapless append is the same. A stream that no snapshot names has no floor and nothing changes for it.
   - **Reads.** A read of one device stream at or below the floor is 410 whether or not an item exists there (Exemem checks the floor first in `read_one_device`; the reference relay in `read_entries`), so a stored entry can never be returned from below a floor.
3. **A device that reads while another trims.** The reader reads each stream from its own head plus one. If that is at or below the floor the relay answers 410 `trimmed` and the reader applies nothing more and reports that it is behind (`incoming.rs` `Cursor::fill`, `relay.rs` `read`). If a trim lands between two pages of one run, the next page answers 410 the same way; the entries already applied were valid and stay. A reader whose head is at or above the floor never reads an entry that a trim can drop.
4. **A device that appends while another trims.** Both act on one stream only above the position (the appender) and at or below it (the trimmer), on different items; the trim moves only the floor item and deletes entries at or below the position, and the append moves only the head. The Exemem transaction of a batch is conditional on the floor it read, so two trims cannot both release the same bytes.

The restore of the snapshot reads each stream after its position, and nothing after a position is ever dropped, so it finds all it reads. I found no ordering that loses an entry. The only effect of a late call is a clear 410 for a device that is behind, and a duplicate answer for a repeated append.

### Left as it is

- **All-devices read.** A read of every device of a writer returns the entries that are stored and does not say that some were dropped. The node uses one-device reads; a shared database is not trimmed.

### Proof

`personal_database_proof.py` step 16 runs after the snapshots of steps 11 to 14, against the reference relay: the relay holds the second snapshot entry alone and no entry at or before its positions, on any stream, and keeps the birth; a device that stopped before those positions gets the error above and applies nothing; a restore from the phrase installs the newest snapshot, replays only the entries after it, and equals the source key by key, alternate keys and file included, with the relay restarted in between (the floor survives a restart); a repeat of an append at and below the floor (signed as A, with entries the proof read before the trim) is answered as a duplicate, is not stored, and a read there is 410; a third snapshot drops the second snapshot entry, its manifest and the files that only it named, and a restore from snapshot 3 equals the source; the devices that read up to each cut hold their state and never report that they are behind. Every running device reads up to the cut before the proof takes a snapshot. The full replay of step 11 runs against a copy of the relay from before the first snapshot. Against the Exemem DEV relay the same snapshots call the routes; the run proves the restores, and asks the relay what it holds through `log/read` and `blob/list` (section 16).

## 15. The snapshot upload does not hold the sync turn

A snapshot of a large home uploads tens of megabytes. Inside the turn, no entry was sent or acknowledged and no other device was read until the last file was up, which on a home uplink took minutes. Now the turn only freezes the snapshot. The slow part runs in one lane beside the turns.

**In the turn** (`snapshot::run`): after a clean turn, when a snapshot is requested or due and no lane runs, the store freezes the files (`capture_synced_snapshot`). It still needs the spool acknowledged and the replay settled at that one instant, so it stays in the turn, and it takes the head rows in the same hold of the write gate. The turn then marks the status `in_progress` and starts the lane. Nothing else changes in the turn.

**In the lane** (`snapshot::upload`, a task in `snapshot_lane.rs`): hash the pinned files, ask the relay what it holds, upload what it lacks, upload the manifest, read the head of the snapshot stream again, sign and append the `snapshot` entry, ask the relay again and upload what is gone (the files stay pinned until here), write `snapshot-last.json` and `snapshot-known.json`, ask the relay to trim, mark a sweep as due, answer the request. The lane starts from a turn and ends by itself. It has no timer and no queue.

**What the entry claims does not change.** `through` is read from the head rows at the freeze, and `own_seq` is taken from it. Neither reads the live store later, so entries that the turns send, and entries that they apply from other devices, after the freeze are not claimed. Those are the tail a restore replays. The pinned files do not change however many writes follow.

**One snapshot at a time.** The lane slot is empty or full. A request that arrives while the lane runs is not joined to it: its cut is older than the request, so a person who waited for "a snapshot after my writes" would get one that lacks them. The request file stays. The turn after the lane ends freezes a fresh cut for it (an unchanged cut names nothing twice). While the lane runs, `pending` does not count the request, so the loop does not spin.

**Shared state, and how each piece is protected:**

| State | Written by | Protection |
|---|---|---|
| The spool, `outgoing.json`, the own head row | the turn only (`outgoing`) | The lane never touches them. |
| The snapshot stream (the `snapshot` entry) | the lane, or the first snapshot inside the turn | The first snapshot runs only while `snapshot_required` is true, and the freeze of a regular snapshot refuses in that case, so the two never overlap. The lane reads the stream head again right before it signs. |
| `snapshot-last.json`, `snapshot-known.json`, the cloud sync status of the snapshot | the lane (the turn only reads) | One lane. The files are written with a rename. The turn reads `snapshot-last.json` only when no lane runs. |
| `snapshot-request`, `-result`, `-error` | `request` (the CLI), then the lane | The lane retires or fails a request only when the file still holds its nonce, so a newer request keeps its file and gets its own answer. |
| The native cloud work guard | taken in the turn at the freeze, held by the lane | A second freeze fails while it is held. |
| The refusal memory (`REFUSED`) | the lane | One mutex. |

**Stop.** When the daemon stops, the sync task aborts the lane and waits for it (`stop_snapshot_lane`). The lane has no await between the append of the `snapshot` entry and the writes of its records, so a stop never falls between the relay naming the snapshot and the node knowing it. Files already committed on the relay are skipped the next time. The request file stays, so the next start takes the snapshot again. **Kill -9** has the same effect on disk: the spool, the saved entries and the stream are untouched, nothing is signed twice, and the same window as before remains between the relay taking the entry and the node writing its record (a retake then signs one more `snapshot` entry; it adds no data entry).

**Status.** While the lane runs, `cloud_sync.snapshot.state` is `in_progress`, and `message` says `uploaded N of M files, X of Y bytes` (before the first file: `hashing the files`). `lastdb status` prints `Snapshot uploading: <message>` below the line of the newest snapshot. A failure puts the earlier state back, and a relay limit sets `blocked` as before. `lastdb backup snapshot --wait` keeps its meaning: it returns when the request is answered, after the entry and the trim. The first snapshot of a home that held data before it began to capture still runs inside the turn, because the spool is not sent until it is named.

**Proof.** `personal_database_proof.py` step 20 (`personal_proof_lane.py`): the reference relay reads a large upload at 24 KiB/s, so a snapshot with a 500 KB file uploads for about 20 s. During it W's writes are acknowledged and read by X before the relay holds a snapshot entry; W is killed with -9 in the first upload and loses and repeats nothing; the second upload takes writes the same way; the published entry names W through the last entry acknowledged at the freeze, and W signed more after it; a home restored from it replays exactly the entries after that position and equals W and X.

## 16. The cloud matches the local store: one snapshot, and a sweep

Basis: Tom, 2026-11-24: "You don't have to keep 2 new ones, just keep the latest one and make sure there's a way to reclaim the previous snapshots too." "Make sure that my cloud storage matches my local storage. There can be temporary disagreements [...] we're uploading a new snapshot and keeping the old snapshot until the new one has been verified." "Add that as a CI step so that we can make sure that the cloud doesn't grow beyond what the local has."

### The problem, measured

On 2026-11-24 the primary's store held 184 data files, 133.8 MB. The relay held 955 files, 1,157 MB, plus 75 MB of entries. A snapshot uploads every data file that changed, compaction rewrites data files, and the relay never deleted a file: `log/trim` drops entries only. A failed snapshot upload also left its files behind for good.

### The rule

After a snapshot is named and verified, the relay holds exactly:

1. the files and the manifest of the newest snapshot,
2. the entries after the positions of that snapshot (with the `birth` entry and that one `snapshot` entry),
3. the sealed files that those entries name.

Everything else is dropped and its bytes are released. While a new snapshot uploads, the relay holds the old and the new one. That is the only permitted disagreement.

### Who owns which fact

- The relay owns what is stored and what is charged. `blob.list` is the truth for "is this file on the relay". The node does not decide what to upload from its own `snapshot-last.json`.
- The publisher's node owns what is needed: the manifest that the newest `snapshot` entry names, and the entries after its positions, which it reads back from the relay.
- The relay reads no manifest and no file. It drops only the digests the publisher names, and only while the streams it was told about are as it was told (`blob.drop`, `relay-protocol.md`).

### The snapshot lane (`personal/snapshot.rs`)

1. Hash the files of the frozen cut.
2. `blob.list`. Upload each manifest file the relay lacks (by digest and length), then the manifest.
3. Sign and append the `snapshot` entry.
4. Verify: `blob.list` again. Every manifest file and the manifest are present with the right length. One that is missing is uploaded again from the cut, which is still pinned. Only then is the snapshot recorded as published. From the append to the record a mark is on disk (`snapshot-unverified`). If the check does not end (a stop, a kill, a failed transfer), the mark stays and the next clean turn takes a snapshot again, which uploads what the relay lacks and names a complete one. A restore reads the newest snapshot alone, so an incomplete one must not stay the newest.
5. `log.trim` to this snapshot. The relay drops the entries it covers and the older `snapshot` entries.
6. Mark a sweep as due for this snapshot (`log-sync/<database>/reclaim-due.json`).

### The sweep (`personal/reclaim.rs`)

It runs inside a sync turn, after the send step, when a sweep is due and the head of the relay's snapshot stream is still that snapshot, or when `lastdb backup reclaim` asked for one. It does not run while a snapshot uploads.

1. `log.streams`, then `blob.list` (all pages).
2. Read the newest `snapshot` entry and its manifest from the relay. Read every entry after the snapshot's positions, up to the listed heads, and collect every digest they name (`mutation_log::named_blobs`: the file of a data entry, the body of a bundle, the manifest of a snapshot). A read that cannot return every one of those entries (a gap, an entry out of place, a 410) stops the sweep, and nothing is dropped.
3. Needed = manifest files + manifest + those digests. Dead = listed minus needed.
4. Nothing is dropped unless the relay lists every file of the newest snapshot and its manifest with their lengths, checked against the listing of step 1 in this same pass. A snapshot with a missing file stops the sweep: the mark stays and says why, and the next snapshot marks again.
5. `blob.drop` in calls of 25, with the heads of step 1 as its condition. When nothing is dead the sweep is done: the mark is removed and the totals are recorded (`reclaim-last.json`, `cloud_sync.reclaim` of `/api/status`, the `Cloud storage:` line of `lastdb status`).

A snapshot this node named and has not verified (`snapshot-unverified`) is never trimmed and never gets a sweep mark: the trim and the mark come after the check of lane step 4.

What a drop released is counted from what the node observes. An answer with no absent digest is the count. Any other call stays written down (`reclaim-named.json`) until the next `blob.list`: a digest this node named that the relay no longer lists was released, with the length the earlier listing gave. A client that asks again after a lost answer therefore does not lose the count.

Because the sweep is a difference against a listing, it also removes the backlog: the files of every earlier snapshot, the sealed files of entries trimmed long ago, and the files of a failed upload.

When the relay refuses a drop (409 `heads`), the next turn lists again. A stream that keeps an entry above its head (an entry stored beyond a gap) is refused every time. After 30 turns in a row the sweep stops trying: the mark stays and records why, the status shows it (`Cloud storage sweep waits: ...`), a waiting request fails with the reason, and it is logged once. The next snapshot marks again, and `lastdb backup reclaim` tries again.

A database with more than 32 streams (the birth stream and the device streams) cannot name every head in one drop. The sweep then fails once with that reason and removes its mark.

### Why the heads are a condition

The one danger of a sweep is a file that is committed and not yet named: a device uploads the sealed file, then appends the entry. Two rules close it without a clock.

1. The device that names a file checks after it names it. After an append whose entries name files, and after the append of a `snapshot` entry, the node asks the relay again for each file and uploads any that is gone (`personal/send.rs`, lane step 4). The spool retires the entries only after that.
2. The drop is one step with the heads. If the entry was appended before the drop, a head moved and the drop does nothing. If it is appended after the drop, rule 1 puts the file back.

A stream that did not exist when the node listed the streams has no head in the drop. The relay therefore refuses a drop (409 `heads`) while the publisher has a stream that the call does not name. If the new device's first entry was appended before the drop, the drop does nothing and the next turn lists the new stream. If it is appended after the drop, rule 1 puts the file back.

### What a person can see

- `lastdb status` prints `Cloud storage: <N> files, <size> on the relay after the last sweep (the snapshot it kept: <N> files, <size>)` after the snapshot line. `lastdb backup reclaim --wait` runs a sweep now and prints what the relay dropped and what it holds.
- The account of what this device put on the relay (`cloud-usage.json`) also counts what the sweeps it asked for released (`released_blobs`, `released_blob_bytes`).
- A restore reads the newest snapshot only. A restore that is in progress while a newer snapshot is swept can lose a file under it. It then fails, and a repeat reads the newer snapshot.
- A device that is behind the newest snapshot's positions restores from the snapshot.
- There is one snapshot. If its files were damaged on the provider, the local database is the only other copy until the next snapshot.

### Proof

`personal_database_proof.py` step 23 (`personal_proof_reclaim.py`), against the reference relay and the Exemem DEV relay: rounds of overwrites, deletes and new files with a snapshot each, with data of the same size. After each round, once the sweep is done, the proof asks the relay through its routes (`log/streams`, `log/read` of every stream from its floor, `blob/list`, `blob.get` of the manifest):

- the relay holds one snapshot entry, and its stored files are exactly the files of that manifest, the manifest, and the sealed files that stored entries name, each with its length, so the stored bytes equal the manifest's file bytes plus the manifest length plus those sealed bytes;
- stored files plus stored entries are not above the native files of the writer's store on disk plus an allowance: the manifest, the stored entries, 29 bytes for each sealed file, and 64 KiB for a file the store rewrites between the measurement and the cut;
- from the first round to the last the relay grew by no more than the local files did, plus what that allowance grew and 64 KiB;
- each sweep released at least the sealed files of its round, and from the second round the manifest before;
- a `blob/drop` that the proof signs itself and that leaves one stream of the publisher out of `heads` is answered 409 `heads` and drops nothing.

The report of `lastdb backup reclaim --wait`, the `cloud_sync.reclaim` object and the `Cloud storage:` line must say what the relay says. On the reference relay the proof also reads the relay's disk: every stored object is one the database owns, and no pending record of an upload is left after its commit. Step 21 (`personal_proof_reconcile.py`) fails on any blob that neither the one snapshot nor a stored entry names, and holds the stored blobs against the accounts: blobs put less blobs released.

### Not in this change

- Shared databases have no snapshot, so nothing trims or sweeps them.
- No clock, no age threshold, no second snapshot, and no path for a relay without the routes: the relay is deployed before the node that needs them.
