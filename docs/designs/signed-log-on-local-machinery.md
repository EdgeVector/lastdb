# Design: the signed log on the previous local machinery

Date: 2026-11-22. Author: Claude, for Tom. Status: approved. Steps 1 to 5 are built: step 1 earlier, steps 2 and 3 (outgoing and incoming, the order rule) together in one change, because neither can be proven without the other, step 4 (the snapshot as the checkpoint), and step 5 (the move of an existing home). Sections 9, 10 and 11 say what was built and what was deferred.

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
2. **The relay holds the only copy of old entries.** If it lost them after a trim, the snapshot still restores the state.
3. **One signed entry per write.** A bulk import would make many entries. The move does not, because it publishes the existing files as the first snapshot.
4. **The primary's home.** #1843 refuses a home whose personal database id is not derived from the recovery phrase. The move maps the registry onto the derived id while the store files stay as they are (inferred that they do not embed the id). So the primary can move in place by a snapshot of its existing files, without copying every record through the write call.
5. **Exemem.** The relay needs the stream listing, and later the trim call. Its file routes use presigned uploads. Section 13 says what is proven against the deployed DEV relay.

## 8. Decisions

Tom approved the plan on 2026-11-22 ("All right, it's approved. Let's do it."), after it was put to him as: keep the cloud log as designed, keep the disk storage as it is, join them in one thin layer.

Settled by the code check after that, within the plan: the hybrid order of section 4a, and no per-entry row for a personal database.

Status: steps 1 to 5 are built. Step 6 is not.

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

Steps 5 and 6: the move of an existing home, and the relay dropping entries a snapshot covers. Shared databases still use the in-memory history. The pure `mutation_log` crate still carries the `bundle` entry kind and the opener of sealed payloads, which only the relay end-to-end tool uses now. The relay's contiguous head (`log/streams`) is used for the listing only; a read that returns fewer entries than the head reports is an error.

## 10. What was built for step 4

### The producer, brought back

- `fold_db/.../storage/disk_backup/snapshot.rs` and `files.rs`, and `local_file_ref::{snapshot_budget, snapshot_rows}`, come back from `d06054159`. `DatabaseSnapshot` freezes the main and file roots with the node marker and lists the native files; a local file reference is sealed into a private copy of the files store, as before. `DbOperations::capture_synced_snapshot` and `Metadata::fork_restored_author` come back too. `adopt_restored_snapshot` only forks the author stream now.
- Changed: the capture also reads the head rows (`signed-replay-head:*`) of the store, in the same section that pins the files. It refuses (returns `None`) unless the spool's acknowledged position equals its head and no `signed-replay-pending:` or `signed-replay-deferred:` row exists. The write gate (`enter_write`) holds while the roots are pinned and the rows are read, so no commit starts or ends in between, and the cut is exact: the files hold exactly the writes that the head rows name.
- Not brought back in this step: `prearm.rs`, `Status.snapshot_required`, `note_accepted_snapshot`, `Error::Snapshot`, the `capture_initial_snapshot` option, `clear_restored_bootstrap_pending`, `release_bootstrap_replay_pins` and `live_file_paths`. They pin the exact files of a home that had data before capture began, and refuse to acknowledge before a first snapshot covers that history. That is the move of an existing home. The snapshot of this step covers the whole store at the freeze, so it needed none of them. Section 11 brings them back.

### The `snapshot` entry

A new entry kind in `mutation_log`, signed by the owner. The kind string is `snapshot`, the domain `lastdb-snapshot-v1`. The body is `manifest_sha256`, `manifest_bytes` and `through`, a list of `{device_id, writer_seq, entry_hash, order}` for each stream the snapshot holds. The exact bytes are in `relay-protocol.md`. It sits on a stream of its own, whose device id is `HKDF-SHA256(node seed, info = "lastdb:personal-database:v1:snapshot-device")`, like the birth device. Devices skip that stream when they apply entries. The reference relay needed no change: it stores a publisher entry of any kind it can deserialize.

### Publishing

After a clean turn, when a snapshot is requested or due, the node captures, hashes each file, uploads each file that the previous snapshot of this device did not hold (by path and digest) with the file-streaming `blob_put`, uploads the manifest, and appends the entry. A crash before the entry leaves unused blobs. A second device that signs on the snapshot stream at the same time gets a refused append and tries again on the next turn.

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

A home that was connected to the earlier cloud backup can hold `signed-replay-pending`, `-baseline`, `-pending-attachment` and `-deferred` rows, and the file pins that go with them. They belong to writes that the old cloud never acknowledged and to remote changes that waited behind them. No entry of the signed log will ever settle them, and they do real harm: a snapshot is refused while any pending or deferred row exists, so the home would never take a second snapshot, and a remote change to a record with a pending row would wait for ever. The old function that cleared them for a restored home fits, and it is used as it was: it keeps every record value and the `winner` stamps and drops only that state. It runs at the one moment when it is safe, when the spool is empty, because a pending row of a write that is in the spool must stay until that write is signed. The copy is cut after it, so the snapshot carries none of them and a restore needs no special path. A record written before keeps its winner stamp, whose time is a real millisecond time, so every entry the new build signs, stamped with a later time, still wins over it.

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

Step 6 (the relay drops the entries a snapshot covers). A second snapshot entry after a kill at one window is left on the relay, harmless and small.

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
- A snapshot that puts every file again is accepted. The relay answers `blob.put` begin for a digest it already holds with no `upload`; the node treats that as stored (before this change it failed with "blob.put begin has no upload.url"). Device D restores from that snapshot and replays nothing. A CI run without the fix failed there with that message.
- A streamed file upload carries one `content-length`. The Exemem relay signs `content-length` and returns it among the upload headers, and the node used to add it a second time, so the object store answered HTTP 400 and the entry that names a file stayed unconfirmed. The reference relay returns no `content-length`, so only the DEV relay showed it.

Not proven against it, because the relay's process, request log and disk are not readable: that the relay holds no key, value, schema name or marker, one entry per write by listing, a restore by full replay from a relay with no snapshot, idle turns reading no entry, reads of a restored home with the relay stopped, the kill before the first snapshot is named, the kill of a restore, kill -9 under load, and the scale run. These stay on the reference relay. The reference relay differs from Exemem in: a repeated `blob.put` begin (reference issues a new upload, Exemem answers with no `upload`), a 5 GB limit on one blob, billing of the publisher's account with a quota, a stored-entry check that does not require gapless appends, and the presigned upload and download URLs of the object store. Storage a run adds to the DEV relay stays there: a node cannot delete it.
