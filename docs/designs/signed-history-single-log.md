# Design: signed-history sync with one durable log (draft)

Date: 2026-11-22. Author: Claude, for Tom. Status: approved by Tom, 2026-11-22. Implemented in #1826; relay and cleanup in the following change.
Scope: `lastdb_node/src/log_sync` in EdgeVector/lastdb. Review basis: LastDB `f6433d3` (#1817) and `38c7d705` (#1823), People DEV sharing runs 44869965932 and 44978664853, PRs 1816 to 1824.

## Current state

Each shared database keeps three copies of one fact, "what this node signed and has not yet uploaded":

| Copy | Where | Written by |
|---|---|---|
| Authored entries | `log-sync/<id>/authored.json` | `append_authored` |
| Pending uploads | `log-sync/<id>/outbox.json` and the in-memory `Outbox` | `persist_outbox` |
| History | in-memory `VerifiedHistory` | `accept_entry` |

Four code paths do the same sequence (accept, append authored, enqueue, persist): `apply.rs:61`, `mirror.rs:127`, `chain_author.rs:126`, and the member-write and proposal code.

## Problems

1. **Divergence.** The pending list is derived from the authored log but stored separately. Any gap between them is permanent: replay rebuilds history but not the outbox. A crash between `append_authored` and `persist_outbox` leaves an entry that is authored and never queued.
2. **Swallowed load errors.** `load_disk_outbox` (`history_store.rs:183-209`) logs and continues with an empty outbox when `outbox.json` cannot be read. The next persist overwrites the file.
3. **Misleading diagnostics.** `cloud_sync.last_error` is node-global and sticky. An `orphan=` label in People means "acknowledged", not "lost". Together they sent a day of work at an outbox wipe that the relay's own data did not support (#1819, #1824).
4. **Dead and repeated code.** `bootstrap.rs` and `apply.rs` are `#[allow(dead_code)]`. Pull restarts at sequence 1 every turn (`spawn.rs:163`). The 64-batch cap returns an error when exactly 2048 entries drain (`spawn.rs:273-289`).
5. **Blind proofs.** The People legs print final state only. They cannot say whether an entry was never appended, appended but not applied, or applied but not read.

## Design

### 1. One log, one cursor

- Each database has one append-only `authored` log and, per device stream, an `uploaded_through` cursor.
- There is no stored outbox. Pending entries are the authored entries with sequence above the cursor.
- Birth is entry 1 of the stream, so it always uploads first.

### 2. One commit function

`commit_authored(history, entry)`:

1. Accept the entry in memory on a copy of the history.
2. Append the entry to the log and sync it.
3. Swap the accepted copy into the map.
4. Request a wake.

Any failure returns an error and leaves the stored history unchanged. All four current callers use it. `chain_author.rs:143` (replay in place after a failed append) goes away.

### 3. Fallible load

`with_mut` on a vacant database reads the log and replays it. A read or parse error is returned to the caller. No empty history is inserted. Read and write size caps match.

### 4. Upload by cursor

```
loop:
  batch = entries after cursor, up to the relay batch size, in log order
  if batch is empty: done
  append(batch)            # relay counts duplicates as stored
  if stored < len(batch):  error for this database
  cursor = last sequence in batch   # write the cursor, then continue
```

There is no batch cap. A crash after the relay accepts and before the cursor write re-sends the batch; the relay treats identical entries as duplicates.
Pull resumes from the stored per-stream head, not from sequence 1.

### 5. Per-database status

`/api/status` reports for each database: `pending` (log head minus cursor), `last_uploaded`, `last_error`. The node-global `last_error` and its clearing rule go away. People prints that row and drops `orphan=`.

### 6. Authorship in materialize

Applied writes carry the signed writer. When the publisher incorporates a member submission, the writer is the proposing member. A member entry that sets `incorporated_from` is malformed (#1824).

### 7. Deletions

Remove `bootstrap.rs`, `apply.rs`, the `outbox.json` file, `upload_ack.json` (PR 1818), and the duplicated persist and acknowledge helpers.

## What this replaces

- #1817 (reload outbox on replay): not needed, there is no outbox file.
- #1818 (requeue past `upload_ack.json`): close it.
- #1816 batch cap: removed.

## Constraints

- No feature flags, compatibility paths, fallbacks, migrations, or unit tests. Proof is the People five-leg DEV run and the relay proof, built and measured in CI.
- The on-disk layout changes. Tom is the only user, so one manual copy of the old home to the new layout is enough. No migration code.
- One LastDB PR. No local builds.

## Order of work

1. Merge #1824 after a green five-leg People dispatch.
2. Close #1818.
3. Land sections 1 to 4 and 7 in one PR.
4. Land section 5 with it so the next failure names its stage.

## Received entries

An entry this node signs can cite another writer's entry: a publisher's incorporation cites the member's proposal. The authored log alone therefore cannot rebuild the history, and a restart used to leave such entries waiting for the relay. While they waited, the history's tip of this node's own stream was an older one, and the next signature reused a sequence number. The relay refused it with `409 conflict entries.writer_seq`.

`received.json` holds the accepted entries of every other stream, in ascending Lamport order. A restart replays the authored log and the received entries together in Lamport order, which meets every dependency before its user, so the history is rebuilt in full without the relay. `commit_authored` also refuses to sign on a stream that has an entry waiting for a dependency.

## Relay

The relay is Exemem. A node syncs with `LASTDB_RELAY_URL` when that is set (a reference relay or a deploy under test), and otherwise with the Exemem API in its saved cloud credentials. No relay URL is stored on disk. A node with neither has no relay and the sync loop idles.

## Record keys and authors

A history record key is one string. A hash schema uses the hash key. A hash-range schema joins hash and range with the unit separator (U+001F), and the schema decides which reading applies. A hash that holds the separator is refused for a ranged schema.

History records are whole-record last-writer-wins. Materialize stamps FoldDB `created_by` and `updated_by` with the winning entry's writer (the proposing member for a publisher incorporation), and rewrites a record whose fields match but whose authors differ. Every node therefore stamps alike, whatever order entries arrived in.

## Compare-and-set across nodes

A FoldDB record revision is a hash of the node's local write stamp, so two nodes show different revisions for the same record. A proposal therefore signs the history entry that wrote the record, not the revision. The member node maps the revision the client read to the winning entry of its history, only when that revision is still the one its store holds. The publisher accepts the proposal when that entry is still the winner of its own history, then applies the write with its local revision. A revision the member no longer holds names no entry and the proposal stays submitted (Decision 11: silent rejection).

## Record ownership

Each history record also keeps its creator: the writer of the earliest accepted write to the key by position, whoever wins now. A member may replace or delete only a record it created, so the publisher applies a replace or delete proposal only when the proposer is the creator. Ownership does not change when another writer, such as the publisher moderating a comment, becomes the winner. FoldDB `created_by` and `updated_by` still carry the winner, which is what the direct-write convergence check needs.

## Void entries

A publisher role reduction carries a cutoff for each device stream of the grantee. A member entry accepted before the reduction can fall past that cutoff, and then it must not count on any node. The entry stays in the accepted entries, so the stream chain and the log are untouched. When a publisher grant entry is accepted, the history looks at the non-publisher direct entries that are now past their cutoff, and recomputes the winner of each key they wrote from the entries that still count. A key falls back to its previous winner, or has none. This is one pass over the accepted entries, only on a publisher grant, and nothing is saved per entry.

The history remembers those keys (`voided_keys`). The materializer writes the new winners as usual and deletes the record of a voided key that has no winner left. A write that was committed locally while the member still believed it had write access is therefore undone once the member's node learns of the reduction.

A cutoff limits a member's direct writes only. A proposal is checked against the member's current role (submit or write), so a member reduced from write to submit can still propose, and a revoked member cannot, even if its `seen_heads` cite an older grant.

## Decisions

- One LastDB PR for the log, cursor and commit function.
- The cursor is its own file, `uploaded.json`: a count of acknowledged log entries.
- `mutation_log::Outbox` and the `outbox.json` file are removed.
- The node-wide `cloud_sync.last_error` stays until People and the `lastdb` CLI stop reading it. Per-database errors are in `cloud_sync.databases[id].upload`.
