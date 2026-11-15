# Unified cloud mutation log

Status: accepted. Tom Tang, 10 November 2026. The
[two-log sharing design](shared-database-mutation-logs.md) describes current
whole-database shares. This document also covers legacy mixed-log history.

`mutation_log` is the pure core. `mutation_log_mem` is the `MutationLog`
port and the in-memory adapter. `mutation_log_sync::drive` is the only
shared-sync step. There is no flag. `lastdbd` calls the log through
`log_sync::spawn`.

## Decision

Each database has an ordered, encrypted log of committed changes under its
`database_id`. Whole-database shares also have a separate pending mutation log
with its own position. Its entries are encrypted for the publisher before upload.
The publisher's LastDB checks every proposed change and publishes the changes
it accepts.

A database is one of two kinds:

- **Personal.** Not shareable. The owner key reads and appends it from any
  of its devices. App-private namespaces live here. `hearth/PhotoMeta` is the
  example: the namespace is `hearth`, and the schema name stays inside the
  entry.
- **Shareable.** Active receivers follow the publisher's log for this database.
  In whole-database mode they can read its published contents across every
  schema. Older spec-bound shares apply their namespace rules. Private records
  belong in a separate database.

## Share access v2

The node's Ed25519 public key is the identity, and it is the same on every
device. In this log that key is `PrincipalId`. ExeMem authorizes requests
against the current publisher-signed member list for that database. Every
active receiver can read the published log and append encrypted mutations
to the active whole-database pending log. Only the publisher can read or
settle the pending log. Schema write grants guide receiver apps and the
publisher enforces them when it processes a submission. Legacy mixed-log
append uses the `Submit` capability. Device IDs identify writers for retries.

## Log shape

Each committed log has a small head. Its sequence is delivery order.
A writer is the pair `(node key, device)`. Two devices of one
key have independent sequences and do not conflict. Two processes writing as
the same writer can still race; the core does not treat that as success. A
writer's `writer_seq` is gap-free:

- A sequence that matches a stored entry is a duplicate only when its content matches.
- A different payload under an accepted sequence is a conflict.
- When an old payload was trimmed and its exact acceptance cannot be verified, replay stops with the pending edit intact.
- An expired or rejected pending submission retains its specific refusal.
- The next consecutive value is accepted. A gap stores nothing.

A device stores its sequence locally. Before sending an edit it saves the
exact numbered upload so a lost response can be retried. On acceptance or
confirmation of a matching duplicate it records the upload progress and
releases its local queue. An older installation can bootstrap its number
from entries the cloud has already accepted. The cloud answers from the log,
snapshots and immutable records of accepted entries. These records describe
what the cloud accepted and do not advance a separate per-device pointer.

### Legacy mixed-log history

The rules in this section apply to the older contribution path and its
retained history. New whole-database pending entries use
[publisher-controlled settlement](shared-database-mutation-logs.md#from-invitation-to-shared-data).

Copying a legacy submission is idempotent on `(writer, writer_seq)` of the
submission, once in total, when the payload matches the stored submission.
Repeating that exact copy, from any device of the publisher key, stores
nothing and does not move that device's tip. A different payload is
`Conflict`. A batch that mixes an already copied submission with a new entry
is rejected. The repeat check runs before the submission lookup, so an exact
retry of a copy whose submission was trimmed is a duplicate. A new writer
sequence that names a trimmed incorporated submission is refused. A pending
submission stays in the log until the publisher re-appends it, rejects it,
or 30 days pass. Historical log snapshots do not include pending submissions. The earlier trim
operation left young pending submissions in place. Reading past those
submissions does not resolve them.
After 30 days, the author's retry and a re-append are `SubmissionExpired`,
naming that writer and sequence. A rejected submission can be trimmed while
it is still young. The author's retry and a re-append are then
`SubmissionRejected`, naming that writer and sequence. A rejection stays in
force while the entry is stored, including after 30 days, so a re-append
stays `SubmissionRejected` and a later drop is not recorded as expired.
When trim drops the entry, the log keeps one record: the writer, the writer
sequence, rejected or expired, and the time the entry was dropped. The
record has no payload. Records are kept for the life of the log, so a later
retry is never a duplicate. Trim does not remove them. The
`incorporated` set is stored with the log. Trim drops a key when the
submission it names is dropped. A persistent adapter stores the sets
alongside the log.

There is no ULID idempotency row. Merge order is the stamp
`(ordering time, writer, sequence)`. The ordering time is the older of
the author time and the time the server accepted the entry. A later
author time counts as the acceptance time. The author time sealed in
the entry stays, because opening the payload uses that number. An
append in the core is one transaction of at most 90 entries. Each writer has
at most one append in flight in that model.

### Published reads and personal recovery

The active cloud operations include append, head, list, grant and revoke.
Each device stores its own download position as a delivery sequence, independent
of its upload sequence. `list` accepts that position and returns the readable
entries plus the last delivery sequence scanned, including positions hidden
by share permissions. A client advances its position only after the resulting
local writes are durable. The signed cloud client keeps this position locally.

`head` reports the database delivery head. Its stamp is the last entry this
reader can see, and it is absent when none are visible. The cloud keeps a
database-wide trim boundary for logs trimmed under the earlier signed snapshot
format. The signed delivery route can read those previously stored snapshots.

A routine personal file snapshot freezes the database only after local queued
writes reach the mutation log and the local applied position matches the cloud
head. The service verifies the file inventory and may advance a database-wide
native cutoff under the same head and owner guards. It keeps the exact
manifest available by digest. A separate database-wide physical floor records
the last delivery entry the service actually removed. Older clients can read
the retained entries. A client below the physical floor must upgrade to a
version that can recover from the native snapshot.

When a previously local personal home connects for the first time, the device
preserves the two native roots at the start of signed capture. The checkpoint
links immutable runs on the same filesystem and keeps the original manifests.
With an empty signed cloud log, the device publishes these files at position
zero and releases its queued mutations only after verifying the published
snapshot. If an older home already queued writes and the exact original
checkpoint is unavailable, those edits remain queued.

After a qualifying `lastdb backup snapshot` of the root personal database, the
device resumes bounded cleanup during its normal sync turns. It records the
candidate digest and covered position before publication, then waits for signed
confirmation of a verified recovery cutoff. Each request checks the exact
cloud cutoff and deletes only entries through the locally recorded target. The
device must have already applied that target. A restarted device uses the
cloud's current physical floor to resume. A backup can complete while cleanup
is pending or unavailable from the service. Read `snapshot_through` and
`reclaimed_through` in `backup reclaim --status` to check progress.

`lastdb backup reclaim --database personal --status` reads the verified
snapshot and physical floor for the owner's personal database.
`lastdb backup reclaim --database personal --allow-required-client-upgrade`
can remove a bounded prefix after the service enables reclamation. Use
`--batches` to limit one invocation to 1–128 requests. The default is 16.
Use `--expect-snapshot DIGEST` on repeated invocations to keep the same
snapshot and covered position. If the cloud selects another snapshot, the
command stops before it sends a reclaim request.
By default each request removes up to eight entries. Use
`--steps-per-request` with the pinned digest to allow up to eight transactions
per request, each removing up to eight entries. The total never passes the
verified snapshot position. After an uncertain response, query `--status`
with the same digest before resuming. Each request checks the snapshot digest
and the expected physical floor. The service verifies the pinned files and
requires immutable records of acceptance for every deleted entry. Their
fingerprints cover the writer and its sequence, author time, namespace and
encrypted payload, so the cloud can verify an exact retry after it deletes
the original entry. The cloud does not add a mutable position for any device.
In production, automatic cleanup requires an existing positive physical floor
established through explicit owner reclamation. A device behind that floor
needs a recovery-capable client. A Search-enabled device that cannot apply an
automatic snapshot reset needs an explicit restore.

`lastdb backup index-log-history --database personal --status` reads the first
delivery covered by the acceptance index. When the service enables indexing
for that personal database, `--batches N` indexes older entries in at most
256 signed requests per invocation, with up to 32 entries in each request.
The command reports progress every 16 requests and prints the last confirmed
position on failure. Read status before retrying after an interruption.
The database is fully indexed at delivery 1. Indexing can run before a file
snapshot exists and leaves the cloud log entries and device positions intact.

A compatible personal device includes its saved download position when asking
for edits. Below the native cutoff it receives an exact signed snapshot
reference and its covered position. The device sends its pending local edits
to the cloud first, downloads the verified files to a private stage, and
applies the entries after the snapshot to that stage. It installs the stage
with the store closed and saves its download position after the new files are
durable. A local write that arrives during staging invalidates the prepared
replacement and is uploaded on the next attempt. The cloud keeps no device
catch-up records.

## Sharing invariant

Tom, verbatim: "for share, you must only allow receiving devices to read the
publishers log. No one else's."

Testable rule:

1. A receiver of a share reads only entries whose writer key is the publisher
   key in that database's log, including re-appends. The cursor is the
   delivery sequence. The receiving device saves that position locally.
2. Only the publisher can read or settle the separate pending log. ExeMem
   also excludes every receiver-authored entry from the receiver's view of
   older mixed logs.
3. The publisher's LastDB checks every downloaded mutation against its current
   sharing settings and record rules. It commits and publishes accepted changes.
   Receivers sync only the resulting publisher-authored entries.
4. A receiver does not see the publisher's personal database, the publisher's
   other databases, or any entry whose database id or writer key is outside
   that scope.
5. App-private namespaces are rejected on append into a shareable database
   (`AppPrivateNamespace`). The read filter also drops them if one is present,
   including for the publisher.
6. App-private namespaces are allowed in the personal database.
7. A personal database cannot be granted to a receiver
   (`PersonalNotShareable`). A contributor grant is rejected there too. An
   owner grant is rejected on a shareable database
   (`DeviceGrantNotShareable`).

Removing a participant is `revoke`. `can_read` is false on the returned
state, and every open append for that key is cleared, on every device. This
log does not rotate keys, re-key a database, or move an epoch.

ExeMem enforces log access when it serves stored bytes. Receiver apps display
permissions. The publisher's LastDB enforces which changes may be published.

`can_read(principal, database, entry)` and `filter_readable(entries, reader, database, policy)`
are that rule. `filter_readable` takes the database and the namespace policy
as well as the reader, because a slice of entries does not carry them.

## Namespaces

In whole-database mode the share contains that database's schemas and data.
The publisher's LastDB checks selected write grants against the exact
installed schema definition IDs. The local database name is a selector.
Older spec-bound shares retain the namespace rules of their selected spec.

`social` is lastdb-std 1.0 and shareable (`spec/index.json`,
`spec/README.md`, `spec/social/`). `social-graph` is the lastdb-std 1.0
graph namespace (`spec/index.json`, `spec/social-graph/`). `spec/README.md`
keeps it personal-only, so a shareable database rejects it.

`lastdb-files` is the namespace the share files command publishes, for
`FileCatalog` and `FileChunk`
(`lastdb_node/src/bin/lastdb/share_cmds/files/mod.rs` and `publisher.rs`).
`scripts/operations/record_share_files.py` writes those same schemas on a
share home.

For spec-bound shares, `BoundPolicy` uses the database's spec to classify
namespaces. An app-private namespace is refused with `AppPrivateNamespace`.
A personal database accepts app-private namespaces.

## How an app addresses both databases

The SDK binds one client to one database.

- `connect({ appId })` with no `databaseId` uses the node's default database.
  Routes stay `/api/...`.
- `connect({ appId, databaseId })` sends `/api/databases/<databaseId>/...`.
  `databaseId` is the id or alias from `lastdb db list`. The alphabet and
  the 64-byte limit match the node selector.

An app that has both databases opens two clients:

```ts
const personal = await connect({ appId: 'hearth' });
const family = await connect({
  appId: 'hearth',
  databaseId: 'social-family',
});
```

`personal` stores `hearth/PhotoMeta`. `family` stores the records people
share. Each client uses the schemas installed in its selected database.

On the node, `GET /api/databases` lists the owner database and any registered
shares. The owner id is `personal`, unless this home is itself a publication,
in which case the id is the publication id. Selecting that id serves the
owner database. Any other registered id is a received share. An unknown id
is 404. The code is `lastdb_node/src/exec/database_routes.rs` and
`lastdb_uds/src/database_path.rs`.

`lastdb db create` registers an owned named database beside `personal` on
the same node. The SDK selects it with `databaseId`.

## Code that supports this

Pure core, no I/O, crate `mutation_log`:

- `mutation_log/src/lib.rs` — step entry and the batch, page, and retention bounds
- `mutation_log/src/model.rs` — entries, stamps, database kind, grants, writer
- `mutation_log/src/state.rs` — log value, one in-flight append per writer
- `mutation_log/src/append.rs` — dedupe, gap, app-private rejection
- `mutation_log/src/auth.rs` — `can_read`, grant, revoke
- `mutation_log/src/read.rs` — `filter_readable`, list, merge order
- `mutation_log/src/retain.rs` — ack, snapshot, trim floor
- `mutation_log/src/namespace.rs` — interim shareable list and `NamespacePolicy`
- `mutation_log/src/port.rs` — `MutationLog` port. Adapters implement it. The
  sync step depends on this trait, not on an adapter.
- `lastdb_node/src/log_sync/inbox.rs` — publisher download, validation, and settlement
- `lastdb_node/src/log_sync/contributions/intake.rs` — schema and record checks before committing a pending mutation

## Cutover

There is no flag. `drive` is the only shared-sync step.

Every owned database, personal and private included, registers with
`POST /api/cloud/backups/v2/mutation-log/create-database` and then syncs
that database id. The owner store records accepted writes for that log.
The original-file backup captures a recovery base for the same database id.
Restore applies the signed log entries after that base.

The signed client resolves the database id to a store and its existing
content key. It uses the same envelope, file upload, and snapshot replay for
private and shareable databases. Publication and grants determine who may
see the contents. The name is a local selector.

ExeMem admits private and shareable databases on the same signed API.
Create an owner-only database with `lastdb db create NAME --private`.
The owner key and cloud account scope are bound at registration.
The database accepts no share grants.

Keep historical signed snapshots available for readers with an expired local
position. The protected daily DEV operation proves two-device sync, exact
upload retries, and fresh restore from the retained log. The separate native
snapshot workflow proves original-file backup and restore. The last DEV run
before the old trim write route retired proved recovery from an expired
cursor with pending local edits ([run 43533113979](https://github.com/EdgeVector/lastdb/actions/runs/43533113979)).

`mutation_log_mem` is the in-memory adapter. Construct it with
`InMemoryMutationLog::new`. `mutation_log_sync` applies one effect through
`drive`. The in-memory adapter can use its transient writer and reader state.
The device daemon keeps separate durable positions and uses the signed cloud
adapter for append, list and snapshot recovery. Neither core crate reads the
process environment, opens a socket, or calls ExeMem.

Exemem's read path enforces `can_read` for stored bytes. One node registers
an owned shareable database beside `personal`. The SDK already selects it
with `databaseId`.
