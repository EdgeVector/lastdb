# Unified cloud mutation log

Status: accepted. Tom Tang, 10 November 2026.
`mutation_log` is the pure core. `mutation_log_mem` is the `MutationLog`
port and the in-memory adapter. `mutation_log_sync::drive` is the only
shared-sync step. There is no flag. `lastdbd` calls the log through
`log_sync::spawn`.

## Decision

LastDB uses one ordered, encrypted cloud mutation log per `database_id`.
The same log serves personal multi-device sync and shares. It covers every
cloud database format. There is no per-type exception and no contributor cap.

one stream per DB, entries tagged by node key plus device

A database is one of two kinds:

- **Personal.** Not shareable. The owner key reads and appends it from any
  of its devices. App-private namespaces live here. `hearth/PhotoMeta` is the
  example: the namespace is `hearth`, and the schema name stays inside the
  entry.
- **Shareable.** Official namespaces only. A receiver reads entries whose
  writer key is the publisher key, including re-appends. An example is a
  database such as `social-family`.

An app may split its data across both. The shareable database holds standard
or official types. The personal database holds what is not meant to be shared.
The publisher does not copy app-private namespaces into a shareable database.
There is no opt-in that overrides this.

Sharing grants who may read or append. It does not reach into storage as a
separate path.

The old Share design's twelve-writer cap is superseded. `MAX_WRITERS = 12`
in `lastdb_node/src/share_contribution.rs` and the twelve-writer note in
`lastdb_node/src/share_contribution/state.rs` still describe the live path.
This change does not delete those constants. `cloud_backup_protocol`'s
`MAX_WRITERS` (1024) is a separate manifest bound and is also still live.
Neither bound exists in the new log.

## Share access v2

The node's Ed25519 public key is the identity, and it is the same on every
device. In this log that key is `PrincipalId`. Exemem's publisher-signed
member list for one share is `grant` and `revoke` on that database. Read
means `Receive`. Read+append means `Receive` plus `Submit`. One share is one
`database_id`, and that is one log. A device id tags the writer; it does not
create another log, cursor, or store.

## Log shape

Each database has one log and a small head. The head's sequence is delivery
order only. A writer is the pair `(node key, device)`. Two devices of one
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

Copying a submission is idempotent on `(writer, writer_seq)` of the
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
append is one transaction of at most 90 entries, and each writer has
at most one append in flight.

The active cloud operations include append, head, list, grant and revoke.
Each device stores its own download position as a delivery sequence, independent
of its upload sequence. `list` accepts that position and returns the readable
entries plus the last delivery sequence scanned, including positions hidden
by share permissions. A client advances its position only after the resulting
local writes are durable. Older clients can still send an authenticated
`ack` through the head. It echoes the position without storing it.

`head` reports the database delivery head. Its stamp is the last entry this
reader can see, and it is absent when none are visible. The cloud keeps a
database-wide trim boundary for logs trimmed under the earlier signed snapshot
format. The signed delivery route can read those previously stored snapshots.

A routine personal file snapshot freezes the database only after local queued
writes reach the mutation log and the local applied position matches the cloud
head. The service verifies the file inventory and may advance a separate
database-wide native cutoff under the same head and owner guards. It keeps the
exact manifest available by digest. The current recovery release keeps older
entries available to clients that do not request native recovery. Only a
protected, generated DEV operation can physically remove its verified prefix.

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
2. A contributor's pending entry is not readable by other receivers. Only
   the publisher key, and that contributor key for its own pending entries,
   can read it.
3. Other receivers see a contribution only after the publisher key re-appends
   it.
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

Enforcement for stored bytes is Exemem's read path, not only the client.
This pure core models the same rule so a later read path can call it. The
client is not the enforcement point.

`can_read(principal, database, entry)` and `filter_readable(entries, reader, database, policy)`
are that rule. `filter_readable` takes the database and the namespace policy
as well as the reader, because a slice of entries does not carry them.

## Namespaces

Shareable databases accept one interim list: `social` and `lastdb-files`.
This list is interim. The real reservation of official names moves to named
databases later. The old schema-prefix reservation plan is superseded.

`social` is lastdb-std 1.0 and shareable (`spec/index.json`,
`spec/README.md`, `spec/social/`). `social-graph` is the lastdb-std 1.0
graph namespace (`spec/index.json`, `spec/social-graph/`). `spec/README.md`
keeps it personal-only, so a shareable database rejects it.

`lastdb-files` is the namespace the share files command publishes, for
`FileCatalog` and `FileChunk`
(`lastdb_node/src/bin/lastdb/share_cmds/files/mod.rs` and `publisher.rs`).
`scripts/operations/record_share_files.py` writes those same schemas on a
share home.

Contributor rules check the caller's schema string
(`lastdb_node/src/share_contribution/apply.rs`) and do not name a catalog.
`photos/Comment` in the contributor proof and the SDK readme is an example,
not a standard schema.

Anything not on the list is app-private. A shareable database rejects it
(`AppPrivateNamespace`). A personal database still accepts it.
`classify_shareable` is that list.

Only official namespaces travel in a share. Schema Service publish/attach
(`schema_service/crates/core/src/shared_surface.rs`) is a catalog opt-in for
a shared schema contract. It is not an exception to this rule, and it is
superseded as a way to put an app-private namespace into a shareable database.
No doc in this repo says app-private record data is shared when an app opts in.

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

`personal` is where `hearth/PhotoMeta` is written. `family` is where official
types are written. This core rejects the opposite write. The node does not
enforce that split yet.

On the node, `GET /api/databases` lists the owner database and any registered
shares. The owner id is `personal`, unless this home is itself a publication,
in which case the id is the publication id. Selecting that id serves the
owner database. Any other registered id is a received share. An unknown id
is 404. The code is `lastdb_node/src/exec/database_routes.rs` and
`lastdb_uds/src/database_path.rs`.

`lastdb db create` registers an owned named database beside `personal` on
the same node. The SDK selects it with `databaseId`. That registration is
not a follow-up. `MAX_WRITERS = 12` remains on the old contribution inbox
until that path is drained.

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
- `mutation_log/src/tests/` — the invariant, including app-private namespaces
  and the refusal to grant a personal database to a receiver

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
The old contributor inbox still has `MAX_WRITERS = 12`.

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
