# Unified cloud mutation log

Status: accepted. Tom Tang, 10 November 2026.
`mutation_log` is the pure core. `mutation_log_mem` is the `MutationLog`
port and the in-memory adapter. The adapter stays off unless a caller
enables `MutationLogConfig`. The live Share path does not construct it.

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

- `writer_seq <= last` with the same content is a duplicate
- `writer_seq <= last` with different content, while the entry is still stored, is `Conflict`, and the call is not `Ok`
- `writer_seq <= last` after that entry has been trimmed is a duplicate
- `last + 1` is accepted
- a higher value is a gap, and the batch is not stored

A duplicate reports the next acceptable writer sequence, one past the stored
tip. A caller whose response was lost can retry, then append the next mutation
at that sequence. A participant appends only after resume loads that tip.
Copying a submission is idempotent on `(writer, writer_seq)` of the
submission, once in total, when the payload matches the stored submission.
Repeating that exact copy, from any device of the publisher key, stores
nothing and does not move that device's tip. A different payload is
`Conflict`. A batch that mixes an already copied submission with a new entry
is rejected. The repeat check runs before the submission lookup, so an exact
retry of a copy whose submission was trimmed is a duplicate. A new writer
sequence that names a trimmed submission is refused. The `incorporated` set
is stored with the log. Trim drops a key when the submission it names is
dropped. A persistent adapter stores the set alongside the log.

There is no ULID idempotency row. Merge order is the stamp
`(authored_at_ms, writer, sequence)`. An append is one transaction of
at most 90 entries, and each writer has at most one append in flight.

The operations are append, head, list, ack, grant, revoke, and snapshot.
Every cursor and every ack is a delivery sequence. An ack is stored per
reading device, writer (node key, device), in the one log. A device that has
appended, or that has pulled once, counts as acked at 0 until it acks. The
first pull records that zero and does not move it. `list` returns the
readable entries and sets `next` to the last delivery sequence scanned,
including entries that reader cannot see, so hidden entries are covered.
`head` reports the log head's delivery sequence, including when every stored
entry is hidden. Its stamp is the last entry this reader can see, and it is
absent when none are visible, so a hidden entry's writer is not reported.
Acking that cursor does not sit below the trim floor. The trim floor
is the minimum of those device positions, and also a 30-day maximum
retention. The floor does not move past the latest snapshot. Revoke drops
every device ack of that key.

Cutting a snapshot stores the shell's `snapshot_ref`, an opaque pointer to
the snapshot bytes. The pointer is non-empty and has no control characters.
The core does not cap its length and does not store those bytes. Trim that would
drop entries is refused unless that snapshot covers the floor. A device
whose position is below the floor gets `CursorExpired`. The sync step then
returns `Reset { snapshot_ref, through, at_ms }` plus the entries still
stored after the lesser of the device's local cursor and its stored ack. A
reinstall that reuses a device id and is behind the stored ack is delivered
from that lesser position, and it resets when that position is below the
floor. The device downloads the snapshot by `snapshot_ref`, then applies the
suffix. The pull does not ack. The caller applies the reset and the entries,
persists them, then acks the returned cursor. Until that ack, the same pull
delivers the same reset and entries again. Applying them twice is
idempotent. Until a snapshot exists, aged entries stay readable and the step
reports that a snapshot is due.

## Sharing invariant

Tom, verbatim: "for share, you must only allow receiving devices to read the
publishers log. No one else's."

Testable rule:

1. A receiver of a share reads only entries whose writer key is the publisher
   key in that database's log, including re-appends. The cursor is the
   delivery sequence. An ack at or below the stored delivery sequence is a
   no-op. An ack past the log head is `AckAhead`.
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

There is no official-namespace list in the tree yet. The core asks a
`NamespacePolicy` and does not embed the list. The held official-namespace
module will supply the real list. Until it does, a caller passes the
classification. A production policy returns official only for a namespace on
that list, and app-private for every other namespace.

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

A locally owned shareable database is still a separate home and daemon, not
a second database beside `personal` on the same node. The SDK can already
select a second registered database. It cannot create the owned shareable
sibling. That registration is a follow-up. This change does not edit the SDK.

## Code that supports this

Pure core, no I/O, crate `mutation_log`:

- `mutation_log/src/lib.rs` — step entry and the batch, page, and retention bounds
- `mutation_log/src/model.rs` — entries, stamps, database kind, grants, writer
- `mutation_log/src/state.rs` — log value, one in-flight append per writer
- `mutation_log/src/append.rs` — dedupe, gap, app-private rejection
- `mutation_log/src/auth.rs` — `can_read`, grant, revoke
- `mutation_log/src/read.rs` — `filter_readable`, list, merge order
- `mutation_log/src/retain.rs` — ack, snapshot, trim floor
- `mutation_log/src/namespace.rs` — `NamespacePolicy`
- `mutation_log/src/port.rs` — `MutationLog` port. Adapters implement it. The
  sync step depends on this trait, not on an adapter.
- `mutation_log/src/tests/` — the invariant, including app-private namespaces
  and the refusal to grant a personal database to a receiver

In-memory port, crate `mutation_log_mem`. The same port serves a personal
database and a shareable database. There is no format parameter and no
contributor cap. `MutationLogConfig` defaults off. `open` returns nothing
unless the caller passes an enabled value. The product does not read
`LASTDB_MUTATION_LOG` and does not construct the adapter, so the live Share
path is unchanged. The shell classifies a string with `env_flag::truthy`
and does not read the process environment itself. `mutation_log_mem` is the
test adapter.

- `mutation_log_mem/src/lib.rs` — config, default off
- `mutation_log_mem/src/port.rs` — `MutationLog`: append, head, list, ack,
  grant, revoke, snapshot
- `mutation_log_mem/src/memory.rs` — in-memory adapter over `step`
- `mutation_log_mem/src/tests.rs` — the same sharing rules, through the port

Shared-sync step, crate `mutation_log_sync`. Personal writers and share
participants call the same `drive`. There is no format parameter and no
contributor cap. `select_path` returns the existing `lastdb_node` shared-sync
worker when the caller passes `enabled == false`, and it does not build a log.
The shell reads `MutationLogConfig` and passes that flag. The worker does not
call this crate, and the product still does not read `LASTDB_MUTATION_LOG`.
The crate depends on `MutationLog` only.

- `mutation_log_sync/src/lib.rs` — `select_path`
- `mutation_log_sync/src/step.rs` — the pure effect for append, pull, and re-append
- `mutation_log_sync/src/drive.rs` — apply that effect through `MutationLog`.
  Append waits until `SyncState::resume` has loaded the writer tip. A pull
  returns the page or `Reset { snapshot_ref, through, at_ms }` plus the
  suffix, and does not ack. The caller acks after it applies and persists.
- `mutation_log_sync/src/tests/` — personal round trip, share round trip, flag off

## Later changes

These are not the current code.

- The `lastdb_node` shared-sync worker adopts `select_path` when a shell
  passes an enabled `MutationLogConfig`. Today the worker stays the existing path.
- Exemem persists one encrypted log per `database_id` and calls this step.
- Exemem's read path enforces `can_read` for stored bytes.
- The official-namespace module supplies `NamespacePolicy`.
- One node registers an owned shareable database beside `personal`. The SDK
  already selects it with `databaseId`.
