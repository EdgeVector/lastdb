# Schema-root data attribution and safe reclaim

> **SUPERSEDED 2026-09-27 (Tom).** Reference counting replaces the
> schema-root copy-only walk below as the primary atom garbage-collection
> mechanism. Brain: `decision-2026-09-27-lastdb-atom-refcount-gc-approved`,
> `decision-2026-09-27-lastdb-schema-root-attribution-retired-for-refcount`.
> This document is not withdrawn — the reachability walk it describes
> becomes a periodic AUDIT that checks the reference-count candidate list
> against true reachability, not the terminal gate. See kanban card
> `lastdb-refcount-audit-harness-repurpose`. The rest of this document
> describes the original, now-superseded design and stays as reference —
> the twelve reclaim-safety rules, the migration-phase table, and the
> acceptance-proof checklist below still inform the audit's design.

| Field | Value |
|---|---|
| Status | Superseded 2026-09-27 — kept as design reference for the audit path |
| Date | 2026-09-21 |
| Scope | LastDB Mini owner maintenance |
| Canonical model | `docs/lastdb-canonical-model.md` |
| Related work | `docs/physical-atom-delete-proof.md`, `fold_db/docs/ATOM_GC_TIP_FOLD_GATE.md`, `fold_db/docs/DELETE_RETURNS_BYTES.md` |
| Delivery plan | `docs/designs/schema-root-data-attribution-delivery-plan.md` (also superseded) |

## Decision

LastDB must attribute every user-data object from schema roots, system roots,
retention roots, and durable reference edges. It must not require an
application manifest or an app owner record.

An app may add a human label to a schema. The label helps an operator answer
"what made this?" It does not decide whether LastDB can delete data.

Reclaim consumes the completed attribution result. It is not the attribution
rule. A path from a live schema root is one valid attribution proof, but an
object can also have a system or retention attribution.

The final reclaim rule is:

```text
delete a copy-local object only after complete attribution proves that no
schema, system, retention, or durable-reference root reaches it
```

This rule finds residue. It does not define normal deletion. A correct delete
path removes live roots and durable edges. A later attribution walk then finds
no unexpected user-data residue. The first migration exists to classify and
remove old residue that lacks that normal lifecycle evidence.

Age, last access time, and a missing app are not attribution or deletion
signals. A live record can be old. An app can be retired while its data remains
live.

After the one-time migration, the promoted database has no user-data object in
an `unattributed` state. Each object is either attributed, deliberately
retained, derived, or removed from the migration copy. An unreadable or
ambiguous object remains `unknown`; it blocks promotion and deletion.

## Problem

LastDB stores a record through this ladder:

```text
schema catalog -> declared field -> molecule -> tip -> atom -> file blob
                                      \-> protein member molecule
```

Today, the database has several useful physical diagnostics. It can inspect a
schema, reap tips from a dropped schema, and garbage-collect unreferenced
atoms. These commands do not yet form one schema-root proof.

The gap causes two failures:

1. An operator cannot ask for every current schema and its complete reachable
   storage graph.
2. An operator cannot classify every physical object as schema data, retained
   system data, derived data, or residue when the catalog no longer names its
   schema.

`schemaidx:` copies are a useful example. They are derived copies, not live
data roots. A catalog-to-atom proof must classify them as derived residue,
then remove and compact them without an app decision.

## Boundary

This design does not add a product scan.

Product reads stay keyed:

- point read: O(1)
- range below one hash: O(log M)
- full scan: unsupported

The new walker is an owner-only maintenance operation. It reads durable
catalog and storage structures with a bounded cursor. It cannot run through a
product query route. It records a checkpoint after every page.

## Complexity and storage cost

Let `F` be declared field molecules, `T` live tip slots, `E` durable local
reference edges, `N` physical objects in the inspected planes, and `A`
materialized schema-to-object associations.

| Work | Time | Durable space | Process memory |
|---|---:|---:|---:|
| Initial attribution closure | O(F + T + E) | O(F + T + E) edges | O(page size) |
| Classify all physical objects | O(N + E) | O(N) epoch rows | O(page size) |
| Optional reverse attribution index | O(A) | O(A) rows | O(page size) |
| One normal write | O(1 + changed references) | O(changed references) | O(changed references) |
| Reclaim plan after bootstrap | O(schema fields + blockers) | O(1) new state | O(page size) |

The physical classification pass is necessarily O(N). A database cannot prove
that a physical object has an attribution without inspecting that object or a
complete derived index for its class. This pass is owner maintenance only.

The permanent cost is the active local edge set and one counter per target.
The temporary cost is one epoch row per object. The database deletes epoch
rows only after it writes the proof receipt. It does not retain a second full
copy of atom content.

The default design does not store a full schema-root list inside an atom. A
shared atom can have many schema paths. An inline list makes a normal write
large and makes shared values ambiguous. The database stores fixed-size local
edges in sidecar keyspaces. It derives schema paths from those edges during an
inventory walk.

## Inline logical size attribution

The schema-root walk reuses the existing per-molecule write-path size counter.
Each attributed molecule row can carry three fixed-size values: logical value
bytes, molecule structure bytes, and retained history bytes. This does not add
a second atom walk for size reporting.

These values are logical, not exclusive physical ownership. One shared atom can
contribute logical bytes to more than one molecule. The physical object walker
counts that atom once for reclaim proof. A missing or pending size counter makes
the size report incomplete. It does not turn a reachable object into residue or
change its attribution class.

An operator can request a materialized reverse index for fast “which schemas
reach this object?” reads. That index costs O(A) rows. The default reports use
the O(F + T + E) graph and do not pay that permanent cost.

The migration reserves space for one full logical database copy: O(N) bytes.
On a copy-on-write file system, its initial physical cost is O(D) changed
pages. `D` grows during scrub and compaction. The operator must still reserve
O(N) capacity because a copy-on-write snapshot can lose sharing.

## Review conclusions

The project needs a node-wide maintenance walk. It does not change the
LastDB product query contract. Product reads remain point or hash-scoped range
reads. The owner-only walker uses explicit collection and physical-shard
cursors. Each cursor commits after a bounded page, so restart uses O(page
size) process memory and never needs an in-memory visited set.

The durable ledger has two different roles:

- The active edge and source-event state protects new writes. A successful
  write has a durable, idempotent source event before the API returns. The
  projector can then catch up from a known frontier.
- The epoch rows prove one historic snapshot. They cost O(N + A) rows for the
  walk and can expire after the verified copy receipt. They do not become a
  permanent copy of atom content.

The first copy operation uses a short owner maintenance write gate. It gates
only the final frontier check, durable flush, snapshot receipt, and copy
identity. It does not gate the long graph walk. A later atomic storage
snapshot API can replace this gate if it returns the same durable frontier and
copy identity.

The implementation must treat all incomplete evidence as `unknown`. This
includes an unfinished pending scope, an unreadable physical object, a missing
edge, a stale cursor, or a missing inline-size counter. A missing size counter
makes the size report incomplete. It does not make a reachable object residue.

## Attribution classes

The migration emits one fixed-size attribution row for each inspected object.
The row has the object class, root count, path digest set reference, and source
sequence. Exact root identities and path digests use bounded sidecar rows. The
database never puts an unbounded root list inside an atom or one metadata row.

| Class | Meaning | Migration action |
|---|---|---|
| `schema-attributed` | One or more catalog schemas reach the object. | Keep. |
| `retention-attributed` | A valid history or `as_of` policy reaches the object. | Keep until policy expiry. |
| `system-attributed` | The node needs the object for catalog, metadata, sync, or recovery. | Keep. |
| `derived-attributed` | A deterministic derived copy has a valid source object. | Rebuild or retain by policy. |
| `unattributed-residue` | The complete snapshot has no valid root path to the object. | Delete only from the migration copy. |
| `unknown` | The walker cannot validate its source, format, or edge. | Stop promotion and deletion. |

`unattributed-residue` is a classification result, not an immediate source
delete instruction. The source database remains unchanged during migration.

## Attribution graph

The maintenance graph has these durable edge classes.

| From | To | Source | Meaning |
|---|---|---|---|
| schema root | declared field molecule | schema catalog | The schema declares the field. |
| protein | member molecule | protein record | Members share one logical value set. |
| molecule key slot | tip | molecule index | The key slot has a current value. |
| tip | atom | tip body locator | The atom stores the field value. |
| atom | file blob | validated atom reference | The blob stores large payload bytes. |
| atom | atom | validated atom reference | A structured value can name another atom. |

The catalog, valid retention policy, and node system roots form the root set.
Proteins add valid paths between member molecules. A tip may point at one
shared atom from several molecule keys. An atom or blob can have several
schema attributions.

The walker must keep two values for each object:

- `attribution`: the root class, root identities, and path digest that prove
  the object state.
- `inbound_count`: the number of durable incoming edges.

The walker writes `marked_epoch` for every attributed object. It writes an
attribution row for every object it inspects. It uses a durable work queue,
partitioned by object hash. The queue and rows stay on disk. The process holds
one page at a time.

The count is a reclaim aid. It is not a complete attribution or reclaim proof.
An unrooted atom cycle can have nonzero inbound counts. The complete graph mark
and attribution row are the proof. The database must treat a missing or stale
edge as `unknown`, not as zero.

## Schema attribution lifecycle

Schema identity, not app identity, drives this lifecycle.

| State | Entry condition | Allowed action |
|---|---|---|
| `active` | Current catalog contains the identity. | Maintain attribution edges. |
| `retired-pending` | The database stored a retirement snapshot, then removed the active catalog identity. | Preserve data and build an attribution proof. |
| `attributed` | A complete epoch emits rows for every object in the schema closure. | Report bytes and shared paths. |
| `copy-reclaimable` | The isolated copy proves no valid attribution for selected residue. | Reap derived indexes and dropped tips in the copy. |
| `copy-reclaimed` | The copy removes selected residue and writes a receipt. | Restore and verify the copy. |
| `compacted` | Logical deletes are durable. | Compact affected collections. |

`retired-pending` is not a deletion state. The database enters
`copy-reclaimable` only after it proves the graph is complete for the selected
catalog generation. A complete source attribution report never deletes source
objects by itself.

## One-time isolated-copy migration

The migration builds attribution proof on the source database, then creates
one exact isolated copy. It walks the source schema by schema into one common
attribution ledger. It does not make one database copy per schema. Separate
copies can duplicate a shared atom or hide a cross-schema path.

`lastdb db attribution init` builds the attribution ledger. It is read-only
until the operator starts the separate copy scrub command.

1. Deploy the durable attribution source boundary and pin source-event
   retention.
2. Reserve a source event frontier `H0` and record the catalog generation.
3. Read every catalog schema through the catalog maintenance iterator.
4. Derive each declared field molecule from the schema and field identity.
5. Read each molecule's durable key slots in bounded pages.
6. Add tip-to-atom edges from every resolved tip.
7. Read protein membership and add protein-to-member edges.
8. Decode validated atom references and add atom-to-atom and atom-to-blob
   edges.
9. Classify every remaining object as schema, retention, system, derived,
    residue, or unknown.
10. Record attribution rows, counts, shard cursors, source sequences, and
    unknown rows in a durable checkpoint.
11. Replay each source event after `H0`. Apply it only when it is newer than
    the source sequence stored by the walker.
12. Repeat steps 3 to 11 until the projector reaches a stable final source
    frontier `H1`.
13. Take an exact database snapshot at frontier `Hcopy`, where `Hcopy = H1`.
    The snapshot operation must either hold the owner write gate or use a
    storage snapshot that records its matching source-event frontier.
14. Create one isolated copy from that snapshot. Copy the attribution ledger
    with the data and record the copy identity.
15. Mark the epoch `complete` only after every root, object cursor, and replay
    page ends, and the projector reaches `H1`.
16. Verify that the copy matches `Hcopy`. If it does not, reject the copy and
    start a new snapshot.
17. Delete only `unattributed-residue` from the isolated copy in bounded,
    receipt-backed batches.
18. Compact the copy, restore it fresh, and repeat the full attribution report.

The source database stays unchanged through every destructive copy step. A failed decode, a
missing atom, a cursor error, or an `unknown` row leaves the epoch incomplete.
It blocks copy scrub, promotion, and source deletion.

The pass can take time. It uses durable cursors and a fixed page budget.
Restarting the daemon or command resumes from the checkpoint. The event log
keeps mutations between `H0` and `Hcopy`. A missing event blocks completion.

The ordered attribution event is a locator, not a product mutation payload. It
lets the projector reread canonical source rows. It cannot replay a write into
a copy made at `H0`. The exact `Hcopy` snapshot is required. A future mutation
intent log can add online copy catch-up, but it is not needed for safe copy
reclamation.

The existing app change feed is not this event log. It is an asynchronous
doorbell. LastDB can return a mutation response before that feed persists its
event. The attribution event must join the mutation commit protocol. It cannot
use the app queue.

The first source-boundary implementation uses this durable order for each
request and replay write:

1. Write and flush one `pending` scope marker.
2. Run the product mutation.
3. Write and flush one idempotent ordered source event.
4. Delete and flush the `pending` marker.

A failure after step 1 leaves the marker. The projector marks that exact scope
`unknown` until it resolves the result. A failed marker delete also leaves a
safe blocker. This rule can retain data after a failed write, but it cannot
hide a write that the walker must classify.

The promoted copy must have zero `unattributed-residue` and zero `unknown`
objects in each user-data plane. Its report keeps system and retention rows
visible, so “zero unattributed” does not mean “no metadata or history.”

## Durable epoch state

The node stores the small active epoch in metadata. It stores O(N) proof rows
in the separate, encrypted, node-local `attribution_ledger` namespace. Cloud
capture and backup exclude that namespace. A new node rebuilds its proof from
its own snapshot; it never replays another node's proof rows.

| Record | Key shape | Size bound |
|---|---|---|
| Epoch | `metadata:attribution:epoch:v1` | One small row |
| Cursor | `attribution_ledger:attr:v1:c:<epoch>:<class>:<shard>` | One small row per active shard |
| Work | `attribution_ledger:attr:v1:w:<epoch>:<shard>:<object>` | One key per queued object |
| Row | `attribution_ledger:attr:v1:r:<epoch-hash>:<object-hash>` | One fixed-size row per inspected object |
| Path | `attribution_ledger:attr:v1:p:<epoch-hash>:<object-hash>:<root-hash>` | One bounded row per materialized root path |
| Source event | `attribution_ledger:event:v1:<sequence>` | One ordered source record per idempotent mutation |
| Pending scope | `attribution_ledger:pending:v1:<mutation-hash>` | One fail-closed bridge across the product write and source event |
| Retired schema | `attribution_ledger:attr:v1:s:<identity>` | One schema definition snapshot |
| Receipt | `attribution_ledger:attr:v1:d:<epoch>:<batch>` | One row per copy delete batch |

The epoch record contains `H0`, the latest applied event frontier, the catalog
generation, exact copy identity and frontier, and classification state. It never stores every
object in one metadata value. The cursor uses a physical shard and an ordered
key position, not a list of all remaining object names. The current
implementation provides durable `r` object rows and `p` root-path rows. It
also walks the catalog-to-molecule layer and records one
`schema-attributed` molecule proof for each declared schema field. It adds
bounded molecule-tip pages, physical-object cursors, residue classification,
receipts, and copy scrub in later slices.

## Ongoing attribution maintenance

After initialization, every durable mutation must keep attribution current. A
successful write cannot create unclassified user-plane data.

The current implementation provides the source boundary for public request
writes and cloud replay writes. It stores a flushed pending scope before the
product write, a flushed idempotent event after it, and clears the marker last.
The edge projector and its edge-maintenance hooks remain required work. Until
they ship, the database has a complete source trail but does not claim a live
edge graph.

1. A write creates the new atom and its validated child references.
2. The durable mutation records its tip-to-atom attribution edge before it
   acknowledges the write.
3. A tip replacement removes the old live edge only after the new edge is
   durable.
4. A tip delete removes its edge and adds the atom to the zero-inbound
   candidate queue when its count reaches zero.
5. A schema catalog update adds or retires schema-root edges in the same
   catalog generation.
6. Protein membership updates add or remove their member edges with the
   protein update.
7. The mutation appends one ordered attribution event. The event identifies its
   source row and event sequence.

Use an idempotent mutation identifier for every edge update. On recovery, the
maintenance worker replays unfinished edge records. It never guesses that an
absent record means an absent edge.

If attribution maintenance falls behind, the database marks the affected scope
`unknown`. It blocks clean-copy promotion and destructive reclaim for that
scope. Reads and writes can continue.

A post-cutover update can replace a legacy tip. The write records that the old
tip stopped being live, but it does not delete the old atom. The atom stays
`unknown` until the backfill resolves every possible attribution path.

## Owner commands

The owner CLI exposes maintenance state. These commands are not app APIs.

```text
lastdb db attribution init [--resume] [--page-size N]
lastdb db attribution status
lastdb db attribution schema <schema-identity>
lastdb db inventory --out report.json
lastdb db attribution copy-scrub --copy <path> --epoch <epoch> --execute
lastdb db attribution verify-copy --copy <path> --epoch <epoch>
lastdb db attribution promote-plan --copy <path> --epoch <epoch>
```

`lastdb db inventory` is the existing byte and plane report. Attribution adds
proof rows to its output. The extended report lists every catalog schema and
all attributed descendants. It also reports system, retention, derived,
residue, and unknown objects by class and byte count. It does not present
physical rows as application data.

The report shows two values for shared data. `inclusive_bytes` charges the
full reachable value to every schema that reaches it. `unique_node_bytes`
counts each physical object once. The report never invents an arbitrary split
of a shared atom.

`copy-scrub` accepts only a complete attribution epoch for its named copy. It
deletes only `unattributed-residue` rows from the copy. It emits a receipt for
each bounded batch. It cannot target the source home.

`verify-copy` restores the scrubbed copy and reruns attribution. It passes only
when every user-data object is attributed and no `unknown` row exists.

`promote-plan` is read-only. It compares the source frontier with the verified
copy and lists the separate approval needed for a source cutover. It never
changes the source home. A later source-reclaim command needs its own design
and a new approval.

## Safety rules

1. Do not delete from app ownership, access age, or last-touch time.
2. Do not define an object as residue only because no current schema reaches it.
   Check system and retention roots first.
3. Do not delete during an incomplete or stale attribution epoch.
4. Do not permit source deletion through `copy-scrub`.
5. Do not use retired `schemaidx:` copies as a source-of-truth edge.
6. Do not let a missing atom reduce another object's inbound count.
7. Do not compact before the copy delete receipt is durable.
8. Require a successful cloud delete receipt before claiming cross-replica
   deletion. A local copy delete can complete while cloud sync is blocked.
9. Run the first destructive proof on an isolated copy of a real home.
10. Keep each delete batch bounded, resumable, and idempotent.
11. Treat live tip-version history and valid `as_of` retention as roots until
    the configured history policy removes them.
12. Build the root set from every catalog in the node. Molecules and atoms can
    cross database boundaries on one node.

## Migration and compatibility

The migration has five phases.

| Phase | Work | Delete allowed |
|---|---|---|
| 0 | Ship attribution formats and read-only reports. | No |
| 1 | Ship attribution events and the `H0` to `H1` source-proof protocol. | No |
| 2 | Run source attribution, make exact isolated copies, and compare each schema closure. | No |
| 3 | Enable the edge projector and require a complete epoch. | No |
| 4 | Enable isolated-copy scrub, compact, restore, and verification. | Copy only, with receipts |
| 5 | Design a separate supervised source cutover. | No default source delete |

Existing app manifests can populate display labels during phase 1. They cannot
change the reachability graph or block a reclaim decision.

The migration preserves the current NoSQL contract. It adds a maintenance
root iterator for the catalog. It does not add an unscoped query endpoint.

## Acceptance proof

The implementation passes only when these checks pass on an isolated copy and
then on a supervised live run.

1. The report lists every catalog schema, declared molecule, and system root.
2. A live record keeps every schema and protein attribution path.
3. A shared atom records every reaching schema without duplicated storage.
4. A history root keeps its atom after the current tip no longer reaches it.
5. Every inspected user-data object has one final attribution class.
6. An unreadable atom, missing edge, or interrupted page produces `unknown`
   and blocks copy scrub and promotion.
7. The copy scrub deletes only `unattributed-residue` rows from the copy.
8. A second copy scrub is idempotent.
9. Compaction reduces the selected copy collection's physical bytes after
   logical deletion.
10. A fresh copy restore has zero `unattributed-residue` and zero `unknown`
    user-data rows.
11. An unrooted atom cycle becomes `unattributed-residue` after the complete
    root-mark pass.
12. A write during initialization appears exactly once after event replay.
13. A missing event-log row leaves the epoch incomplete and blocks promotion.
14. A crash after a pending marker leaves that scope `unknown` and blocks
    promotion until a retry or audit resolves it.

## Relationship to existing work

- `ATOM_GC_TIP_FOLD_GATE` already proves that atom reclaim must preserve live
  fold paths. This design makes those paths explicit and durable.
- `DELETE_RETURNS_BYTES` already defines the delete-to-compaction chain. This
  design supplies the schema-root proof before the chain begins.
- `physical-atom-delete-proof` already requires a physical positive control.
  This design requires that proof before a cross-replica delete claim.
- `reap-dropped-schema` remains useful for legacy residue. The new reclaim
  command replaces its limited dropped-tip proof with a complete lifecycle
  proof.
- `lastdb db inventory` already reports byte totals, plane roles, and partial
  per-schema atom data. Attribution extends that report. It does not replace
  the existing owner-only inventory route.
