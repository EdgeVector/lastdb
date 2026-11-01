# Molecule drain (retired)

A format 6 home kept the field molecules of a dropped schema (tips, headers and
bindings) until a background drain erased them. A schema install waited for the
drain of the same schema (`SchemaBeingDropped`).

Record model plan #911 replaced that layout. A format 7 home stores one record
for each key (`rk:`) and has no field molecule rows. API version 15 removed the
`molecule-drain` route, and record model 8f removed the drain.

A schema drop first stores the schema state `Blocked`. Every write reads the
state inside the native write gate, so a write during the drop is refused with
"is blocked and cannot be mutated", and no row lands behind the release. Queries
are refused from the same point.

The drop then releases the rows of the schema before it removes the schema
record and its catalog rows (`atom_store/schema_release.rs`). It reads only
the key prefixes of the schema in its record scope: the records and their
tombstones (`rk:`), the field history entries (`rh:`) and the entries of each
declared alternate key (`ak:`). Each batch reads at most 256 rows and deletes
them with their blob counter changes, their conflict rows and their history
entry counts in one write. The request waits for the release. A drop that
fails or times out leaves the schema installed and blocked, and the next drop
goes on. An installed schema cannot change its alternate keys, so the entries
of the current alternate revision are all the alternate entries it has.

The pacing of the background writers is in `docs/lastdb-time-queue.md`.
