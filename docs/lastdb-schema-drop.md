# Schema drop

A schema drop first stores the schema state `Blocked`. Every write reads the
state inside the native write gate. A write during the drop gets the refusal
"is blocked and cannot be mutated", and no record lands behind the release.
Queries get a refusal from the same point.

The drop then releases the stored entries of the schema. After that, it
removes the schema record and its catalog entries
(`record_store/schema_release.rs`). The release reads only the key prefixes of
the schema in its record scope:

- the records and their tombstones (`rk:{S}:`);
- the field history entries (`rh:{S}:`);
- the entries of each declared alternate key (`ak:{field_key}:`).

Each batch reads at most 256 entries. One write deletes them with their blob
counter changes, their kept conflict records and the history entry counts of
their records. The request waits for the release. A drop that fails or times
out leaves the schema installed and blocked. The next drop continues from the
keys that remain.

An installed schema cannot change its alternate keys. So the entries of the
current alternate revision are all the alternate entries that the schema has.

The entries of a database binding live in the scope of its instance. The
catalog drop releases that scope. The records that another user sent live in a
`from:` scope that belongs to the share.

The pacing of the background writers is in `docs/lastdb-time-queue.md`.
