# Personal field key authority

A format 8 home keeps one key bundle for each field key of a schema: the record space, the field IDs and the alternate key IDs.
A bundle lives in the `field_keys` namespace. Its metadata entry is `fkb:{field_key_id}`.
Each access domain gets one encrypted wrap entry, `fkw:{field_key_id}:{domain hash}`.
The [canonical model](lastdb-canonical-model.md) wins where this page differs.

Schema publication establishes the complete key contract of the schema before its native publication.
The plaintext bundle stays only in the process cache. A catalog share adds a wrap. It never encrypts the field key data again.

Personal queries, counts, and mutation batches load the existing key bundles of each touched schema before they use field keys.
The exact schema metadata supplies the field key identities.
Two batch reads resolve all uncached bundles of one schema: the metadata entries, then the node wraps.
This step does not fetch record values.

Corrupt metadata or a missing node wrap refuses the operation.
Personal writes do not create bundles or change record encryption.
Named database writes keep the bundle creation path of the schema.

Personal batches load their addressed schemas before preconditions, record reads, and key plans.
The schema supplies its own field keys and named alternate keys.
Namespaces, access checks, count rules, and warm set record rules keep their own authority.

Code: `fold_db/crates/core/src/storage/field_key_store.rs` and its `batch_load.rs` and `establish.rs`.
