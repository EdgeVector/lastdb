# Canonical field identity

Schema Service supplies reusable field definitions. A definition describes one field and its type.
Field identity does not determine a molecule address or connect mutable values in different schemas.
Each local schema owns its field molecules and records.

## Stable identity algorithms

The existing canonical algorithm hashes the name, trimmed description, type, and version in a fixed JSON order.
Names remain case-sensitive. The algorithm remains unchanged by the value-model change.

A declared field uses the versioned declaration identity algorithm.
Its input contains the algorithm version, declaration handle, field name, type, and field version.
A local rename or a different app owner does not authorize a record-value relationship.
Preserve stored identity versions. An older binary must not overwrite a newer identity.

## Value addresses

A fresh field molecule derives its address from the schema and field names.
An explicit conversion can retain a private molecule address in schema metadata.
Reads and writes use the installed schema metadata, not a field-identity search across schemas.
Equal field identities can hold different values at the same key in separate schemas.

## Alternate keys

A named alternate key belongs to one source schema and uses its declared fields.
It points to the source primary key and stores no payload copy.
Primary record changes and alternate address changes use one native transaction.
See [the canonical model](lastdb-canonical-model.md) and [the conversion guide](multi-key-schema-conversion.md).
