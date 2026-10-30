# LastDB storage layout

The logical path is schema → field → molecule → atom → file.
A schema owns its field molecules and record values.
Canonical field identity supplies reusable definitions and types.

## Primary records and alternate keys

A field molecule maps a primary key to the current atom for that field.
A named alternate molecule maps its declared key to the primary record key.
It stores no payload atom or separate field tip.
One native transaction changes the record and all affected alternate addresses.

New field molecules derive their default address from the schema and field names.
Converted schemas retain their private addresses in schema metadata.
The installed metadata supplies the authority for reads and writes.

## Bytes and counters

The native engine stores encrypted sorted run files.
The main store holds logical records and metadata. The CAS store holds large file bytes.
An atom points to its file. A logical tip stores an atom identity.

The counter planner updates tip references, atom counters, and molecule counters in the same batch.
A zero atom counter releases the body in that batch.
A kept conflict record or field history entry can retain a reference to the body.
The history policy controls those retained field values.

Schema fields and database catalog references bind molecules.
A molecule with no bindings, slots, or kept records can leave through the molecule drain.
Compaction returns the disk bytes after logical deletion.
File blob counters and their dwell control CAS cleanup.

## Memory, backup, and upgrades

The warm set contains logical records selected by requests.
Storage objects and file handles stay outside that logical set.
A database catalog can explicitly reference an existing schema's molecules on the same node.
Equal field definitions in different schemas do not create that reference.

Native backups retain encrypted bytes and their format metadata.
A compatible format marker alone does not complete an app data conversion.
The daemon refuses older shared-value declarations at startup.
Complete the explicit conversion with a previous compatible release before the upgrade.

Keep the primary unchanged during source and CI work.
Use [the conversion guide](multi-key-schema-conversion.md) for the data boundary.
Use [the canonical model](lastdb-canonical-model.md) for the complete record and counter rules.
