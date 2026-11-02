# LastDB storage layout

The logical path is schema → record → file blob, plus alternate entries.
Every home is format 8 (the record model). A schema owns its records and their values.
Canonical field identity supplies reusable definitions and types.
A key has one stored record. No other stored value holds the fields of that key.

## Records and alternate keys

A record key (`rk:`) holds one record: every field of the schema at one primary key,
a stamp for each field, and one record metadata block. A tombstone is a record with no
fields and a delete floor. No separate delete barrier exists.
A named alternate entry (`ak:`) maps its declared key to the primary key.
It stores no copy of the record.
One native transaction changes the record and all affected alternate entries.

Each schema has one record space identity. Schema publication mints one key bundle
for that record space.
The installed schema metadata supplies the authority for reads and writes.

## Bytes and counters

The native engine stores encrypted sorted run files.
The main store holds records, alternate entries and metadata. The CAS store holds file blobs.
A field value or the record metadata names a file blob by its reference.

The write path changes the file blob counters from the old and the new record in the same batch.
A field history entry that names a file blob holds one reference. A conflict record holds none.
File blob counters and their one-hour dwell control CAS cleanup.
Compaction returns the disk bytes after logical deletion.

## Memory, backup, and upgrades

The warm set holds one record at one key, selected by requests. Alternate entries stay on disk.
Storage objects and file handles stay outside that set.
A database catalog can explicitly reference an existing schema on the same node.
Equal field definitions in different schemas do not create that reference.

Native backups retain encrypted bytes and their format metadata.
Backup profile 6 describes a format 8 home. The cloud contract stays V2.
This release opens only a format 8 home. Any other home gets the generic failure.
A move from an older home is one manual copy into a fresh home. No in-place conversion exists.

Keep the primary unchanged during source and CI work.
Use [the multi-key schema guide](multi-key-schemas.md) for the data boundary.
Use [the canonical model](lastdb-canonical-model.md) for the complete record and counter rules.
