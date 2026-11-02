# Counter safety during restore

A normal write changes a record, its blob counters, and its queue entries in one
native batch. A restore must keep that rule before cleanup can delete data.

## Restore

A V2 restore installs the files of a home into an inactive native target. A
serving process refuses a restore before any data change. The restore proof
(`storage/disk_backup/run_restore/proof.rs`) reads the two authenticated roots,
main and CAS, and holds them for the whole proof. It opens no live home. The
stages, in order:

- Account: the backup profile must be 6, the key format entry `layout:keys`
  must be `typed-ranges`, and the delete replay cutoff must be valid.
- Schemas: the typed schema and range profiles, and the field keys in the
  `field_keys` namespace.
- Blobs: each key of the `blob_counts` collection passes the current key law.
- Records: each file pointer in a record or in a field history entry (`rh:`)
  names a blob that the CAS root holds. The proof copies and opens each blob.

The restore reply counts the proved records in `verified_records`.

The blob counters come back with the records that they count. The proof checks
their keys. It does not recount the references. To compare the counters with the
references, run `lastdb db verify-refs` on the started home, or
`lastdb db recount` on a stopped home (`docs/lastdb-blob-counter.md`).
