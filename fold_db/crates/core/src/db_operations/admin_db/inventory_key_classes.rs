//! The `main` key-prefix classes the live byte inventory reports.

pub(super) const MAIN_KEY_CLASSES: &[(&str, &str, &str)] = &[
    ("record", "rk:", "rk:{S}:{hash}\\0{range} — the record of one key"),
    ("alternate_key", "ak:", "ak:… — the alternate key entries of the records"),
    (
        "record_conflict",
        "rcf:",
        "rcf:{S}:{sha256} — the kept conflict rows of a record",
    ),
    (
        "record_conflict_marker",
        "rcm:",
        "rcm:… — the conflict marker of a record key",
    ),
    (
        "record_history",
        "rh:",
        "rh:{S}:{sha(record)}:{field}:{id} — field history copies of old record values",
    ),
    (
        "blob_counter",
        "bref:v3:c:",
        "bref:v3:c:{sha256(blob_ref)}\\0c — blob counter (references to a local file blob, and its dwell)",
    ),
    (
        "blob_queue",
        "bref:v3:q:",
        "bref:v3:q:{bucket}\0{zero_since}:{sha256(blob_ref)} — time-bucketed dwell queue of local file blobs at zero (the dwell pass reads it)",
    ),
    (
        "blob_queue_bucket_directory",
        "bref:v3:d\0",
        "bref:v3:d\0{bucket} — one row for each bucket of the blob dwell queue (the dwell pass lists it)",
    ),
    (
        "retention_queue",
        "crq:v1:",
        "crq:v1:{bucket}\0{expires}:{record} — time-bucketed queue of kept conflict records (the janitor reads it)",
    ),
    (
        "retention_bucket_directory",
        "crb:v1\0",
        "crb:v1\0{bucket} — one row for each bucket of the retention queue (the janitor lists it)",
    ),
    (
        "record_delete_ledger",
        "dellog\0",
        "dellog:{ts}:{uuid} — hard-delete audit row (purge / delete). Never removed by a janitor.",
    ),
];
