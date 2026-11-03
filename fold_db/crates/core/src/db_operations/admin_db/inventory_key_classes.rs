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
        "retention_queue",
        "crq:",
        "crq:{bucket}\0{expires}:{record} — time-bucketed queue of kept conflict records (the janitor reads it)",
    ),
    (
        "retention_bucket_directory",
        "crb\0",
        "crb\0{bucket} — one row for each bucket of the retention queue (the janitor lists it)",
    ),
    (
        "record_delete_ledger",
        "dellog\0",
        "dellog:{ts}:{uuid} — hard-delete audit row (purge / delete). Never removed by a janitor.",
    ),
];
