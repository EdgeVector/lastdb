//! The `main` key-prefix classes the live byte inventory reports.

pub(super) const MAIN_KEY_CLASSES: &[(&str, &str, &str)] = &[
    (
        "record",
        "rk:",
        "rk:{S}:{hash}\\0{range} — the record of one key",
    ),
    (
        "alternate_key",
        "ak:",
        "ak:… — the alternate key entries of the records",
    ),
    (
        "record_history",
        "rh:",
        "rh:{S}:{sha(record)}:{field}:{id} — field history copies of old record values",
    ),
    (
        "history_policy",
        "ahp:",
        "ahp:{field} — the history policy of a field",
    ),
    (
        "history_count",
        "ahk:",
        "ahk:{sha(record)} — the history entry count of a record",
    ),
    (
        "history_queue",
        "ahq:",
        "ahq:{bucket}\\0{expires}:{id} — time-bucketed expiry queue of the history entries",
    ),
    (
        "history_bucket_directory",
        "ahb\0",
        "ahb\\0{bucket} — one row for each bucket of the history expiry queue",
    ),
    (
        "record_delete_ledger",
        "dellog\0",
        "dellog\\0{ts}:{uuid} — hard-delete audit row (purge / delete)",
    ),
];
