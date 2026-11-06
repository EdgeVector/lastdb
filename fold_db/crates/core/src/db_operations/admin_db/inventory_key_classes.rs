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
        "record_delete_ledger",
        "dellog\0",
        "dellog\\0{ts}:{uuid} — hard-delete audit row (purge / delete)",
    ),
];
