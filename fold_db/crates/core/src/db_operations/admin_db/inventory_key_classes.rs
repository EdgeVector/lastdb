//! The `main` key-prefix classes the live byte inventory reports.

/// Label of the counter class, which the walk of `atom:` reports beside the bodies.
pub(super) const ATOM_COUNTER_CLASS_LABEL: &str =
    "atom:{uuid}\\0c — atom counter (references to the body)";

pub(super) const MAIN_KEY_CLASSES: &[(&str, &str, &str)] = &[
    ("atom_content", "atom:", "atom:{uuid}\\0b — field values"),
    (
        "mutation_history",
        "history\0",
        "history:{molecule}:{ts} — per-field mutation log",
    ),
    (
        "record_history",
        "rh:",
        "rh:{S}:{sha(record)}:{field}:{id} — field history copies of old record values",
    ),
    (
        "molecule_per_key_index",
        "mk:",
        "mk:{M}:… — current tip pointer per record key",
    ),
    (
        "molecule_header",
        "mh:",
        "mh:{M} — molecule header with the slots and records counters",
    ),
    (
        "molecule_generation_delete",
        "mgd:v1:",
        "mgd:v1:{M}:… — one generation delete marker for each slot an erase removed",
    ),
    (
        "molecule_generation_record",
        "mgr:v1:",
        "mgr:v1:{M}:{generation}:… — immutable generation slot records",
    ),
    (
        "molecule_generation_pointer",
        "mgp:v1:",
        "mgp:v1:{M} — the active generation of a molecule",
    ),
    (
        "delete_barrier",
        "rdel:v2:",
        "rdel:v2:{hex of mk key} — the order of a Delete for one tip key",
    ),
    (
        "conflict_index",
        "mcc:",
        "mcc:{M} — ids of the kept conflict records of a molecule",
    ),
    (
        "conflict",
        "conflict\0",
        "conflict:… — merge conflict records",
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
        "retention_directory",
        "crs:v1:",
        "crs:v1:{M}\0{slot}\0{record} — slot directory of kept conflict records (a purge reads it)",
    ),
    (
        "atom_delete_ledger",
        "dellog\0",
        "dellog:{ts}:{uuid} — hard-delete audit row (purge / delete). Never removed by a janitor.",
    ),
];
