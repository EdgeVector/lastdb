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
        "molecule_binding_record",
        "mref:v1:e:",
        "mref:v1:e:{M}\\0{source} — one record for each schema field, protein member or catalog that binds a molecule",
    ),
    (
        "molecule_binding_counter",
        "mref:v1:n:",
        "mref:v1:n:{M} — bindings counter of a molecule",
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
    // Protein membership. Absent from this table until 2026-07-30, which
    // is why an orphan-protein leak grew unnoticed: each prefix is
    // scanned independently and there is no catch-all bucket, so an
    // unlisted key class is not merely mis-labelled — it is invisible,
    // and `lastdb db inventory` under-reports the main tree by its size.
    (
        "protein_record",
        "protein:",
        "protein:{uuid} — protein record (member list). Nothing deletes these; empty ones are orphans.",
    ),
    (
        "protein_member_backref",
        "molprot:",
        "molprot:{M} → protein uuid — molecule's binding back-ref",
    ),
    (
        "protein_fold_state",
        "fldprot:",
        "fldprot:{field_hash} — per-field protein fold state",
    ),
    (
        "protein_fold_queue",
        "pfq:",
        "pfq:{job_id} — pending sibling-tip fold job",
    ),
    // Listed so the audit trail's own footprint stays visible. Nothing
    // reaps these by design (see `db_operations::delete_ledger`), so an
    // unbounded class that is NOT reported would be exactly the kind of
    // invisible growth the protein-record entry above was added for.
    (
        "atom_delete_ledger",
        "dellog\0",
        "dellog:{ts}:{uuid} — hard-delete audit row (purge / delete). Never removed by a janitor.",
    ),
];
