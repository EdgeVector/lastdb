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
        "molecule_drain_candidate",
        "mref:v1:d\0",
        "mref:v1:d\0{scope}\0{M} — a molecule that lost its last binding; the drain removes it and deletes the row",
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
        "dellog:{ts}:{uuid} — hard-delete audit row (purge / gc-atoms). Never auto-reaped.",
    ),
];
