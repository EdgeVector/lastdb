//! The `main` key-prefix classes the live byte inventory reports.

/// Label of the counter class, which the walk of `atom:` reports beside the bodies.
pub(super) const ATOM_COUNTER_CLASS_LABEL: &str =
    "atom:{uuid}\\0c — atom counter (references to the body)";

pub(super) const MAIN_KEY_CLASSES: &[(&str, &str, &str)] = &[
    ("atom_content", "atom:", "atom:{uuid}\\0b — field VALUES"),
    (
        "legacy_schema_secondary_index",
        "schemaidx:",
        "schemaidx:{len}:{schema}:{atom} — legacy main-tree marker",
    ),
    (
        "mutation_history",
        "history:",
        "history:{molecule}:{ts} — per-field mutation log",
    ),
    (
        "molecule_per_key_index",
        "mk:",
        "mk:{M}:… — current tip pointer per record key",
    ),
    (
        "tip_version_chain",
        "tv:",
        "tv:{id} — archived tip versions (prev_tip_id chain for as_of)",
    ),
    (
        "molecule_update_order_log",
        "mord:",
        "mord:{M}:{seq} — append-only update order",
    ),
    (
        "molecule_hash_range_page_index",
        "mhr:",
        "mhr:{M}\\0… — HashRange page index",
    ),
    (
        "molecule_legacy_blob",
        "ref:",
        "ref:{M} — pre per-key whole-molecule blob",
    ),
    ("molecule_header", "mh:", "mh:{M} — molecule header"),
    (
        "molecule_update_order_count",
        "moc:",
        "moc:{M} — order log count",
    ),
    (
        "molecule_hash_range_index_complete",
        "mhi:",
        "mhi:v2:{M} — page index complete marker",
    ),
    (
        "molecule_update_order_legacy",
        "mo:",
        "mo:{M} — legacy single-record order",
    ),
    (
        "conflict",
        "conflict:",
        "conflict:… — merge conflict records",
    ),
    (
        "legacy_schema_index_sentinel",
        "schemaidx_v1_done",
        "schemaidx_v1_done legacy main-tree sentinel",
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
        crate::db_operations::delete_ledger::ATOM_DELETE_LEDGER_PREFIX,
        "dellog:{ts}:{uuid} — hard-delete audit row (purge / gc-atoms). Never auto-reaped.",
    ),
];
