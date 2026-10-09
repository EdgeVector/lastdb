//! Inventory, tombstone and history-clear report types, plus the adaptive inventory page sizing.

use super::*;

/// One key-class in the `main` tree (atoms, history, indexes, …).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MainKeyClass {
    pub class: String,
    pub prefix: String,
    pub keys: u64,
    pub bytes: u64,
}

/// Per-schema logical atom storage — the dedicated `lastdb db schemas` report.
///
/// This is the atom-only walk (`storage_breakdown`). It does **not** scan
/// history, order-log, or tip-format the way [`DbInventory`] does.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaStorageReport {
    /// RFC 3339 timestamp when this walk finished.
    pub measured_at: DateTime<Utc>,
    /// Always `true`. This is an admin scan, not a cheap status gauge.
    pub heavy: bool,
    /// One row per schema that has at least one atom, largest first.
    pub per_schema: Vec<SchemaStorage>,
    pub total_logical_bytes: u64,
    pub total_atoms: u64,
    pub schema_count: u64,
    /// Atom-size histogram from the same walk (p50/p95/p99/max + ≥16/32/64 KiB).
    #[serde(default)]
    pub histogram: crate::db_operations::AtomHistogram,
}

/// Fast logical-current storage for one declared schema catalog.
///
/// Values intentionally overlap when schemas share a molecule or an atom.
/// This is a catalog-plus-counter result, not a physical disk partition.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaCurrentStorageReport {
    pub schema_binding: String,
    pub metric: String,
    pub logical_value_bytes: u64,
    pub schema_structure_bytes: u64,
    pub schema_current_bytes: u64,
    pub active_slot_count: u64,
    pub retained_history_bytes: u64,
    pub deduplicated_across_schemas: bool,
    pub deduplicated_within_schema: bool,
    pub complete: bool,
    /// Why `complete` is false: `hydrate_missed`, `stale_after_unclean_stop`,
    /// `molecule_counters_not_bootstrapped`, `missing_molecule_counters`,
    /// `schema_meter_domain_absent`, or `schema_meter_domain_incomplete`.
    /// Absent when the report is exact.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incomplete_reason: Option<String>,
    pub counter_epoch: u64,
    pub pending_protein_folds: u64,
    pub declared_molecule_count: u64,
    pub missing_molecule_counters: u64,
    pub measured_at: DateTime<Utc>,
}

/// One labelled schema row in the scan-free storage report.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaLogicalStorageRow {
    /// Runtime schema identity used by the catalog and molecule counters.
    pub schema_binding: String,
    /// Human label from the stored schema metadata, or the binding when no
    /// descriptive label exists.
    pub label: String,
    /// Current logical value plus molecule structure bytes.
    pub bytes: u64,
    pub logical_value_bytes: u64,
    pub schema_structure_bytes: u64,
    pub active_slot_count: u64,
    pub complete: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incomplete_reason: Option<String>,
}

/// Labelled logical schema storage from catalog metadata and stored counters.
///
/// This report never reads atoms, tips, or filesystem planes. Shared molecules
/// can appear in more than one schema row by design.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaLogicalStorageReport {
    pub measured_at: DateTime<Utc>,
    /// Always false: the report reads bounded metadata and counters only.
    pub heavy: bool,
    pub complete: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incomplete_reason: Option<String>,
    pub per_schema: Vec<SchemaLogicalStorageRow>,
    pub total_bytes: u64,
    pub total_logical_value_bytes: u64,
    pub total_schema_structure_bytes: u64,
    pub total_active_slot_count: u64,
    pub schema_count: u64,
}

impl SchemaCurrentStorageReport {
    #[must_use]
    pub fn new(schema_binding: String, declared_molecule_count: u64) -> Self {
        Self {
            schema_binding,
            metric: "logical_current_schema_bytes".to_string(),
            logical_value_bytes: 0,
            schema_structure_bytes: 0,
            schema_current_bytes: 0,
            active_slot_count: 0,
            retained_history_bytes: 0,
            deduplicated_across_schemas: false,
            deduplicated_within_schema: false,
            complete: false,
            incomplete_reason: None,
            counter_epoch: 0,
            pending_protein_folds: 0,
            declared_molecule_count,
            missing_molecule_counters: 0,
            measured_at: Utc::now(),
        }
    }
}

impl SchemaStorageReport {
    pub fn from_breakdown(breakdown: StorageBreakdown) -> Self {
        let total_atoms = breakdown.per_schema.iter().map(|s| s.atom_count).sum();
        let schema_count = breakdown.per_schema.len() as u64;
        Self {
            measured_at: Utc::now(),
            heavy: true,
            total_logical_bytes: breakdown.total_logical_bytes,
            per_schema: breakdown.per_schema,
            total_atoms,
            schema_count,
            histogram: breakdown.histogram,
        }
    }
}

/// Full inventory report for owner tooling / `lastdb db inventory`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DbInventory {
    /// Key-class breakdown of the `main` namespace (live decrypted sizes).
    pub main_classes: Vec<MainKeyClass>,
    pub main_total_keys: u64,
    pub main_total_bytes: u64,
    /// Per-schema atom logical sizes (plaintext JSON of atoms).
    pub per_schema_atoms: Vec<SchemaStorage>,
    pub per_schema_atoms_total_bytes: u64,
    /// Rough history row counts per schema (via field molecules).
    pub per_schema_history: Vec<SchemaHistoryStat>,
    /// Per-schema HashRange update-order log sizes (via field molecules).
    #[serde(default)]
    pub per_schema_order_log: Vec<SchemaOrderLogStat>,
    /// Tip format stats (`mk:` values): thin vs legacy fat.
    #[serde(default)]
    pub tip_format: TipFormatStats,
    /// Attribution ledger summary: object counts by class (schema/retention/
    /// system/derived/residue/unknown) and total path-row count. Absent
    /// (all-zero) on a home written before this field existed.
    #[serde(default)]
    pub attribution: crate::db_operations::AttributionSummary,
    pub notes: Vec<String>,
}

/// How many `mk:` tip values are thin vs legacy fat (signed).
///
/// A pass is bounded (see [`AtomStore::scan_tip_format`]), so a report is a
/// window over the `mk:` keyspace, not necessarily the whole of it.
/// `more_remaining` says whether the window closed early; `next_after_key`
/// resumes it. Reading either count as a store total without checking
/// `more_remaining` understates the plane.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TipFormatStats {
    pub tips_scanned: u64,
    pub tips_thin: u64,
    pub tips_fat: u64,
    pub tips_unreadable: u64,
    pub fat_bytes_approx: u64,
    pub thin_bytes_approx: u64,
    /// True when this pass stopped on its key cap or time budget with keys
    /// left in the range.
    #[serde(default)]
    pub more_remaining: bool,
    /// Cursor to resume past: the last `mk:` storage key this pass decided.
    #[serde(default)]
    pub next_after_key: Option<String>,
}

/// Report from rewriting fat `mk:` tips to thin in place.
///
/// Bounded and resumable like [`TipFormatStats`]: one call decides a window of
/// the `mk:` keyspace and reports whether more is left.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ThinTipMigrateReport {
    pub dry_run: bool,
    pub tips_scanned: u64,
    pub tips_already_thin: u64,
    pub tips_rewritten: u64,
    pub tips_unreadable: u64,
    pub bytes_before_approx: u64,
    pub bytes_after_approx: u64,
    /// True when this pass stopped on its key cap or time budget with keys
    /// left in the range.
    #[serde(default)]
    pub more_remaining: bool,
    /// Cursor to resume past: the last `mk:` storage key this pass decided.
    #[serde(default)]
    pub next_after_key: Option<String>,
}

/// Report from deleting empty, unbound `protein:` rows.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProteinGcReport {
    pub dry_run: bool,
    pub proteins_scanned: u64,
    pub proteins_with_members: u64,
    pub proteins_referenced_by_backref: u64,
    pub molprot_backrefs_scanned: u64,
    pub orphan_proteins: u64,
    pub proteins_deleted: u64,
    pub bytes_freed_approx: u64,
}

/// Per-molecule row of [`TombstoneFlagAudit`].
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MoleculeTombstoneFlagStat {
    /// Molecule uuid as it appears in the `mk:{M}:…` key.
    pub molecule: String,
    /// `schema.field`, when the caller supplied a label map.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    pub keys: u64,
    pub meta_tombstoned: u64,
    /// Keys whose `meta.tombstoned` is false but whose atom CONTENT is a
    /// tombstone — the legacy population that still shortens a page.
    pub content_tombstoned_meta_false: u64,
    /// Live keys whose atom body could not be resolved at all.
    pub atoms_missing: u64,
}

/// Result of auditing `KeyMetadata.tombstoned` against atom content.
///
/// `KeyMetadata.tombstoned` is `#[serde(default)]`, so an `mk:` record written
/// before the field existed deserializes as `false` even when its atom content
/// is a tombstone. `PageFill` (fold #919) spends the page window on the flag,
/// and `resolve_value` rejects such a row afterwards by content — so those rows
/// re-open the short-page defect on a narrower population.
///
/// [`Self::content_tombstoned_meta_false`] is the size of that population. Zero
/// means the flag and the content agree everywhere and no backfill is owed.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TombstoneFlagAudit {
    /// `mk:` records walked (after any molecule filter).
    pub keys_scanned: u64,
    /// Records whose value would not deserialize as a `PerKeyRecord`.
    pub keys_unreadable: u64,
    /// Records with `meta.tombstoned == true` — agreed dead, cheap to skip.
    pub meta_tombstoned: u64,
    /// Records with the flag false whose atom body resolved to live content.
    pub live: u64,
    /// **The number this audit exists for**: flag false, content tombstone.
    pub content_tombstoned_meta_false: u64,
    /// Flag false and no atom body resolved (neither flat, hinted, nor located).
    pub atoms_missing: u64,
    /// Atom bodies fetched to decide the above (the audit's real cost).
    pub atoms_fetched: u64,
    /// Molecules touched, worst first (`content_tombstoned_meta_false` desc).
    pub per_molecule: Vec<MoleculeTombstoneFlagStat>,
}

/// Result of stamping `meta.tombstoned` onto legacy `mk:` records.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TombstoneFlagBackfillReport {
    pub dry_run: bool,
    /// The audit that decided the work (also the post-state when `dry_run`).
    pub audit: TombstoneFlagAudit,
    /// `mk:` records rewritten with `meta.tombstoned = true`.
    pub keys_stamped: u64,
    /// True when a `max_keys` cap stopped this call before the end of the
    /// selected records. Resume with `after_key = next_after_key`.
    pub more_remaining: bool,
    /// Last selected key this call decided — the resume cursor. `None` when the
    /// call selected nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_after_key: Option<String>,
}

/// One tombstoned storage slot discovered by the bounded owner walk.
///
/// Storage hash/range segments are intentionally not serializable or
/// debuggable: they may be BlindV1/OpeV1 material and only travel directly to
/// the guarded storage-slot purge implementation.
pub(crate) struct LegacyTombstoneSlot {
    pub molecule_uuid: String,
    pub storage_hash: String,
    pub storage_range: String,
}

/// Internal result of one bounded storage-cursor tombstone discovery pass.
pub(crate) struct LegacyTombstoneScan {
    pub keys_scanned: u64,
    pub keys_unreadable: u64,
    pub atoms_fetched: u64,
    pub atoms_missing: u64,
    pub slots: Vec<LegacyTombstoneSlot>,
    pub more_remaining: bool,
    pub next_after_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaHistoryStat {
    pub schema_name: String,
    pub history_events: u64,
    pub history_bytes_approx: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaOrderLogStat {
    pub schema_name: String,
    pub order_log_entries: u64,
    pub order_log_bytes_approx: u64,
    pub order_count_keys: u64,
    pub order_count_bytes_approx: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryClearReport {
    pub dry_run: bool,
    pub keep_last_per_key: usize,
    pub schemas_touched: usize,
    pub history_rows_deleted: u64,
    pub history_bytes_freed_approx: u64,
    pub per_schema: Vec<SchemaHistoryClearStat>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaHistoryClearStat {
    pub schema_name: String,
    pub history_rows_deleted: u64,
    pub history_bytes_freed_approx: u64,
}

/// Peak resident bytes one inventory page is allowed to hold.
///
/// `db inventory` is the verb an operator reaches for when a node is *already*
/// under memory pressure and they want to know how big something is, so the
/// measurement must not cost what it measures. Every accumulator below reads a
/// bounded page, folds it into two integers, and drops it — peak footprint is
/// one page rather than one keyspace.
pub(in crate::db_operations) const INVENTORY_PAGE_BYTE_BUDGET: u64 = 8 * 1024 * 1024;

/// Lower bound on the adaptive page, and the size of the *first* page of every
/// scan.
///
/// One fixed row limit cannot serve this verb: `moc:` rows are tens of bytes
/// while `atom:` rows run to `ABSOLUTE_MAX_ATOM_CONTENT_BYTES` (1 MiB). A page
/// large enough to keep the small classes cheap in round trips is a spike on
/// the large ones, so the first page is deliberately small and each page
/// re-aims the next from the average row size it just measured.
pub(in crate::db_operations) const INVENTORY_PAGE_ROWS_MIN: usize = 16;

/// Upper bound on the adaptive page, so a class of tiny rows cannot grow the
/// page without limit on row count alone.
pub(in crate::db_operations) const INVENTORY_PAGE_ROWS_MAX: usize = 4096;

/// Re-aim the next page at [`INVENTORY_PAGE_BYTE_BUDGET`] from what the page
/// just read actually weighed.
pub(in crate::db_operations) fn inventory_next_page_rows(
    page_bytes: u64,
    rows_in_page: usize,
) -> usize {
    let avg = (page_bytes / rows_in_page.max(1) as u64).max(1);
    usize::try_from(INVENTORY_PAGE_BYTE_BUDGET / avg)
        .unwrap_or(INVENTORY_PAGE_ROWS_MAX)
        .clamp(INVENTORY_PAGE_ROWS_MIN, INVENTORY_PAGE_ROWS_MAX)
}
