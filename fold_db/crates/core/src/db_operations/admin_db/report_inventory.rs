//! Inventory, tombstone and history-clear report types, plus the adaptive inventory page sizing.

use super::*;

/// One key-class in the `main` tree (records, alternate keys, history, …).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MainKeyClass {
    pub class: String,
    pub prefix: String,
    pub keys: u64,
    pub bytes: u64,
}

/// Per-schema logical record storage — the dedicated `lastdb db schemas` report.
///
/// This is the record walk (`storage_breakdown`). It does **not** scan
/// history the way [`DbInventory`] does. Its rows count the live records of
/// each schema and their stored value bytes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaStorageReport {
    /// RFC 3339 timestamp when this walk finished.
    pub measured_at: DateTime<Utc>,
    /// Always `true`. This is an admin scan, not a cheap status gauge.
    pub heavy: bool,
    /// One row per schema that has at least one live record, largest first.
    pub per_schema: Vec<SchemaStorage>,
    pub total_logical_bytes: u64,
    /// Live records of all schemas.
    #[serde(default)]
    pub total_records: u64,
    /// Stored value bytes of those records. They are part of
    /// `total_logical_bytes`.
    #[serde(default)]
    pub total_record_bytes: u64,
    pub schema_count: u64,
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
            declared_molecule_count,
            missing_molecule_counters: 0,
            measured_at: Utc::now(),
        }
    }
}

impl SchemaStorageReport {
    pub fn from_breakdown(breakdown: StorageBreakdown) -> Self {
        let total_records = breakdown.per_schema.iter().map(|s| s.record_count).sum();
        let total_record_bytes = breakdown.per_schema.iter().map(|s| s.record_bytes).sum();
        let schema_count = breakdown.per_schema.len() as u64;
        Self {
            measured_at: Utc::now(),
            heavy: true,
            total_logical_bytes: breakdown.total_logical_bytes,
            per_schema: breakdown.per_schema,
            total_records,
            total_record_bytes,
            schema_count,
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
    /// The live records of each schema (`record_count`, `record_bytes`). The
    /// name stays from the atom inventory, because benchmark readers use it.
    pub per_schema_atoms: Vec<SchemaStorage>,
    pub per_schema_atoms_total_bytes: u64,
    /// The range reads this inventory made. A read that used its key budget in a
    /// run of deleted keys went on above the last key it examined (`skips`). A
    /// read that hit another budget of one call was retried with fewer rows or a
    /// narrower key range (`shrinks` and `splits`).
    #[serde(default)]
    pub scan: crate::db_operations::record_store::WalkStats,
    /// Attribution ledger summary: object counts by class (schema/retention/
    /// system/derived/residue/unknown) and total path-row count. Absent
    /// (all-zero) on a home written before this field existed.
    #[serde(default)]
    pub attribution: crate::db_operations::AttributionSummary,
    pub notes: Vec<String>,
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
/// A fixed record limit cannot bound all values: some records contain few bytes
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
