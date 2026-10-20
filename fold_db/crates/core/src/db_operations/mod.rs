// Core database operations
pub mod change_feed;
pub mod core;
pub mod db_catalog_store;
// Domain stores (private namespace fields, public operation methods)
pub mod admin_db;
pub mod atom_store;
pub mod attribution_ledger;
mod conflict_index_heal;
mod conflict_metadata;
mod conflict_operations;
pub mod delete_ledger;
/// Local file-blob reachability GC. Gated on `sharing` because the
/// `cas_blobs` plane and the `$lastdb_file` pointer codec live there; the
/// shipped daemon (`cloud-sync` default) always carries it.
pub mod file_blob_gc;
pub mod keep_small;
pub mod lineage_index;
pub(crate) mod logical_membership;
pub mod metadata_store;
pub mod molecule_key_store;
mod public_key_operations;
pub mod public_key_store;
pub mod retired_intents;
mod schema_operations;
pub mod schema_store;
pub mod search_index;
mod storage_breakdown;
pub mod tip_atoms;
// Re-exports
pub(crate) use admin_db::GcAtomsRunScope;
pub use admin_db::{
    last_locator_only_probe, AtomGcReport, AutomaticGcAtomsDeleteCheckpoint,
    AutomaticGcAtomsDeleteOptions, AutomaticGcAtomsDeletePhase, AutomaticGcAtomsDeleteReport,
    AutomaticGcAtomsDeleteResult, AutomaticGcAtomsGraceGate, AutomaticGcAtomsPinLogReferencePage,
    AutomaticGcAtomsProbeCheckpoint, AutomaticGcAtomsProbeOptions, AutomaticGcAtomsProbePhase,
    AutomaticGcAtomsProbeReport, AutomaticGcAtomsProbeResult, DanglingTipRepairOptions,
    DanglingTipRepairReport, DanglingTipRepairScope, DanglingTipRepairScopeReport, DbInventory,
    GcAtomsRevalidationTally, HistoryClearReport, LocatorOnlyPopulationReport,
    LocatorOnlyProbeOptions, MainKeyClass, MoleculeHashBucketReport, MoleculeHashBucketRow,
    MoleculeKeyRow, MoleculeKeysReport, ProteinGcReport, SchemaCurrentStorageReport,
    SchemaHistoryClearStat, SchemaHistoryStat, SchemaLogicalStorageReport, SchemaLogicalStorageRow,
    SchemaRecordKey, SchemaRecordKeysReport, SchemaRecordKeysRequest, SchemaStorageReport,
    DEFAULT_LOCATOR_ONLY_PROBE_MAX_TIPS, GC_ATOMS_DELETE_CHECKPOINT_KEY,
    GC_ATOMS_PROBE_CHECKPOINT_KEY, LOCATOR_ONLY_PROBE_STRATA, LOCATOR_ONLY_PROBE_TARGET_QUOTA,
};
pub use atom_store::{
    allow_after_peer_apply_from_env, PeerApplyState, ALLOW_AFTER_PEER_APPLY_ENV, PEER_APPLY_KEY,
};
pub(crate) use atom_store::{automatic_gc_audit_id, ChangedKey, FilterLayout, OneDSlot};
pub use atom_store::{
    AtomGcAuditDecision, AtomGcAuditResult, AtomGcCandidate, AtomGcCandidatePage,
    AtomGcReapOptions, AtomGcReapReport, AtomStore, DroppedSchemaReapCursor,
    DroppedSchemaReapPhase, DroppedSchemaReapReport, DroppedSchemaTipProbe, DroppedSchemaTipProof,
    MoleculeData, DEFAULT_ATOM_GC_GRACE_WINDOW, MAX_DROPPED_SCHEMA_REAP_OPS,
};
pub use attribution_ledger::{
    classification_for_roots, AttributionClass, AttributionClassCounts, AttributionEvent,
    AttributionLedger, AttributionObjectKind, AttributionPath, AttributionPendingScope,
    AttributionRecord, AttributionRootKind, AttributionSize, AttributionSummary,
    ATTRIBUTION_PATH_PREFIX, ATTRIBUTION_RECORD_PREFIX, LIVE_ATTRIBUTION_EPOCH_ID,
};
pub use change_feed::{ChangeFeedEvent, ChangeFeedPage, ChangeFeedStore};
pub use db_catalog_store::{
    drop_prefixed_rows_for_instance, leftover_db_hash_prefix, referenced_instance_ids,
    DbCatalogEntry, DbCatalogKeySelection, DbCatalogStore, UNPREFIXED_INSTANCE_ID,
};
pub use delete_ledger::{
    key_fingerprint, AtomDeleteLedgerEntry, DeleteLedgerHandle, ATOM_DELETE_LEDGER_PREFIX,
    LEDGER_VERB_DELETE, LEDGER_VERB_DELETE_CONVERGE, LEDGER_VERB_GC_ATOMS, LEDGER_VERB_PURGE,
    LEDGER_VERB_REPAIR_TIPS,
};
pub use keep_small::{
    atom_histogram, keep_small_schema_shard_key, AtomHistogram, KeepSmallHardEraseDebit,
    KeepSmallHardEraseTotals, KeepSmallMeters, KeepSmallSchemaShard, KeepSmallSnapshot,
    LiveBudgetTotals, MeterDomainTrust, MeterTrustPayload, MeterTrustState, MoleculeStorageCounter,
    MoleculeTipCounterSource, SchemaMeter, ATOM_HISTOGRAM_16KIB, ATOM_HISTOGRAM_32KIB,
    ATOM_HISTOGRAM_64KIB, KEEP_SMALL_HARD_ERASE_TOTALS_KEY, KEEP_SMALL_SCHEMA_SHARD_PREFIX,
    KEEP_SMALL_SNAPSHOT_COLLECTION, KEEP_SMALL_SNAPSHOT_KEY, KEEP_SMALL_TRUST_VERSION,
    KEEP_SMALL_UNATTRIBUTED_SCHEMA, KEEP_SMALL_UNATTRIBUTED_SHARD_KEY, LIVE_BUDGET_BYTES,
};
// Annotate matches on this. It stays public on a default build: the host
// crate does not enable `cloud-sync` just to read the page flag.
pub use conflict_metadata::{MoleculeConflictState, SELECTED_CONFLICT_MAX_MOLECULES};
pub use conflict_operations::HomeConflictAnnotation;
// Replay appends one event in the same batch that records a conflict. The
// engine is gated behind `cloud-sync`. A default-feature build must not
// re-export these or clippy fails on the unused names.
pub(crate) use conflict_operations::{
    home_conflict_event_key, next_home_conflict_event_seq, HomeConflictEvent,
};
pub use core::{
    note_page_key_forms, note_tombstoned_row, with_query_row_drop_tally, DbOperations, KeyForm,
    MoleculeGateHold, MoleculeGateHoldStats, QueryRowDrops,
};
pub use lineage_index::LineageIndex;
pub use metadata_store::MetadataStore;
pub use molecule_key_store::{MoleculeKeyBundle, MoleculeKeyStore};
pub use public_key_store::PublicKeyStore;
pub use schema_operations::{SchemaRetentionAttributionReport, SchemaRootAttributionReport};
pub use schema_store::{SchemaDropReceipt, SchemaStore};
pub use search_index::{
    run_search_rebuild_for_home, IndexChange, IndexChangeBatch, IndexChangeKind, IndexSink,
    SearchRebuildReport,
};
// SchemaStorage is embedded in DbInventory; StorageBreakdown stays module-private.
pub use storage_breakdown::SchemaStorage;
