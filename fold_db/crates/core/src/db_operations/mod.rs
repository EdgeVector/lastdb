// Core database operations
pub mod change_feed;
pub mod core;
pub mod db_catalog_store;
// Domain stores (private namespace fields, public operation methods)
pub mod admin_db;
pub mod attribution_ledger;
mod conflict_metadata;
pub mod delete_ledger;
/// Local file-blob reachability GC. Gated on `sharing` because the
/// `cas_blobs` plane and the `$lastdb_file` pointer codec live there; the
/// shipped daemon (`cloud-sync` default) always carries it.
pub mod keep_small;
pub mod lineage_index;
pub mod metadata_store;
pub mod record_store;
/// Molecule key bundles are storage key material; the store lives in
/// `storage`. This path stays for existing callers.
pub use crate::storage::molecule_key_store;
mod public_key_operations;
pub mod public_key_store;
mod schema_operations;
pub mod schema_store;
pub mod search_index;
mod storage_breakdown;
// Re-exports
pub use admin_db::{
    DbInventory, MainKeyClass, SchemaRecordKey, SchemaRecordKeysReport, SchemaRecordKeysRequest,
    SchemaStorageReport,
};
pub use attribution_ledger::{
    classification_for_roots, AttributionClass, AttributionClassCounts, AttributionEvent,
    AttributionLedger, AttributionObjectKind, AttributionPath, AttributionPendingScope,
    AttributionRecord, AttributionRootKind, AttributionSize, AttributionSummary,
    ATTRIBUTION_PATH_PREFIX, ATTRIBUTION_RECORD_PREFIX, LIVE_ATTRIBUTION_EPOCH_ID,
};
pub use change_feed::{ChangeFeedEvent, ChangeFeedPage, ChangeFeedStore};
pub use db_catalog_store::{
    drop_prefixed_rows_for_instance, referenced_instance_ids, DbCatalogEntry,
    DbCatalogKeySelection, DbCatalogStore, UNPREFIXED_INSTANCE_ID,
};
pub use delete_ledger::{
    key_fingerprint, AtomDeleteLedgerEntry, ATOM_DELETE_LEDGER_PREFIX, LEDGER_VERB_DELETE,
    LEDGER_VERB_DELETE_CONVERGE, LEDGER_VERB_PURGE,
};
pub use keep_small::{
    keep_small_schema_shard_key, KeepSmallMeters, KeepSmallSchemaShard, KeepSmallSnapshot,
    MeterDomainTrust, MeterTrustPayload, MeterTrustState, MoleculeStorageCounter, SchemaMeter,
    KEEP_SMALL_SCHEMA_SHARD_PREFIX, KEEP_SMALL_SNAPSHOT_COLLECTION, KEEP_SMALL_SNAPSHOT_KEY,
    KEEP_SMALL_SNAPSHOT_VERSION, KEEP_SMALL_TRUST_VERSION, KEEP_SMALL_UNATTRIBUTED_SCHEMA,
    KEEP_SMALL_UNATTRIBUTED_SHARD_KEY, LIVE_BUDGET_BYTES,
};
pub use record_store::{
    AtomCounterReport, BlobCounterReport, BlobCounterView, BlobRecountReport,
    ConflictRetentionReport, DwellCursor, DwellGuard, DwellStep, NetStep, RecordStore,
    RecountOptions, RecountReport, VerifyReport, VerifyRequest, VerifySummary, WalkStats,
    WriterWatch, DWELL_CHUNK, RECORD_WALK_ROWS,
};
// Annotate matches on this. It stays public on a default build: the host
// crate does not enable `cloud-sync` just to read the page flag.
pub use crate::storage::molecule_key_store::{MoleculeKeyBundle, MoleculeKeyStore};
pub use conflict_metadata::RecordConflictState;
pub use core::{
    note_page_key_forms, note_page_resume, note_walk_positions, page_can_resume,
    with_query_page_tally, with_query_row_drop_tally, with_refilled_query_row_drops, DbOperations,
    KeyForm, PageEnd, PageResume, QueryRowDrops,
};
pub use lineage_index::LineageIndex;
pub use metadata_store::MetadataStore;
pub use public_key_store::PublicKeyStore;
pub use schema_operations::{SchemaRetentionAttributionReport, SchemaRootAttributionReport};
pub use schema_store::{SchemaDropReceipt, SchemaStore};
pub use search_index::{
    run_search_rebuild_for_home, IndexChange, IndexChangeBatch, IndexChangeKind, IndexSink,
    SearchRebuildReport,
};
// SchemaStorage is embedded in DbInventory; StorageBreakdown stays module-private.
pub use storage_breakdown::SchemaStorage;
