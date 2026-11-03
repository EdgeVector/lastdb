// Core database operations
pub mod change_feed;
pub mod core;
// Domain stores (private namespace fields, public operation methods)
pub mod admin_db;
pub mod delete_ledger;
pub mod metadata_store;
pub mod record_store;
/// Field key bundles are storage key material; the store lives in
/// `storage`. This path stays for existing callers.
pub use crate::storage::field_key_store;
mod schema_operations;
pub mod schema_store;
pub mod search_index;
mod storage_breakdown;
// Re-exports
pub use crate::storage::field_key_store::{FieldKeyBundle, FieldKeyStore};
pub use admin_db::{
    DbInventory, MainKeyClass, SchemaRecordKey, SchemaRecordKeysReport, SchemaRecordKeysRequest,
    SchemaStorageReport,
};
pub use change_feed::{ChangeFeedEvent, ChangeFeedPage, ChangeFeedStore};
pub use core::{
    note_page_key_forms, note_page_resume, note_walk_positions, page_can_resume,
    with_query_page_tally, with_query_tally, DbOperations, KeyForm, PageEnd, PageKeyForms,
    PageResume,
};
pub use delete_ledger::{
    key_fingerprint, RecordDeleteLedgerEntry, LEDGER_VERB_DELETE, LEDGER_VERB_PURGE,
};
pub use metadata_store::MetadataStore;
pub use record_store::{RecordStore, WalkStats, WriterWatch, RECORD_WALK_ROWS};
pub use schema_store::SchemaStore;
pub use search_index::{
    run_search_rebuild_for_home, IndexChange, IndexChangeBatch, IndexChangeKind, IndexSink,
    SearchRebuildReport,
};
// SchemaStorage is embedded in DbInventory; StorageBreakdown stays module-private.
pub use storage_breakdown::SchemaStorage;
