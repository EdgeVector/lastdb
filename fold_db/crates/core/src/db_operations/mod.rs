// Core database operations
pub mod core;
// Domain stores (private namespace fields, public operation methods)
pub mod admin_db;
pub mod delete_ledger;
pub mod record_store;
mod schema_operations;
pub mod schema_store;
pub mod search_index;
mod storage_breakdown;
// Re-exports
pub use crate::storage::format::metadata::{OutgoingPortablePage, PortablePeerProgress};
pub use admin_db::{
    SchemaRecordKey, SchemaRecordKeysReport, SchemaRecordKeysRequest, SchemaStorageReport,
};
pub use core::{
    note_page_key_forms, note_page_resume, page_can_resume, with_query_page_tally,
    with_query_tally, DbOperations, KeyForm, PageEnd, PageKeyForms, PageResume,
};
pub use delete_ledger::{
    key_fingerprint, RecordDeleteLedgerEntry, LEDGER_VERB_DELETE, LEDGER_VERB_PURGE,
};
pub use record_store::{RecordStore, WriterWatch};
pub use schema_store::SchemaStore;
pub use search_index::{IndexChange, IndexChangeBatch, IndexChangeKind};
pub use storage_breakdown::SchemaStorage;
