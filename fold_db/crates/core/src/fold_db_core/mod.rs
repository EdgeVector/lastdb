//! FoldDB Core — embedded database coordinator.

use async_trait::async_trait;
pub use cloud_backup_protocol::v2::blobs::BlobPath;

pub mod factory;
pub mod fold_db;
pub mod mutation_manager;
pub mod purge;
pub mod query;

pub use fold_db::FoldDB;
pub use mutation_manager::MutationManager;
pub use query::QueryExecutor;

/// Owner-visible body when a personal home has no saved credentials.
pub const SIGN_IN_TO_FETCH_FILE: &str = "sign in to fetch this file";

/// Result of one on-demand blob download.
pub enum BlobFetch {
    Bytes(Vec<u8>),
    Absent,
    Unavailable(String),
}

/// Downloads the sealed row of one record file version.
#[async_trait]
pub trait RecordBlobSource: Send + Sync {
    async fn fetch(&self, path: &BlobPath, sealed_bytes: u64) -> BlobFetch;
}
