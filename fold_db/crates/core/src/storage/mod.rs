//! Native local storage, typed helpers, and physical maintenance adapters.

pub mod cloud_backup_plan;
pub mod config;
/// The account-key proof record of a node home. It lives with the store it
/// reads, above the crypto primitives it seals with.
pub(crate) mod content_key_proof;
pub(crate) mod current_keys;
pub mod disk_backup;
pub mod disk_cleanup;
pub mod disk_pack;
pub mod error;
pub(crate) mod format;
pub(crate) mod local_file_ref;
pub mod traits;

// Re-exports for convenience
pub use config::DatabaseConfig;
pub use error::StorageError;
pub use traits::KvMutation;
