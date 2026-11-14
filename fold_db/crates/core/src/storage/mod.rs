//! Native local storage modules, error surface, and internal format capabilities.

pub mod disk_backup;
pub mod disk_cleanup;
pub mod disk_pack;
pub mod error;
pub(crate) mod format;
pub(crate) mod local_file_ref;
pub(in crate::storage) mod retired;

pub(crate) use disk_pack::transaction::traits::KvMutation;
pub use error::{config, config::DatabaseConfig, StorageError};
pub(crate) use format::{content_key_proof, current_keys};
