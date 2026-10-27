//! Native local storage, typed helpers, and physical maintenance adapters.
//!
//! ## Architecture
//!
//! ```text
//! Domain (db_operations, factory, …)
//!         │  TypedKvStore / KvStore / NamespacedStore
//!         ▼
//! EncryptingNamespacedStore (local CAS records only)
//!         │
//!         ▼
//! disk_pack::PackNamespacedStore (native node home: format v2 run files
//!         for every new home, v1 packed files for an older home)
//! ```
//!
//! Cloud multi-device export is **not** a KvStore decorator. No store-level
//! capture exists; `crate::sync::capture` holds only the mutation admission
//! gate. Native cloud activation remains disabled.
//!
//! Normal logical constructors use the native packed engine and refuse older
//! nonempty formats without migration.

pub mod config;
pub(crate) mod current_keys;
pub mod disk_pack;
pub mod encrypting_namespaced_store;
/// Kv-level at-rest encryptor — implementation detail of [`EncryptingNamespacedStore`].
pub(crate) mod encrypting_store;
pub mod error;
pub mod traits;

// Re-exports for convenience
pub use config::{CloudSyncConfig, DatabaseConfig, P2pSyncConfig, StorageEngine};
pub use error::StorageError;

pub use encrypting_namespaced_store::{EncryptingNamespacedStore, CATALOG_NAMESPACES};
pub use traits::{KvMutation, KvStore, NamespacedStore, PartitionedScan, TypedKvStore};
