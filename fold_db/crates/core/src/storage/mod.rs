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
//! disk_pack::PackNamespacedStore (native node home: sorted run files.
//!         Open refuses a home of another format.)
//! ```
//!
//! Cloud multi-device export is **not** a KvStore decorator. No store-level
//! capture exists; `crate::fold_db_core::admission` holds only the mutation
//! admission gate. Backup V2 is a separate module, and the disk home opens its run files.
//!
//! Normal constructors use the native packed engine. They refuse a nonempty
//! home of another format.

pub mod config;
/// The account-key proof record of a node home. It lives with the store it
/// reads, above the crypto primitives it seals with.
pub(crate) mod content_key_proof;
pub(crate) mod current_keys;
pub mod disk_backup;
pub mod disk_cleanup;
pub mod disk_pack;
pub mod encrypting_namespaced_store;
/// Kv-level at-rest encryptor — implementation detail of [`EncryptingNamespacedStore`].
pub(crate) mod encrypting_store;
pub mod error;
/// per-field-key key bundles: the storage key material behind field key hashes.
pub mod field_key_store;
pub mod traits;

// Re-exports for convenience
pub use config::{DatabaseConfig, StorageEngine};
pub use error::StorageError;

pub use encrypting_namespaced_store::EncryptingNamespacedStore;
pub use traits::{KvMutation, KvStore, NamespacedStore, PartitionedScan, TypedKvStore};
