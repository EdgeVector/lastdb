//! Local storage layer: traits, backends, and optional wrappers.
//!
//! ## Architecture
//!
//! ```text
//! Domain (db_operations, factory, …)
//!         │
//!         ▼
//! ┌───────────────────┐
//! │  traits::         │  KvStore + NamespacedStore  (backend-agnostic)
//! │  TypedKvStore     │  typed JSON helpers on KvStore
//! └─────────┬─────────┘
//!           │ implemented by
//!     ┌─────┴──────┐
//!     ▼            ▼
//!  laststore::   inmemory_backend
//!  (product)     (tests / ephemeral)
//!     │
//!     ▼ optional wrappers
//!  encrypting_*  (at-rest AES-GCM; migration dual-read)
//!
//! Cloud multi-device export is **not** a KvStore decorator. Store-level
//! capture lives under `crate::sync::capture` (cold path).
//! ```
//!
//! **Last Store only:** Sled was removed after the Mini cutover. Product and
//! test code use [`LastStoreNamespacedStore`] or [`InMemoryNamespacedStore`].

pub mod config;
pub mod encrypting_namespaced_store;
/// Kv-level at-rest encryptor — implementation detail of [`EncryptingNamespacedStore`].
pub(crate) mod encrypting_store;
pub mod error;
pub mod inmemory_backend;
pub mod laststore;
pub mod node_config_store;
pub mod reap_unsealed;
pub mod reseal_at_rest;
pub mod traits;

// Re-exports for convenience
pub use config::{CloudSyncConfig, DatabaseConfig, P2pSyncConfig, StorageEngine};
pub use error::StorageError;
pub use inmemory_backend::InMemoryNamespacedStore;
pub use laststore::{
    apply_tip_residue_copy_page, classify_index_residue_copy, classify_protein_residue_copy,
    classify_tip_residue_copy, dual_read_metrics_reset, dual_read_metrics_snapshot,
    DualReadMetricsSnapshot, IndexPlanePrefixInventory, IndexResidueReclaimOptions,
    IndexResidueReclaimReport, LastStoreNamespacedStore, PlaneResidueCopyAction,
    PlaneResidueDrainOptions, PlaneResidueDrainReport, PlaneResidueFamily, TipResidueCopyAction,
    TipResidueCopyPageReport, TipResidueDrainOptions, TipResidueDrainReport,
    INDEX_RESIDUE_KEY_PREFIXES, INDEX_RESIDUE_LEGACY_COLLECTIONS, ORDER_LOG_COLLECTIONS,
    ORDER_LOG_KEY_PREFIXES, PROTEIN_FAMILY_KEY_PREFIXES, RECLAIMABLE_RETIRED_INDEX_PREFIXES,
    TIP_RESIDUE_KEY_PREFIXES, TIP_RESIDUE_LEGACY_COLLECTIONS,
};

pub use encrypting_namespaced_store::{
    EncryptingNamespacedStore, DEFAULT_ENCRYPT_FLIPPED_NAMESPACES, LASTSTORE_PLAINTEXT_NAMESPACES,
};
pub use node_config_store::{NodeConfigStore, NodeIdentity};
pub use reap_unsealed::{ReapUnsealedCheckpoint, ReapUnsealedOptions, ReapUnsealedReport};
pub use reseal_at_rest::{
    collection_is_allowed, ResealAtRestCheckpoint, ResealAtRestOptions, ResealAtRestReport,
    ResealAtRestTarget, RESEAL_AT_REST_ALLOWLIST,
};
pub use traits::{
    ExecutionModel, FlushBehavior, KvMutation, KvStore, NamespacedStore, PartitionedScan,
    TypedKvStore,
};
