//! Backend-agnostic storage traits and the typed JSON adapter.
//!
//! ## Trait surface (what to implement vs what to use)
//!
//! | Type | Role |
//! |------|------|
//! | [`KvStore`] | Byte key/value ops. **Implement this** for a new backend. |
//! | [`NamespacedStore`] | Open/list/delete logical trees. **Implement this** too. |
//! | [`TypedKvStore`] | JSON serialize/deserialize helper on top of any `KvStore`. **Use, don't reimplement.** |
//! | [`PartitionedScan`] | Result of poison-aware scans (encrypting backends). |
//!
//! Scan variants on [`KvStore`] (`scan_prefix`, `_keys`, `_paged`, `scan_range`,
//! `_lossy`, `_partition_undecryptable`) are **one family** with different
//! failure/pagination contracts — not parallel APIs. Defaults are correct but
//! unbounded; ordered backends (Last Store) should override for lazy iterators.
//!
//! See the parent [`crate::storage`] module docs for the layering diagram
//! (traits → laststore/inmemory → encrypting wrappers).

mod scan;
mod typed;

pub use scan::*;
pub use typed::*;

mod kv;
mod namespaced;
pub use kv::KvStore;
pub use namespaced::NamespacedStore;
