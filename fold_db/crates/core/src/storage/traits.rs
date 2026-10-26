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

use super::error::{StorageError, StorageResult};

pub(crate) fn key_u64_after_last_marker(key: &[u8], marker: &[u8]) -> Option<u64> {
    if marker.is_empty() || key.len() < marker.len() {
        return None;
    }
    let marker_start = key.windows(marker.len()).rposition(|part| part == marker)?;
    let suffix = key.get(marker_start + marker.len()..)?;
    std::str::from_utf8(suffix).ok()?.parse().ok()
}

mod scan;
mod stats;
mod typed;

pub use scan::*;
pub use stats::*;
pub use typed::*;

mod kv;
mod namespaced;
pub use kv::KvStore;
pub use namespaced::NamespacedStore;
