//! Backend-agnostic storage traits.
//!
//! ## Trait surface
//!
//! | Type | Role |
//! |------|------|
//! | [`KvStore`] | Byte key/value ops. **Implement this** for a new backend. |
//! | [`NamespacedStore`] | Open logical trees. **Implement this** too. |
//!
//! Scan variants on [`KvStore`] (`scan_prefix`, `_keys`, `_paged`, `scan_range`)
//! are **one family** with different failure/pagination contracts — not parallel APIs.

mod kv;
mod namespaced;
mod scan;

pub use kv::KvStore;
pub use namespaced::NamespacedStore;
pub use scan::*;
