//! # Last Store
//!
//! The disk engine of [LastDB](https://thelastdb.com): disk format v2
//! ([`disk_run`]). A home holds immutable, sorted, encrypted run files and one
//! published `CURRENT` list of them. See `README.md` for the layout.
//!
//! Crate name on crates.io / Cargo: **`laststore`**. Product name: **Last Store**.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod disk_run;
mod durability;
mod error;
pub mod frame;
pub mod space;

pub use error::{Error, Result};

/// True when isolated storage diagnostics (`LASTDB_SNAPSHOT_TIMING=1`) are enabled.
pub fn diagnostics_enabled() -> bool {
    std::env::var("LASTDB_SNAPSHOT_TIMING").is_ok_and(|v| matches!(v.trim(), "1" | "true" | "on"))
}

/// Crate version string.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
