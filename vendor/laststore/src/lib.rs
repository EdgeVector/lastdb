//! # Last Store
//!
//! The disk engine of [LastDB](https://thelastdb.com): disk format v2
//! ([`disk_run`]). A home holds immutable, sorted, encrypted run files and one
//! published `CURRENT` list of them. See `README.md` for the layout.
//!
//! Crate name on crates.io / Cargo: **`laststore`**. Product name: **Last Store**.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod commit_timing;
pub mod disk_run;
mod durability;
mod error;
pub mod frame;
mod read_receipt;
mod read_receipt_io;
pub mod space;

pub use error::{Error, Result};
pub use read_receipt::{
    observe_disk_reads, record_decryption, record_lock_wait, record_logical_warm_hits,
    record_logical_warm_misses, record_queue_wait, record_requested_keys, DiskReadContext,
    DiskReadReceipt,
};

/// Crate version string.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
