//! Persisted LastDB-home storage ledger.
//!
//! `GET /api/storage/home` reads one metadata key and returns this snapshot.
//! It never inventories the filesystem or product collections. The bounded
//! reconcile job owns snapshot creation; the read path only validates the
//! persisted arithmetic before it can call a report complete.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::fs;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};

/// Point-read key for the latest durable home-storage snapshot.
pub const HOME_STORAGE_SNAPSHOT_KEY: &str = "home_storage:snapshot:v1";

/// Point-read key for the active bounded filesystem inventory.
pub const HOME_STORAGE_RECONCILE_KEY: &str = "home_storage:reconcile:v1";

pub const HOME_STORAGE_RECONCILE_WORK_DEFAULT: usize = 128;
pub const HOME_STORAGE_RECONCILE_WORK_MAX: usize = 65_536;
const ROOTS_PER_BUCKET_MAX: usize = 16;
const UNRESOLVED_SCOPES_MAX: usize = 64;
const DIRECTORY_LISTING_BYTES_MAX: usize = 64 * 1024 * 1024;

/// Public metric name. This changes only with a wire-contract change.
pub const HOME_STORAGE_METRIC: &str = "lastdb_home_storage_v1";

/// Public metric name for logical data counted once per reaching app.
pub const HOME_STORAGE_APP_ATTRIBUTION_METRIC: &str = "lastdb_home_inclusive_app_logical_bytes_v1";

/// `10_000` basis points means `1.0x`.
const ONE_X_BASIS_POINTS: u64 = 10_000;

/// One mutually exclusive top-level class below the configured LastDB home.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HomeStorageBucketKind {
    DatabaseStore,
    DatabaseAuxiliary,
    Backup,
    Recovery,
    RuntimeBinary,
    RuntimeApp,
    Log,
    Candidate,
    /// A path that the current classifier does not recognize. It remains an
    /// accounted byte; a future release can give the path a better name.
    UnknownPath,
}

/// Exact aggregate for one mutually exclusive home class.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HomeStorageBucket {
    pub kind: HomeStorageBucketKind,
    pub apparent_bytes: u64,
    pub allocated_bytes: u64,
    /// Number of distinct filesystem entries folded into this bucket.
    pub entry_count: u64,
    /// Largest or otherwise useful roots that explain the bucket. Reconcile
    /// bounds this list; the normal GET only returns the persisted values.
    #[serde(default)]
    pub roots: Vec<String>,
}

/// Filesystem totals measured at one snapshot frontier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct HomeStorageTotals {
    pub apparent_bytes: u64,
    pub allocated_bytes: u64,
}

mod attribution;
mod reconcile;
mod report;

pub use attribution::*;
pub use reconcile::*;
pub use report::*;
