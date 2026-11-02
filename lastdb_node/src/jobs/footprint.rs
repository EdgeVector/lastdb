//! Native logical memory relief and measured process-memory reports.
//!
//! Pressure removes least-recently-used clean records and asks the
//! allocator to return free pages. Held and unflushed records stay protected.
//! Definitions and complete keys retain their separate allowances. This does
//! not turn the stage-one warm byte measurement into an admission limit.

use fold_db::clock::unix_secs;
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};

use fold_db::memory_budget::{IneffectiveTrimTracker, PressureLatch};

use crate::host::Host;

#[path = "footprint/defend.rs"]
mod defend;
#[path = "footprint/evict.rs"]
mod evict;
#[path = "footprint/governor.rs"]
mod governor;

pub use defend::defend_measured_footprint;
pub use evict::*;
pub use governor::*;

const GIB: u64 = 1024 * 1024 * 1024;
