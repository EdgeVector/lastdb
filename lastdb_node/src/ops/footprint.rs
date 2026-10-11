//! Measured-footprint LRU defence and operator shed.
//!
//! The node never refuses a request because memory is high. When the host is
//! under memory pressure, or `phys_footprint` crosses the soft line, it evicts
//! idle warm groups in LRU order, holds the effective warm budget at the
//! post-step resident, and asks malloc to return free pages. `/api/admin/shed`
//! is the same path driven by the external guard. The configured warm env is
//! a maximum. It is not the budget `fits` sees during a pressure drain.

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
