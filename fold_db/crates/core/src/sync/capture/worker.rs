//! Unchanged local automatic-compaction policy and trigger definitions.
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
mod compact_policy;
mod compact_triggers;
pub(crate) use compact_policy::*;
pub(crate) use compact_triggers::*;
