//! Self-metrics jobs over a booted host: the sampler loop and the status
//! snapshot build.
//!
//! The status types that these jobs fill live in [`crate::ops::self_metrics`],
//! below the host. These jobs take `&Host`, so they sit above it.
//!
//! Each tick publishes one [`StatusSnapshot`] on the host and appends
//! `logs/self-metrics.jsonl`. `GET /api/status`, `lastdb status`, and
//! `lastdb ops` read that snapshot. The tick does not write LastDB.

use fold_db::clock::unix_secs;
use fold_db::process_cpu::CpuProbe;
use std::time::{Duration, Instant};

use fold_db::request_phases::{self, RequestPhase};

use crate::host::Host;
use crate::ops::self_metrics::*;

mod drain;
mod sampler;
mod snapshot_build;
mod snapshot_memory;

pub use sampler::*;
pub use snapshot_build::*;
