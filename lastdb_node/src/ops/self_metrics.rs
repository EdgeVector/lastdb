//! lastdbd self-metrics status model: the status snapshot, the sampler state,
//! and the status lines.
//!
//! The host holds this state, so it sits below the host. The sampler loop and
//! the snapshot build take `&Host`; they live in [`crate::jobs::self_metrics`].
//!
//! **Primary sink (default):** append-only JSONL under the node home
//! (`logs/self-metrics.jsonl`). This does **not** depend on LastDB mutations, so
//! RSS / UDS vitals remain reviewable when the database is wedged
//! (Tom 2026-07-19).
//!
//! **Optional sink:** set `LASTDB_SELF_METRICS_TO_DB=1` to also best-effort write
//! `lastdb_telemetry/SelfMetricSample` rows (series `lastdbd-self`). DB write
//! failures never fail the sample once the log line is written.

use fold_db::clock::unix_secs;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::json;

use super::gauge::{Availability, Gauge, Unit, Window};

#[macro_use]
mod gauge_health;

mod at_rest;
mod atom_counter_lines;
mod build_health;
mod conflict_retention_lines;
mod constants;
mod durability;
mod env;
mod fields;
mod footprint_governor;
mod format;
mod gauge_wire;
mod health_lines;
mod integrity;
mod log_line;
mod log_streams;
mod memory_budget;
mod process_probes;
mod purge_lines;
mod qos_uds_watchers;
mod read_health;
mod recoverability;
mod request_ops_lines;
mod request_ops_rollup;
mod resident;
mod sampler_state;
mod status_lines;
mod status_snapshot;
mod ttl_backoff;
mod ttl_pass;

pub use at_rest::*;
pub use atom_counter_lines::*;
pub use build_health::*;
pub use conflict_retention_lines::*;
pub use constants::*;
pub use durability::*;
pub use env::*;
pub(crate) use fields::*;
pub use footprint_governor::*;
pub(crate) use format::*;
pub(crate) use gauge_wire::*;
use health_lines::*;
pub use integrity::*;
pub use log_line::*;
pub use log_streams::*;
pub use memory_budget::*;
pub use process_probes::*;
pub use purge_lines::*;
pub use qos_uds_watchers::*;
pub use read_health::*;
use recoverability::*;
pub use request_ops_lines::*;
use request_ops_rollup::*;
pub use resident::*;
pub use sampler_state::*;
pub use status_lines::*;
pub use status_snapshot::*;
pub use ttl_pass::TtlPassReport;
