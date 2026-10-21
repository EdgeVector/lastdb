//! lastdbd self-metrics sampler and status surface.
//!
//! **Primary sink (default):** append-only JSONL under the node home
//! (`logs/self-metrics.jsonl`). This does **not** depend on LastDB mutations, so
//! RSS / sync / UDS vitals remain reviewable when the database is wedged
//! (Tom 2026-07-19).
//!
//! **Optional sink:** set `LASTDB_SELF_METRICS_TO_DB=1` to also best-effort write
//! `lastdb_telemetry/SelfMetricSample` rows (series `lastdbd-self`). DB write
//! failures never fail the sample once the log line is written.

use fold_db::clock::unix_secs;
use fold_db::process_cpu::CpuProbe;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use fold_db::access::{AccessContext, CallerTransport};
use fold_db::request_phases::{self, RequestPhase};
use fold_db::schema::types::field::HashRangeFilter;
use fold_db::schema::types::operations::{Mutation, MutationType, Query};
use fold_db::schema::types::schema::DeclarativeSchemaType;
use fold_db::schema::types::{DeclarativeSchemaDefinition, KeyConfig, KeyValue};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::gauge::{Availability, Gauge, Unit, Window};
use crate::host::Host;

mod at_rest;
mod atom_counter_lines;
mod build_health;
mod conflict_retention_lines;
mod constants;
mod drain;
mod durability;
mod env;
mod fields;
mod file_blob;
mod format;
mod gauge_wire;
mod health_lines;
mod integrity;
mod log_line;
mod log_streams;
mod memory_budget;
mod molecule_edge_health;
mod process_probes;
mod prune;
mod purge_lines;
mod qos_uds_watchers;
mod read_health;
mod recoverability;
mod resident;
mod sampler;
mod sampler_state;
mod schema;
mod snapshot_build;
mod snapshot_memory;
mod status_lines;
mod status_snapshot;
mod sync_health;
mod sync_health_wire;
mod sync_lines;
mod sync_probe;
mod ttl_backoff;
mod ttl_pass;
mod write;
mod write_fields;

pub use at_rest::*;
pub use atom_counter_lines::*;
pub use build_health::*;
pub use conflict_retention_lines::*;
pub use constants::*;
pub use drain::*;
pub use durability::*;
pub use env::*;
use fields::*;
pub use file_blob::*;
use format::*;
use gauge_wire::*;
use health_lines::*;
pub use integrity::*;
pub use log_line::*;
pub use log_streams::*;
pub use memory_budget::*;
pub use molecule_edge_health::*;
pub use process_probes::*;
use prune::*;
pub use purge_lines::*;
pub use qos_uds_watchers::*;
pub use read_health::*;
pub(crate) use recoverability::crash_loss_window_secs_from_status_json;
use recoverability::*;
pub use resident::*;
pub use sampler::*;
pub use sampler_state::*;
use schema::*;
pub use snapshot_build::*;
pub use status_lines::*;
pub use status_snapshot::*;
pub use sync_health::*;
pub use sync_lines::*;
use sync_probe::*;
pub use ttl_pass::TtlPassReport;
use write::*;
