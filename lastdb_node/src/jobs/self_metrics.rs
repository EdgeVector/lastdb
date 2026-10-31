//! Self-metrics jobs over a booted host: the sampler loop, the status snapshot
//! build, the telemetry schema heal, the row writes, and retention prune and
//! drain.
//!
//! The status types that these jobs fill live in [`crate::ops::self_metrics`],
//! below the host. These jobs take `&Host`, so they sit above it.

use fold_db::clock::unix_secs;
use fold_db::process_cpu::CpuProbe;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use fold_db::access::{AccessContext, CallerTransport};
use fold_db::request_phases::{self, RequestPhase};
use fold_db::schema::types::field::HashRangeFilter;
use fold_db::schema::types::operations::{Mutation, MutationType, Query};
use fold_db::schema::types::schema::DeclarativeSchemaType;
use fold_db::schema::types::{DeclarativeSchemaDefinition, KeyConfig, KeyValue};
use serde_json::{json, Value};

use crate::host::Host;
use crate::ops::self_metrics::*;

mod drain;
mod prune;
mod sampler;
mod schema;
mod snapshot_build;
mod snapshot_memory;
mod write;
mod write_fields;

pub use drain::*;
use prune::*;
pub use sampler::*;
use schema::*;
pub use snapshot_build::*;
use write::*;
