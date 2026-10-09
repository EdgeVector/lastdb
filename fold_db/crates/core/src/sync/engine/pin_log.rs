//! Target-scoped durable mutation log for cloud-sync pin mode + continuous
//! mutation-log-first segment upload (Phase A single-writer scaffold; Phase B
//! multi-writer concurrent streams).
//!
//! Pin mode freezes a sealed base set at F0 and sends post-F0 mutations to an
//! append-only log for the active sync target. Continuous MutationLog capture
//! reuses the same durable store without freeze. This module owns the local
//! durable log, continuous segment seal/upload under `log/{writer_id}/{seq}`,
//! published frontier F (per-writer vector + scalar max), and replay/status
//! helpers. Snapshot publish orchestration for rare compact stays with the
//! backup controller.
//!
//! Multi-writer (Phase B): each device/process has a stable `writer_id`
//! (`SyncEngine::device_id`). Writers append anytime without a snapshot lock;
//! sealed segments land under distinct `log/{writer_id}/` prefixes on the
//! shared cloud plane. Local R/W never awaits upload.

use super::super::org_sync::SyncTarget;
use super::restore_progress::{self as progress, RestorePhase, RestoreProgress, TransferOperation};
use super::*;
use crate::clock::unix_millis;
use crate::hex::sha256_hex;
use crate::sync::snapshot_log::{Frontier, MutationLogSegmentId};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

mod append;
mod audit;
mod compaction;
mod frontier;
mod keys;
mod local_cloud;
mod model;
mod offline_s0_marker;
mod persist;
mod pin_mode;
mod replay;
mod restore;
mod seal;
mod state;
mod upload_cycle;
mod wire;

pub use audit::*;
use compaction::*;
use keys::*;
pub use local_cloud::*;
pub use model::*;
pub use offline_s0_marker::*;
pub use replay::*;
pub use restore::*;
pub use seal::*;
pub(crate) use state::*;
use wire::*;
