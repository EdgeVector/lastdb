//! Read-only retained log authentication and metadata audit. No producer or cloud I/O.
use crate::crypto::CryptoProvider;
use crate::hex::sha256_hex;
use crate::sync::error::{SyncError, SyncResult};
use crate::sync::log::{LogEntry, LogOp};
use crate::sync::snapshot_log::MutationLogSegmentId;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
mod audit;
mod keys;
mod model;
mod retired_source;
mod seal;
mod wire;
pub use audit::{
    audit_pin_log_plane, PinLogPlaneReport, PinLogWriterStat, PIN_LOG_OPERATOR_KEYS_PER_CALL,
};
use model::*;
pub use model::{MutationLogSegment, PinLogRecord, PinLogTargetStatus, PIN_LOG_NAMESPACE};
pub use replay::verify_retired_replay_source;
pub use retired_source::{AuthenticatedReplayObject, RetiredReplaySource};
use wire::*;
mod replay;
use keys::*;
use seal::*;
