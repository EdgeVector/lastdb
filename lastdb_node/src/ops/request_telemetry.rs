//! Per-request op telemetry for the Mini daemon.
//!
//! Answers: *who called*, *what kind of op*, *which schema*, *how long*, and
//! *how big* — so operators can see worst offenders (e.g. kanban list storms)
//! without a sidecar metrics store.
//!
//! ## Attribution (self-reported + OS peer)
//!
//! Clients send a free-form identity on every request:
//!
//! - Preferred: `X-LastDB-Client: kanban` / `brain` / `lastgit` / …
//! - Fallback: `X-App-Id` (legacy app-identity hint)
//! - Still missing → the UDS peer process, as `peer:<comm>` (or
//!   `peer:pid-<n>` when the name cannot be read)
//! - Only when the kernel reports no peer pid → `"unknown"`
//! - Optional: `X-LastDB-Request-Id` for joining one client action to child ops
//!
//! Every sample also records, when available:
//!
//! - `path` — `METHOD /route` without query (names the route even when
//!   `client=unknown` and `schema` is empty)
//! - `peer_pid` / `peer_comm` — UDS kernel peer pid and best-effort process
//!   name, so an unlabeled `curl` / agent script can still be named
//!
//! Self-reported labels are **not** a security boundary — any local process
//! can claim any label. Peer pid is kernel-set on UDS and is for ops triage.
//!
//! ## Storage
//!
//! In-process only (ring of recent samples + aggregate map). No durable
//! write path: recording every request into LastDB would amplify the load
//! being measured. Snapshots ride `/api/status` and `lastdb ops`.
//!
//! **Cheap health vs forensics:** default `GET /api/status` (and `lastdb
//! status`) serializes only the request-ops *scalars* (`sample_count`,
//! `ring_capacity`). The 256-sample `recent` ring and ranking tables are
//! opt-in via `?recent=1` / `?forensics=1` (what `lastdb ops` requests).
//! Shipping the full ring on every fleet health probe bloated a busy-primary
//! status body to ~188 KB (~97% request_ops).
//!
//! ## Phases
//!
//! Samples can carry a per-phase microsecond breakdown ([`PhaseTimings`])
//! answering *where inside the node* a slow request spent its time. This
//! module owns only the model; the mutation path populates it separately.
//! An all-zero set means "not reported" and is treated as absent on every
//! surface — omitted from JSON, rendered as nothing.

use fold_db::clock::unix_millis;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Mutex;

use fold_db::request_phases::TipKeySketch;
use lastdb_uds::uds_http::UdsRequest;
use serde::{Deserialize, Serialize};

/// Preferred client self-ID header (case-insensitive on the wire).
pub const CLIENT_HEADER: &str = "x-lastdb-client";
/// Legacy fallback used by some app-identity paths.
pub const CLIENT_HEADER_FALLBACK: &str = "x-app-id";
/// Optional caller-minted correlation ID (case-insensitive on the wire).
pub const REQUEST_ID_HEADER: &str = "x-lastdb-request-id";

const MAX_CLIENT_LEN: usize = 64;
const MAX_REQUEST_ID_LEN: usize = 96;
const MAX_SCHEMA_LEN: usize = 128;
const DEFAULT_RING_CAP: usize = 256;
const DEFAULT_TOP_N: usize = 32;
const MAX_AGGREGATE_KEYS: usize = 512;
const KEY_USE_BUCKET_MS: u64 = 15 * 60 * 1000;
const KEY_USE_BUCKETS: u64 = 4;
const MAX_KEY_USE_SERIES: usize = 128;
/// Distinct HTTP statuses retained per aggregate in the error breakdown.
///
/// Real traffic uses a small closed set (400/404/409/413/500/503). The cap
/// exists so a client that invents statuses cannot grow the map without
/// bound; anything past it is counted in `error_statuses_overflow` rather
/// than dropped, so the breakdown never disagrees with `error_count`.
const MAX_ERROR_STATUS_KEYS: usize = 8;

mod app_verb_render;
mod key_use;
mod op_aggregate;
mod op_kind;
mod ops_render;
mod phase_timings;
mod render_helpers;
mod request_parse;
mod rollup;
mod runtime;
mod snapshot;

pub use app_verb_render::*;
pub use key_use::*;
pub use op_aggregate::*;
pub use op_kind::*;
pub use ops_render::*;
pub use phase_timings::*;
pub use render_helpers::*;
pub use request_parse::*;
pub use rollup::*;
pub use runtime::*;
pub use snapshot::*;
