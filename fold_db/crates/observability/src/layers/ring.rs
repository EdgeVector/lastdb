//! RING layer — bounded in-memory `VecDeque<LogEntry>` queryable in-process.
//!
//! Phase 1 / T5. The RING layer captures every event the registry emits into
//! a fixed-capacity ring buffer that another part of the process can drain on
//! demand. In Phase 3 it replaces `LoggingSystem::query_logs` and powers the
//! `/api/logs` HTTP endpoint that the dashboard polls.
//!
//! ## LogEntry shape
//!
//! The on-the-wire JSON shape preserves the legacy `LoggingSystem::LogEntry`
//! contract (retired in Phase 3 / T7) so the dashboard parser did not change
//! when the endpoint was rewired:
//!
//! ```json
//! {
//!   "id": "<uuid v4>",
//!   "timestamp": 1714060800123,
//!   "level": "INFO",
//!   "event_type": "module::path",
//!   "message": "the formatted event message",
//!   "user_id": null,
//!   "metadata": { "trace_id": "...", "span_id": "...", "field.name": "..." }
//! }
//! ```
//!
//! `user_id` is left as `None` here on purpose: task-local user context lives
//! in `fold_db_core` and the observability crate has zero deps on it. Phase 3
//! will bridge that when it consolidates logging — until then RING is the
//! plumbing, not the policy.
//!
//! ## Trace correlation
//!
//! When a `tracing-opentelemetry` layer is also installed in the registry,
//! the current span carries a real W3C span context. The RING layer reads
//! that context and writes `trace_id` (32 hex chars) and `span_id` (16 hex
//! chars) into `LogEntry.metadata` so individual log lines can be joined
//! against distributed traces at query time.
//!
//! ## Concurrency
//!
//! `on_event` is synchronous and called on the event-emitting thread. The
//! buffer is guarded by `std::sync::RwLock`, which is cheap on the hot write
//! path because writes hold the lock for the duration of one `push_back` (+
//! one `pop_front` at capacity). Queries take a read lock and clone out the
//! slice they need.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
#[cfg(feature = "otel")]
use tracing_opentelemetry::OtelData;
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

/// Default capacity for the RING buffer when `init_*` does not specify one.
pub const OBS_RING_CAPACITY: usize = 5000;

/// In-memory log entry. JSON shape preserves the legacy
/// `LoggingSystem::LogEntry` contract (retired in Phase 3 / T7).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogEntry {
    pub id: String,
    pub timestamp: i64,
    pub level: LogLevel,
    pub event_type: String,
    pub message: String,
    pub user_id: Option<String>,
    pub metadata: Option<HashMap<String, String>>,
}

/// Log level. Serializes to UPPERCASE strings (`"TRACE"`, `"DEBUG"`, ...) —
/// the same shape the legacy `LoggingSystem::LogLevel` used.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    pub(super) fn from_tracing(level: &tracing::Level) -> Self {
        match *level {
            tracing::Level::TRACE => Self::Trace,
            tracing::Level::DEBUG => Self::Debug,
            tracing::Level::INFO => Self::Info,
            tracing::Level::WARN => Self::Warn,
            tracing::Level::ERROR => Self::Error,
        }
    }
}

/// Handle to a RING buffer that lets other parts of the process query the
/// recently-emitted entries. Cheap to clone — internally an `Arc`.
#[derive(Clone)]
pub struct RingHandle {
    buffer: Arc<RwLock<VecDeque<LogEntry>>>,
    capacity: usize,
}

impl RingHandle {
    /// Return up to `limit` most-recent entries, optionally filtered to those
    /// with `timestamp >= from_timestamp`. Results are ordered oldest → newest
    /// to match the existing `WebOutput::query` contract that the dashboard
    /// already consumes.
    pub fn query(&self, limit: Option<usize>, from_timestamp: Option<i64>) -> Vec<LogEntry> {
        self.query_range(limit, from_timestamp, None)
    }

    /// Like [`Self::query`] but bounds the upper end of the time window too:
    /// entries are kept when `from_timestamp <= timestamp <= until_timestamp`
    /// (ms since epoch; either bound optional). Powers `folddb logs --since
    /// --until` over the daemon path. Results stay oldest → newest, and an
    /// explicit `limit` keeps the most-recent N *within* the window.
    pub fn query_range(
        &self,
        limit: Option<usize>,
        from_timestamp: Option<i64>,
        until_timestamp: Option<i64>,
    ) -> Vec<LogEntry> {
        let buf = self
            .buffer
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let from_ts = from_timestamp.unwrap_or(i64::MIN);
        let until_ts = until_timestamp.unwrap_or(i64::MAX);

        // Walk newest → oldest so an explicit `limit` keeps the *most recent*
        // N entries; reverse at the end to restore chronological order.
        let mut picked: Vec<LogEntry> = buf
            .iter()
            .rev()
            .filter(|e| e.timestamp >= from_ts && e.timestamp <= until_ts)
            .take(limit.unwrap_or(usize::MAX))
            .cloned()
            .collect();
        picked.reverse();
        picked
    }

    /// Buffer capacity (the bound enforced on each push).
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Render the most-recent `limit` entries as plain text lines, oldest →
    /// newest, one entry per line: `<timestamp_ms> <LEVEL> <event_type>:
    /// <message>`. Used by the crash-report writer to embed a tail of the
    /// in-memory event log into the on-disk report. Kept dependency-free (no
    /// timestamp formatting) so it stays callable from a panic hook where we
    /// must avoid surprises.
    pub fn tail_text(&self, limit: usize) -> Vec<String> {
        self.query(Some(limit), None)
            .into_iter()
            .map(|e| {
                let level: &str = match e.level {
                    LogLevel::Trace => "TRACE",
                    LogLevel::Debug => "DEBUG",
                    LogLevel::Info => "INFO",
                    LogLevel::Warn => "WARN",
                    LogLevel::Error => "ERROR",
                };
                format!("{} {} {}: {}", e.timestamp, level, e.event_type, e.message)
            })
            .collect()
    }

    /// Current entry count. Primarily for tests; not part of the stable API
    /// that `/api/logs` will rely on.
    pub fn len(&self) -> usize {
        self.buffer
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    /// `true` when no entries have been recorded yet.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Subscriber layer that records each event into a bounded ring buffer.
pub struct RingLayer {
    handle: RingHandle,
}

/// Build a RING layer + the handle that lets the rest of the process query
/// it. Capacity is clamped to a minimum of 1 — a zero-capacity ring is a
/// foot-gun that silently drops every event.
pub fn build_ring_layer(capacity: usize) -> (RingLayer, RingHandle) {
    let cap = capacity.max(1);
    let handle = RingHandle {
        buffer: Arc::new(RwLock::new(VecDeque::with_capacity(cap))),
        capacity: cap,
    };
    (
        RingLayer {
            handle: handle.clone(),
        },
        handle,
    )
}

impl<S> Layer<S> for RingLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let event_meta = event.metadata();
        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);

        #[cfg_attr(not(feature = "otel"), allow(unused_mut))]
        let mut metadata_map = visitor.fields;

        // Lift trace_id / span_id directly off the parent span's `OtelData`
        // extension when the `otel` feature is enabled. Absent that feature
        // (Lambda light) or when no OTel layer is installed, skip these fields.
        #[cfg(feature = "otel")]
        if let Some(span_ref) = ctx.event_span(event) {
            let exts = span_ref.extensions();
            if let Some(otel_data) = exts.get::<OtelData>() {
                if let Some(trace_id) = otel_data.builder.trace_id {
                    metadata_map.insert("trace_id".to_string(), format!("{trace_id:032x}"));
                }
                if let Some(span_id) = otel_data.builder.span_id {
                    metadata_map.insert("span_id".to_string(), format!("{span_id:016x}"));
                }
            }
        }
        #[cfg(not(feature = "otel"))]
        let _ = ctx;

        let entry = LogEntry {
            id: uuid::Uuid::new_v4().to_string(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_millis() as i64),
            level: LogLevel::from_tracing(event_meta.level()),
            event_type: event_meta.target().to_string(),
            message: visitor.message,
            user_id: None,
            metadata: if metadata_map.is_empty() {
                None
            } else {
                Some(metadata_map)
            },
        };

        let mut buf = self
            .handle
            .buffer
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if buf.len() == self.handle.capacity {
            buf.pop_front();
        }
        buf.push_back(entry);
    }
}

/// `tracing::field::Visit` impl that pulls the `message` field out separately
/// (it's how `tracing::info!("hello")` is recorded — as a debug field named
/// `message`) and stuffs everything else into a `String → String` map.
#[derive(Default)]
pub(super) struct FieldVisitor {
    pub(super) message: String,
    pub(super) fields: HashMap<String, String>,
}

impl FieldVisitor {
    fn store(&mut self, field: &Field, value: String) {
        if field.name() == "message" {
            self.message = value;
        } else {
            self.fields.insert(field.name().to_string(), value);
        }
    }
}

impl Visit for FieldVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.store(field, value.to_string());
    }
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.store(field, format!("{value:?}"));
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.store(field, value.to_string());
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.store(field, value.to_string());
    }
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.store(field, value.to_string());
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.store(field, value.to_string());
    }
    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.store(field, value.to_string());
    }
}
