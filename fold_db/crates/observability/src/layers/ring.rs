//! RING layer — a bounded in-memory record of tracing events.
//!
//! The layer keeps the newest events. The handle keeps that buffer alive.
//! No caller reads the buffer back.

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
    /// Random UUID assigned when the event is recorded.
    pub id: String,
    /// Unix time in milliseconds, or 0 when the clock is before the epoch.
    pub timestamp: i64,
    /// Tracing level of the event.
    pub level: LogLevel,
    /// Tracing target of the event.
    pub event_type: String,
    /// Message taken from the event fields.
    pub message: String,
    /// Present on the JSON shape. The ring recorder stores `None`.
    pub user_id: Option<String>,
    /// Other tracing fields, or `None` when the event has none.
    pub metadata: Option<HashMap<String, String>>,
}

/// Log level. Serializes to UPPERCASE strings (`"TRACE"`, `"DEBUG"`, ...) —
/// the same shape the legacy `LoggingSystem::LogLevel` used.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum LogLevel {
    /// Serializes as `TRACE`.
    Trace,
    /// Serializes as `DEBUG`.
    Debug,
    /// Serializes as `INFO`.
    Info,
    /// Serializes as `WARN`.
    Warn,
    /// Serializes as `ERROR`.
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

/// Handle that keeps the RING buffer alive. The recording layer writes events
/// through it. Cheap to clone — internally an `Arc`.
#[derive(Clone)]
pub struct RingHandle {
    buffer: Arc<RwLock<VecDeque<LogEntry>>>,
    capacity: usize,
}

/// Subscriber layer that records each event into a bounded ring buffer.
pub struct RingLayer {
    handle: RingHandle,
}

/// Build a RING layer and the handle that keeps its buffer alive.
/// Capacity is clamped to a minimum of 1. A zero-capacity ring drops every event.
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
