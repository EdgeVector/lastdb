//! WEB layer — fans out each event as a JSON `LogEntry` over a
//! [`tokio::sync::broadcast`] channel for the dashboard's
//! `/api/logs/stream` SSE consumer.
//!
//! Phase 3 / T4. Replaced the pre-existing `WebOutput` (since retired with
//! `LoggingSystem` in Phase 3 / T7), which serialized a `LogEntry` to JSON
//! and broadcast it on a `broadcast::Sender<String>`. This layer is the
//! tracing-native version that `/api/logs/stream` subscribes to.
//!
//! ## Wire shape
//!
//! Identical to [`crate::layers::ring::LogEntry`] so the dashboard parser
//! does not change between RING/poll and WEB/stream:
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
//! `trace_id` and `span_id` are added to `metadata` when a parent span
//! carries an `OtelData` extension — purely additive, the dashboard's
//! existing parser ignores unknown metadata keys.
//!
//! ## Backpressure
//!
//! [`broadcast::Sender::send`] is non-blocking and returns `Err` only when
//! there are no live receivers — it does not block the tracing pipeline
//! when subscribers fall behind. Slow consumers see `RecvError::Lagged`
//! on their next `recv()` and skip ahead; we accept that trade-off for
//! the SSE endpoint where dropping is preferable to back-pressuring the
//! whole process.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::broadcast;
use tracing::{Event, Subscriber};
#[cfg(feature = "otel")]
use tracing_opentelemetry::OtelData;
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

use crate::layers::ring::{FieldVisitor, LogEntry, LogLevel};

/// Default capacity for the WEB broadcast channel when `init_*` does not
/// specify one. Sized to absorb a brief burst without forcing slow SSE
/// consumers into `Lagged` immediately.
pub const OBS_WEB_CAPACITY: usize = 1024;

/// Cheap-to-clone handle that hands out broadcast receivers for the WEB
/// layer. Phase 3 / T5 will hold one of these inside the HTTP server state
/// so each `/api/logs/stream` connection can call [`WebHandle::subscribe`].
#[derive(Clone)]
pub struct WebHandle {
    sender: Arc<broadcast::Sender<String>>,
}

impl WebHandle {
    /// New SSE-style receiver of serialized [`LogEntry`] JSON strings.
    pub fn subscribe(&self) -> broadcast::Receiver<String> {
        self.sender.subscribe()
    }

    /// Current number of live subscribers — primarily for tests.
    pub fn receiver_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

/// Subscriber layer that serializes each event as a [`LogEntry`] JSON
/// string and fans it out over the broadcast channel held by
/// [`WebHandle`].
pub struct WebLayer {
    sender: Arc<broadcast::Sender<String>>,
}

/// Build a WEB layer + the handle that lets HTTP handlers subscribe to it.
/// Capacity is clamped to a minimum of 1 — `broadcast::channel(0)` panics.
pub fn build_web_layer(capacity: usize) -> (WebLayer, WebHandle) {
    let cap = capacity.max(1);
    let (sender, _) = broadcast::channel(cap);
    let sender = Arc::new(sender);
    (
        WebLayer {
            sender: sender.clone(),
        },
        WebHandle { sender },
    )
}

impl<S> Layer<S> for WebLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let event_meta = event.metadata();
        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);

        #[cfg_attr(not(feature = "otel"), allow(unused_mut))]
        let mut metadata_map = visitor.fields;

        // See the long-form rationale on `RingLayer::on_event` — same
        // dance: lift trace_id / span_id from the parent span's
        // `OtelData` extension when the `otel` feature is on and an
        // OpenTelemetry layer is installed. Absent that the keys simply
        // don't appear, which the dashboard parser tolerates.
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

        // Serialize once, fan out to every subscriber. `send` is
        // non-blocking and returns Err only when no receivers exist —
        // that's expected before the dashboard connects, so we
        // deliberately swallow the error rather than dropping
        // observability noise into stderr on every event.
        if let Ok(json) = serde_json::to_string(&entry) {
            let _ = self.sender.send(json);
        }
    }
}
