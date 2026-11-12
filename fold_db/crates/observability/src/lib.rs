//! Shared telemetry substrate for the LastDB workspace.
#![warn(missing_docs)]

mod error_layer;

use std::collections::HashSet;
use std::{fmt, io, time::SystemTime, time::UNIX_EPOCH};

use once_cell::sync::OnceCell;
use serde_json::{Map, Value};
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_log::LogTracer;
use tracing_subscriber::fmt::{format::Writer, FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::{layer::SubscriberExt, registry::LookupSpan, EnvFilter, Registry};

use error_layer::{build_error_layer, SentryGuard};

static INIT_ONCE: OnceCell<()> = OnceCell::new();

const STATIC_DENY_LIST: &[&str] = &[
    "auth_token",
    "auth.token",
    "password",
    "api_key",
    "api.key",
    "secret",
    "email",
    "phone",
    "ssn",
];

/// Panic marker hooks.
pub mod crash {
    use std::io::Write;

    /// Emit only a fixed marker to stderr.
    pub fn write_panic_marker() {
        let _ = writeln!(std::io::stderr().lock(), "LastDB panic");
    }

    /// Install fixed panic output so captured stderr cannot retain raw panic payloads.
    pub fn install_private_panic_hook() {
        std::panic::set_hook(Box::new(|_| write_panic_marker()));
    }
}

/// Subscriber layer namespaces.
pub mod layers {
    /// Optional Sentry ERROR sink.
    pub mod error {
        pub use crate::error_layer::{
            build_error_layer, ErrorLayer, SentryGuard, OBS_SENTRY_RELEASE_ENV,
        };
    }
}

/// Outbound HTTP trace propagation hook.
pub mod propagation {
    /// Pass-through hook at outgoing HTTP call sites.
    pub fn inject_w3c<T>(builder: T) -> T {
        builder
    }
}

/// Errors raised by [`init_lambda`].
#[derive(Debug, thiserror::Error)]
pub enum ObsError {
    /// `init_lambda` was called more than once in the same process.
    #[error("observability already initialized")]
    AlreadyInitialized,
    /// Could not install the global tracing subscriber.
    #[error("failed to install tracing subscriber: {0}")]
    SubscriberInstall(String),
}

/// RAII handle returned by [`init_lambda`].
#[must_use = "ObsGuard must be held for the lifetime of the binary or log lines may be dropped"]
pub struct ObsGuard {
    _sentry: Option<SentryGuard>,
    _worker: WorkerGuard,
}

impl fmt::Debug for ObsGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObsGuard")
            .field("sentry_guard", &self._sentry.is_some())
            .finish_non_exhaustive()
    }
}

/// Initialize observability for an AWS Lambda handler.
pub fn init_lambda(service_name: &'static str, _version: &str) -> Result<ObsGuard, ObsError> {
    assert!(
        !service_name.trim().is_empty(),
        "service.name must be non-empty (got {service_name:?})"
    );
    INIT_ONCE
        .set(())
        .map_err(|()| ObsError::AlreadyInitialized)?;
    let _ = LogTracer::init();
    let (writer, _worker) = tracing_appender::non_blocking(io::stdout());
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let (error_layer, _sentry) = build_error_layer().unzip();
    let fmt_layer = tracing_subscriber::fmt::layer()
        .event_format(RedactingFormat::from_env())
        .with_writer(writer);
    tracing::subscriber::set_global_default(
        Registry::default()
            .with(env_filter)
            .with(fmt_layer)
            .with(error_layer),
    )
    .map_err(|e| ObsError::SubscriberInstall(e.to_string()))?;
    Ok(ObsGuard { _sentry, _worker })
}

#[derive(Clone, Debug)]
struct RedactingFormat {
    deny_list: HashSet<String>,
}

impl RedactingFormat {
    fn from_env() -> Self {
        let extra = std::env::var("OBS_REDACT_EXTRA").unwrap_or_default();
        let deny_list = STATIC_DENY_LIST
            .iter()
            .copied()
            .chain(extra.split(',').map(str::trim).filter(|s| !s.is_empty()))
            .map(str::to_owned)
            .collect();
        Self { deny_list }
    }
}

impl<S, N> FormatEvent<S, N> for RedactingFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let meta = event.metadata();
        let level = *meta.level();
        let mut visitor = JsonFieldVisitor {
            deny_list: &self.deny_list,
            body: None,
            attributes: Map::new(),
        };
        event.record(&mut visitor);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let severity_num: u32 = match level {
            Level::TRACE => 1,
            Level::DEBUG => 5,
            Level::INFO => 9,
            Level::WARN => 13,
            Level::ERROR => 17,
        };
        let mut obj = Map::from_iter([
            ("time_unix_nano".into(), Value::String(nanos.to_string())),
            ("severity_text".into(), Value::String(level.to_string())),
            ("severity_number".into(), Value::from(severity_num)),
            (
                "body".into(),
                Value::String(visitor.body.unwrap_or_default()),
            ),
            ("target".into(), Value::String(meta.target().to_string())),
        ]);
        if let Some(span) = ctx.lookup_current() {
            obj.insert("span".into(), Value::String(span.name().to_string()));
        }
        if !visitor.attributes.is_empty() {
            obj.insert("attributes".into(), Value::Object(visitor.attributes));
        }
        writeln!(
            writer,
            "{}",
            serde_json::to_string(&Value::Object(obj)).map_err(|_| fmt::Error)?
        )
    }
}

struct JsonFieldVisitor<'a> {
    deny_list: &'a HashSet<String>,
    body: Option<String>,
    attributes: Map<String, Value>,
}

impl JsonFieldVisitor<'_> {
    fn record_field(&mut self, name: &str, value: Value) {
        if name == "message" {
            self.body = Some(match value {
                Value::String(s) => s,
                other => other.to_string(),
            });
        } else {
            let val = if self.deny_list.contains(name) {
                Value::String("<redacted>".into())
            } else {
                value
            };
            self.attributes.insert(name.to_owned(), val);
        }
    }
}

impl Visit for JsonFieldVisitor<'_> {
    fn record_str(&mut self, f: &Field, v: &str) {
        self.record_field(f.name(), Value::String(v.into()));
    }
    fn record_i64(&mut self, f: &Field, v: i64) {
        self.record_field(f.name(), Value::Number(v.into()));
    }
    fn record_u64(&mut self, f: &Field, v: u64) {
        self.record_field(f.name(), Value::Number(v.into()));
    }
    fn record_f64(&mut self, f: &Field, v: f64) {
        self.record_field(
            f.name(),
            serde_json::Number::from_f64(v).map_or(Value::Null, Value::Number),
        );
    }
    fn record_bool(&mut self, f: &Field, v: bool) {
        self.record_field(f.name(), Value::Bool(v));
    }
    fn record_debug(&mut self, f: &Field, v: &dyn fmt::Debug) {
        self.record_field(f.name(), Value::String(format!("{v:?}")));
    }
}
