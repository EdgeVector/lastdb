//! Initialization and redacting JSON log formatter for AWS Lambda handlers.

use std::collections::HashSet;
use std::fmt;
use std::io;
use std::time::{SystemTime, UNIX_EPOCH};

use once_cell::sync::OnceCell;
use serde_json::{Map, Value};
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_log::LogTracer;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{EnvFilter, Registry};

use crate::error_layer::{build_error_layer, SentryGuard};
use crate::ObsError;

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
const REDACTED_PLACEHOLDER: &str = "<redacted>";
const OBS_REDACT_EXTRA_ENV: &str = "OBS_REDACT_EXTRA";

struct FmtGuard {
    _worker: WorkerGuard,
}

/// RAII handle returned by [`init_lambda`].
#[must_use = "ObsGuard must be held for the lifetime of the binary or log lines may be dropped"]
pub struct ObsGuard {
    fmt_guard: Option<FmtGuard>,
    sentry_guard: Option<SentryGuard>,
}

impl fmt::Debug for ObsGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObsGuard")
            .field("fmt_guard", &self.fmt_guard.is_some())
            .field("sentry_guard", &self.sentry_guard.is_some())
            .finish()
    }
}

impl Drop for ObsGuard {
    fn drop(&mut self) {
        let _ = self.sentry_guard.take();
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

    let (writer, worker) = tracing_appender::non_blocking(io::stdout());
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let (error_layer, sentry_guard) = match build_error_layer() {
        Some((layer, guard)) => (Some(layer), Some(guard)),
        None => (None, None),
    };
    let fmt_layer = tracing_subscriber::fmt::layer()
        .event_format(RedactingFormat::from_env())
        .with_writer(writer);

    let subscriber = Registry::default()
        .with(env_filter)
        .with(fmt_layer)
        .with(error_layer);
    tracing::subscriber::set_global_default(subscriber)
        .map_err(|e| ObsError::SubscriberInstall(e.to_string()))?;

    Ok(ObsGuard {
        fmt_guard: Some(FmtGuard { _worker: worker }),
        sentry_guard,
    })
}

#[derive(Clone, Debug)]
struct RedactingFormat {
    deny_list: HashSet<String>,
}

impl RedactingFormat {
    fn from_env() -> Self {
        let mut set: HashSet<String> = STATIC_DENY_LIST.iter().map(|s| (*s).to_string()).collect();
        let raw = std::env::var(OBS_REDACT_EXTRA_ENV).unwrap_or_default();
        for extra in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            set.insert(extra.to_string());
        }
        Self { deny_list: set }
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
        let metadata = event.metadata();
        let level = *metadata.level();
        let mut visitor = JsonFieldVisitor {
            deny_list: &self.deny_list,
            body: None,
            attributes: Map::new(),
        };
        event.record(&mut visitor);

        let mut obj = Map::new();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        obj.insert("time_unix_nano".into(), Value::String(nanos.to_string()));
        obj.insert("severity_text".into(), Value::String(level.to_string()));
        obj.insert(
            "severity_number".into(),
            Value::from(match level {
                Level::TRACE => 1_u32,
                Level::DEBUG => 5,
                Level::INFO => 9,
                Level::WARN => 13,
                Level::ERROR => 17,
            }),
        );
        obj.insert(
            "body".into(),
            Value::String(visitor.body.unwrap_or_default()),
        );
        obj.insert(
            "target".into(),
            Value::String(metadata.target().to_string()),
        );
        if let Some(span) = ctx.lookup_current() {
            obj.insert("span".into(), Value::String(span.name().to_string()));
        }
        if !visitor.attributes.is_empty() {
            obj.insert("attributes".into(), Value::Object(visitor.attributes));
        }

        let line = serde_json::to_string(&Value::Object(obj)).map_err(|_| fmt::Error)?;
        writeln!(writer, "{line}")
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
            return;
        }
        let final_value = if self.deny_list.contains(name) {
            Value::String(REDACTED_PLACEHOLDER.into())
        } else {
            value
        };
        self.attributes.insert(name.to_string(), final_value);
    }
}

impl Visit for JsonFieldVisitor<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.record_field(field.name(), Value::String(value.into()));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.record_field(field.name(), Value::Number(value.into()));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.record_field(field.name(), Value::Number(value.into()));
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        let num = serde_json::Number::from_f64(value).map_or(Value::Null, Value::Number);
        self.record_field(field.name(), num);
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.record_field(field.name(), Value::Bool(value));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.record_field(field.name(), Value::String(format!("{value:?}")));
    }
}
