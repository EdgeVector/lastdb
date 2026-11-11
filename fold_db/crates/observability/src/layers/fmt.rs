//! FMT layer — JSON formatter with format-time PII redaction.
//!
//! Defense-in-depth alongside the [`crate::redact!`] / [`crate::redact_id!`]
//! macros: even if a call site forgets to wrap a sensitive value, this
//! formatter still scrubs the value at write time when the field name
//! matches the deny-list.
//!
//! Output shape follows the OpenTelemetry log data model — one JSON object
//! per line with `time_unix_nano`, `severity_text`, `severity_number`,
//! `body`, `target`, optional `span`, and an `attributes` object holding
//! the event's structured fields.

use std::collections::HashSet;
use std::fmt;
use std::io;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Map, Value};
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_appender::non_blocking::{NonBlocking, WorkerGuard};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::registry::LookupSpan;

/// Holds the [`tracing_appender`] worker thread alive so its background
/// flush keeps draining the queue.
#[must_use = "FmtGuard must be held for the lifetime of the binary or log lines may be dropped"]
pub struct FmtGuard {
    _worker: WorkerGuard,
}

/// Wrap process stdout in [`tracing_appender::non_blocking`].
pub(crate) fn build_stdout_writer() -> (NonBlocking, FmtGuard) {
    let (non_blocking, worker) = tracing_appender::non_blocking(io::stdout());
    (non_blocking, FmtGuard { _worker: worker })
}

// ---------------------------------------------------------------------------
// Deny-list — field names whose values are scrubbed at format time.
// ---------------------------------------------------------------------------

/// Compile-time deny-list. The dotted variants (`auth.token`, `api.key`)
/// match the canonical attribute style from [`crate::attrs`]; the
/// underscore variants match common ad-hoc field names.
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

#[derive(Clone, Debug)]
pub(crate) struct DenyList {
    set: HashSet<String>,
}

impl DenyList {
    /// Static list plus comma-separated names from the `OBS_REDACT_EXTRA`
    /// env var. Reads the env var fresh on every call — the layer
    /// constructor calls this once at startup, so the snapshot is taken
    /// when the binary boots.
    pub(crate) fn from_env() -> Self {
        let raw = std::env::var(OBS_REDACT_EXTRA_ENV).unwrap_or_default();
        let extras: Vec<&str> = raw
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect();
        Self::with_extras(&extras)
    }

    pub(crate) fn with_extras(extras: &[&str]) -> Self {
        let mut set: HashSet<String> = STATIC_DENY_LIST.iter().map(|s| (*s).to_string()).collect();
        for extra in extras {
            set.insert((*extra).to_string());
        }
        Self { set }
    }

    pub(crate) fn contains(&self, name: &str) -> bool {
        self.set.contains(name)
    }
}

// ---------------------------------------------------------------------------
// RedactingFormat — custom FormatEvent impl
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct RedactingFormat {
    deny_list: DenyList,
}

impl RedactingFormat {
    pub(crate) fn from_env() -> Self {
        Self {
            deny_list: DenyList::from_env(),
        }
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

        let mut visitor = JsonFieldVisitor::new(&self.deny_list);
        event.record(&mut visitor);
        let JsonFieldVisitor {
            body, attributes, ..
        } = visitor;

        let mut obj = Map::new();
        obj.insert(
            "time_unix_nano".into(),
            Value::String(now_unix_nanos().to_string()),
        );
        obj.insert("severity_text".into(), Value::String(level.to_string()));
        obj.insert(
            "severity_number".into(),
            Value::from(severity_number(level)),
        );
        obj.insert("body".into(), Value::String(body.unwrap_or_default()));
        obj.insert(
            "target".into(),
            Value::String(metadata.target().to_string()),
        );
        if let Some(span) = ctx.lookup_current() {
            obj.insert("span".into(), Value::String(span.name().to_string()));
        }
        if !attributes.is_empty() {
            obj.insert("attributes".into(), Value::Object(attributes));
        }

        let line = serde_json::to_string(&Value::Object(obj)).map_err(|_| fmt::Error)?;
        writeln!(writer, "{line}")
    }
}

fn severity_number(level: Level) -> u32 {
    // Map tracing levels to the OTel SeverityNumber enum.
    match level {
        Level::TRACE => 1,
        Level::DEBUG => 5,
        Level::INFO => 9,
        Level::WARN => 13,
        Level::ERROR => 17,
    }
}

fn now_unix_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos())
}

// ---------------------------------------------------------------------------
// Visitor — walks event fields, applying the deny-list per field name.
// ---------------------------------------------------------------------------

struct JsonFieldVisitor<'a> {
    deny_list: &'a DenyList,
    body: Option<String>,
    attributes: Map<String, Value>,
}

impl<'a> JsonFieldVisitor<'a> {
    fn new(deny_list: &'a DenyList) -> Self {
        Self {
            deny_list,
            body: None,
            attributes: Map::new(),
        }
    }

    fn record_field(&mut self, name: &str, value: Value) {
        // tracing routes the macro's bare-string message to the special
        // field named `message`; promote it to the OTel `body` slot.
        //
        // NOTE — this return is *before* the deny-list check below, and that is
        // not an oversight that can be fixed here: by the time the layer sees
        // it, everything a macro interpolated into its format string has already
        // been rendered into this one opaque string. There is no field name left
        // to match on, and scrubbing the body wholesale would blank every log
        // line in the process.
        //
        // The consequence is worth stating plainly, because it is the opposite
        // of what "we have a redaction layer" suggests: **the deny-list protects
        // structured fields only.** A secret passed as its own named field is
        // redacted; the same secret interpolated into the format string is not,
        // and neither is anything reachable through a `{:?}` on a struct that
        // happens to hold user data. The CI lint
        // (`scripts/lints/lint-redaction.sh`) has the same shape and the same
        // blind spot — it matches a `name = value` pair, so it cannot see a
        // positional argument either.
        //
        // So the call site is the only place this can be got right: redact into
        // the format argument (`redact!` / `redact_id!`, or a purpose-built
        // log-safe view like `HashRangeFilter::redacted`). Pinned by
        // `message_body_is_not_scrubbed_by_the_deny_list`.
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

impl<'a> Visit for JsonFieldVisitor<'a> {
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
