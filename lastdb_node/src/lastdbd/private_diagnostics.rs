//! Fixed-field formatting for daemon stderr, separate from CLI product output.

use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::registry::LookupSpan;

pub(crate) struct PrivateFormat;

impl<S, N> FormatEvent<S, N> for PrivateFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        _context: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let metadata = event.metadata();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_secs());
        write!(
            writer,
            "timestamp={timestamp} severity={} category={}",
            metadata.level(),
            category(metadata.target())
        )?;
        let mut metrics = Metrics::default();
        if metadata.target() == "lastdbd" {
            event.record(&mut metrics);
        }
        for (name, value) in metrics.values {
            write!(writer, " {name}={value}")?;
        }
        writeln!(writer)
    }
}

fn category(target: &str) -> &'static str {
    match target {
        "lastdbd::session_ledger" => "session",
        "lastdbd::allocator" => "allocator",
        "lastdbd::shutdown" => "shutdown",
        "lastdb_node::cloud" => "cloud",
        "observability::crash" => "panic_report",
        _ => "daemon",
    }
}

#[derive(Default)]
struct Metrics {
    values: Vec<(&'static str, u64)>,
}

impl Visit for Metrics {
    fn record_u64(&mut self, field: &Field, value: u64) {
        if matches!(
            field.name(),
            "workers" | "queue_capacity" | "max_field_value_bytes" | "default" | "absolute_max"
        ) {
            self.values.push((field.name(), value));
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        if let Ok(value) = u64::try_from(value) {
            self.record_u64(field, value);
        }
    }

    // Messages, errors, span context, and arbitrary field values never pass.
    fn record_debug(&mut self, _field: &Field, _value: &dyn fmt::Debug) {}
}
