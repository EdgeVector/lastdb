//! OpenTelemetry trace ids on Sentry events.

use tracing::Subscriber;
use tracing_opentelemetry::OtelData;
use tracing_subscriber::registry::LookupSpan;

/// Copy the W3C `trace_id` / `span_id` of the event's span onto the Sentry
/// event as tags. A span without `OtelData` leaves the event untouched.
pub(super) fn tag_with_trace_context<'a, S>(
    mut sentry_event: sentry::protocol::Event<'a>,
    event: &tracing::Event<'_>,
    ctx: &tracing_subscriber::layer::Context<'_, S>,
) -> sentry::protocol::Event<'a>
where
    S: Subscriber + for<'b> LookupSpan<'b>,
{
    if let Some(span_ref) = ctx.event_span(event) {
        let exts = span_ref.extensions();
        if let Some(otel_data) = exts.get::<OtelData>() {
            if let Some(trace_id) = otel_data.builder.trace_id {
                sentry_event
                    .tags
                    .insert("trace_id".to_string(), format!("{trace_id:032x}"));
            }
            if let Some(span_id) = otel_data.builder.span_id {
                sentry_event
                    .tags
                    .insert("span_id".to_string(), format!("{span_id:016x}"));
            }
        }
    }
    sentry_event
}
