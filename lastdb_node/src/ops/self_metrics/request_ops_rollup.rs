//! Request-ops rollup rows: the merge of the two ranked aggregate lists and
//! the row key that the interval delta in the sampler state uses.

use super::*;

pub(super) fn request_ops_rollup_aggregates(
    snapshot: &crate::request_telemetry::RequestTelemetrySnapshot,
) -> Vec<crate::request_telemetry::OpAggregate> {
    let mut by_key = HashMap::new();
    for aggregate in snapshot
        .top_by_total_ms
        .iter()
        .chain(snapshot.top_by_count.iter())
    {
        let key = format!(
            "{}\0{}\0{}",
            aggregate.client,
            aggregate.kind.as_str(),
            aggregate.schema.as_deref().unwrap_or("")
        );
        by_key.entry(key).or_insert_with(|| aggregate.clone());
    }
    let mut aggregates: Vec<_> = by_key.into_values().collect();
    aggregates.sort_by(|a, b| {
        b.sum_ms
            .cmp(&a.sum_ms)
            .then_with(|| b.count.cmp(&a.count))
            .then_with(|| b.max_ms.cmp(&a.max_ms))
    });
    aggregates
}

/// Key for the previous-snapshot map the interval delta is computed against.
///
/// Must agree with `OpAggregate::key` on what makes two rows the same row. It
/// does not merely to be tidy: two live aggregates that collide here would
/// both be delta'd against one stored previous value, so the interval each
/// one persists would be wrong. The route joined the live key, so it joins
/// this one.
pub(super) fn request_ops_rollup_key(aggregate: &crate::request_telemetry::OpAggregate) -> String {
    format!(
        "{}\0{}\0{}\0{}",
        aggregate.client,
        aggregate.kind.as_str(),
        aggregate.schema.as_deref().unwrap_or(""),
        aggregate.route.as_deref().unwrap_or("")
    )
}
