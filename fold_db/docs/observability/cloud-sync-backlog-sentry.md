# Cloud Sync Backlog Sentry Signal

Cloud sync keeps individual R2 upload/download retry failures at WARN. A
single failed presigned URL round-trip is usually transient and should not open
a Sentry issue by itself.

The sync engine promotes an aggregate ERROR event when repeated transfer
failures for the same `sync_target` and `failure_class` coincide with a pending
queue crossing a backlog threshold. Thresholds are evaluated at 50%, 75%, 90%,
and imminent `max_pending` (95%). Successful sync clears the aggregation state.

The ERROR event target is `fold_db::sync::backlog` and includes only
incident-safe fields:

- `sync_target`
- `failure_class`
- `failure_count`
- `pending_count`
- `max_pending`
- `pending_threshold_percent`
- `oldest_pending_age_secs`
- `sync_concurrency`
- `chunk_size`
- `last_success_age_secs`

Presigned R2 URLs, query strings, authorization headers, API keys, signatures,
tokens, and similar request material are redacted before they can appear in the
ERROR payload or sync status error text. Use the fields above for incident
triage; do not add raw request URLs or headers to sync error events.
