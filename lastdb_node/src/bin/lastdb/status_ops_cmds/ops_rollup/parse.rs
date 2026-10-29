pub(crate) fn field_str(fields: &serde_json::Value, name: &str) -> Option<String> {
    fields.get(name).and_then(|v| match v {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Null => None,
        other => other.as_str().map(str::to_string),
    })
}

pub(crate) fn field_u64(fields: &serde_json::Value, name: &str) -> Option<u64> {
    fields.get(name).and_then(serde_json::Value::as_u64)
}

pub(crate) fn op_kind_from_str(raw: &str) -> Option<lastdb_node::request_telemetry::OpKind> {
    match raw {
        "query" => Some(lastdb_node::request_telemetry::OpKind::Query),
        "query_batch" => Some(lastdb_node::request_telemetry::OpKind::QueryBatch),
        "mutation" => Some(lastdb_node::request_telemetry::OpKind::Mutation),
        "mutation_batch" => Some(lastdb_node::request_telemetry::OpKind::MutationBatch),
        "status" => Some(lastdb_node::request_telemetry::OpKind::Status),
        "schema" => Some(lastdb_node::request_telemetry::OpKind::Schema),
        "search" => Some(lastdb_node::request_telemetry::OpKind::Search),
        "history" => Some(lastdb_node::request_telemetry::OpKind::History),
        "atom" => Some(lastdb_node::request_telemetry::OpKind::Atom),
        "deliver" => Some(lastdb_node::request_telemetry::OpKind::Deliver),
        "local_watch" => Some(lastdb_node::request_telemetry::OpKind::LocalWatch),
        "file_blob" => Some(lastdb_node::request_telemetry::OpKind::FileBlob),
        "other" => Some(lastdb_node::request_telemetry::OpKind::Other),
        _ => None,
    }
}

pub(crate) fn parse_since_duration_ms(raw: &str) -> Result<u64, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("since must not be empty".into());
    }
    let (digits, unit) = trimmed.split_at(trimmed.len().saturating_sub(1));
    let value = digits
        .parse::<u64>()
        .map_err(|_| format!("invalid since duration '{raw}'"))?;
    let seconds = match unit {
        "s" => value,
        "m" => value.saturating_mul(60),
        "h" => value.saturating_mul(60 * 60),
        "d" => value.saturating_mul(24 * 60 * 60),
        _ => return Err(format!("invalid since duration unit in '{raw}'")),
    };
    seconds
        .checked_mul(1000)
        .ok_or_else(|| format!("since duration '{raw}' is too large"))
}
