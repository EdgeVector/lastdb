use super::*;

pub(crate) fn alert_check(data_dir: Option<PathBuf>, args: AlertCheckArgs) -> Result<(), String> {
    let home = lastdb_node::host::resolve_home(data_dir)?;
    let socket = lastdb_uds::uds::socket_path(&home.join("data"));
    let notification = if args.no_notify {
        lastdb_node::health_alert::Notification::Disabled
    } else if let Some(path) = args.notification_log {
        lastdb_node::health_alert::Notification::LogFile(path)
    } else {
        lastdb_node::health_alert::Notification::MacOs
    };
    let config = lastdb_node::health_alert::Config {
        socket,
        state_file: args
            .state_file
            .unwrap_or_else(|| lastdb_node::health_alert::default_state_file(&home)),
        failures_before_alert: args.failures_before_alert,
        cooldown: Duration::from_secs(args.cooldown_secs),
        notification,
        heartbeat_command: args.heartbeat_command,
        acknowledged_incident_file: args.acknowledged_incident_file,
        routine_name: args.routine_name,
        now: SystemTime::now(),
        slowest_request_warn_ms: if args.no_degradation_probe {
            None
        } else {
            Some(args.slowest_request_warn_ms)
        },
    };
    let outcome = lastdb_node::health_alert::run_once(&config)?;
    println!(
        "lastdbd-mini-health-alert: {} {}",
        outcome.heartbeat_level(),
        outcome.summary()
    );
    Ok(())
}

pub(crate) fn probe_status(
    socket: &Path,
    timeout: Duration,
) -> Result<lastdb_node::self_metrics::StatusSnapshot, String> {
    // Cheap health path (no forensic ring) — used by `lastdb status`.
    probe_status_with_target(socket, "/api/status", timeout)
}

/// Full request-ops forensics (recent ring + rankings). Used by `lastdb ops`.
pub(crate) fn probe_status_forensics(
    socket: &Path,
    timeout: Duration,
) -> Result<lastdb_node::self_metrics::StatusSnapshot, String> {
    probe_status_with_target(socket, "/api/status?recent=1", timeout)
}

pub(crate) fn probe_status_with_target(
    socket: &Path,
    target: &str,
    timeout: Duration,
) -> Result<lastdb_node::self_metrics::StatusSnapshot, String> {
    let req = format!(
        "GET {target} HTTP/1.1\r\nHost: localhost\r\n{}Connection: close\r\n\r\n",
        client_headers()
    );
    let response = request_readonly_with_retry(socket, req.as_bytes(), timeout)?;
    if !response.starts_with("HTTP/1.1 200 ") {
        let status = response.lines().next().unwrap_or("<empty response>");
        return Err(format!("status probe returned {status}"));
    }
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body)
        .ok_or_else(|| "status response had no body".to_string())?;
    let value: serde_json::Value =
        serde_json::from_str(body).map_err(|e| format!("invalid status JSON: {e}"))?;
    serde_json::from_value(
        value
            .get("status")
            .cloned()
            .unwrap_or(serde_json::Value::Null),
    )
    .map_err(|e| format!("invalid status payload: {e}"))
}

/// Best-effort runtime schema-name labels for the human ops tables.
///
/// Request telemetry records the catalog `name`, not `identity_hash`. Join on
/// that exact field and keep the status JSON unchanged. The default schema
/// list omits record counts and full per-field metadata, so this does not turn
/// an operator render into a data scan.
pub(crate) fn probe_schema_labels(
    socket: &Path,
    timeout: Duration,
) -> Result<lastdb_node::request_telemetry::SchemaLabels, String> {
    let req = format!(
        "GET /api/schemas?include_system=true HTTP/1.1\r\nHost: localhost\r\n{}Connection: close\r\n\r\n",
        client_headers()
    );
    let response = request_readonly_with_retry(socket, req.as_bytes(), timeout)?;
    if !response.starts_with("HTTP/1.1 200 ") {
        let status = response.lines().next().unwrap_or("<empty response>");
        return Err(format!("schema catalog returned {status}"));
    }
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body)
        .ok_or_else(|| "schema catalog response had no body".to_string())?;
    let value: serde_json::Value =
        serde_json::from_str(body).map_err(|e| format!("invalid schema catalog JSON: {e}"))?;
    let payload = value.get("data").unwrap_or(&value);
    let schemas = payload
        .get("schemas")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "schema catalog payload had no schemas array".to_string())?;
    let mut labels = lastdb_node::request_telemetry::SchemaLabels::new();
    for schema in schemas {
        let Some(name) = schema.get("name").and_then(serde_json::Value::as_str) else {
            continue;
        };
        let Some(label) = schema
            .get("descriptive_name")
            .and_then(serde_json::Value::as_str)
            .filter(|label| !label.trim().is_empty())
        else {
            continue;
        };
        labels.insert(name.to_string(), label.to_string());
    }
    Ok(labels)
}
