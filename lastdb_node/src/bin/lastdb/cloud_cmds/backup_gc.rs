//! Cloud backup GC command and its receipt validation. Moved verbatim from `cloud_cmds.rs`.

use super::*;

/// Accept or attach to a durable job. Every socket operation is short; the
/// optional wait polls one exact ID and never holds the original connection.
#[allow(clippy::too_many_arguments)]
pub(crate) fn cloud_backup_gc(
    home: &Path,
    execute: bool,
    json_only: bool,
    job_id: Option<&str>,
    status: bool,
    wait: bool,
    request_id: Option<&str>,
) -> Result<(), String> {
    let socket = home.join("data").join("folddb.sock");
    if !socket.exists() {
        return Err(format!(
            "daemon socket missing at {} — start lastdbd first",
            socket.display()
        ));
    }
    let request_id = request_id.map_or_else(|| uuid::Uuid::new_v4().to_string(), str::to_owned);
    let body = if let Some(id) = job_id {
        serde_json::json!({"job_id":id})
    } else if status {
        serde_json::json!({"status":true})
    } else {
        // Print before submission so a lost response still has a reattach key.
        eprintln!("GC request ID: {request_id}");
        serde_json::json!({"dry_run":!execute,"request_id":request_id})
    };
    let mut value = parse_backup_gc_response(&post_json(&socket, "/api/sync/backup-gc", &body)?)?;
    let mut expected_id = job_id
        .map(str::to_owned)
        .or_else(|| (!status).then(|| request_id.clone()));
    let terminal_result = loop {
        let receipt = validate_backup_gc_receipt(&value, expected_id.as_deref(), status && !wait)?;
        let Some((id, state)) = receipt else {
            break Ok(());
        };
        if !wait {
            break Ok(());
        }
        if state != "queued" && state != "active" {
            break backup_gc_wait_result(&id, &state);
        }
        expected_id = Some(id.clone());
        std::thread::sleep(Duration::from_secs(1));
        value = parse_backup_gc_response(&post_json(
            &socket,
            "/api/sync/backup-gc",
            &serde_json::json!({"job_id":id}),
        )?)?;
    };
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string())
        );
        return terminal_result;
    }
    let report = value
        .get("job")
        .or_else(|| value.pointer("/data/job"))
        .unwrap_or(&value);
    println!(
        "Backup orphan GC job: {}",
        serde_json::to_string_pretty(report).unwrap_or_else(|_| report.to_string())
    );
    terminal_result
}

/// Validate the versioned receipt before either polling or reporting success.
/// Status may explicitly report no job; an execution/wait may not.
// lint:fn-size-ok moved verbatim from its original module
pub(crate) fn validate_backup_gc_receipt(
    value: &serde_json::Value,
    expected_id: Option<&str>,
    allow_no_job: bool,
) -> Result<Option<(String, String)>, String> {
    let job = value
        .get("job")
        .or_else(|| value.pointer("/data/job"))
        .ok_or("GC response lacks job receipt")?;
    if job.is_null() && allow_no_job {
        return Ok(None);
    }
    // Receipt versions 1 and 2 share the fields below. Version 2 adds
    // optional typed fields; anything newer is refused by name.
    let version = match job.get("version").and_then(serde_json::Value::as_u64) {
        Some(version @ (1 | 2)) => version,
        Some(version) => {
            return Err(format!(
                "GC_RECEIPT_VERSION_UNSUPPORTED: receipt version {version}; this CLI accepts 1 to 2"
            ))
        }
        None => return Err("GC_RECEIPT_VERSION_UNSUPPORTED: receipt version missing".into()),
    };
    if version >= 2 {
        if job
            .get("stop_code")
            .is_some_and(|v| !v.is_null() && !v.is_string())
        {
            return Err("GC receipt has a non-string stop_code".into());
        }
        if job
            .get("dispositions")
            .is_some_and(|v| !v.is_null() && !v.is_object())
        {
            return Err("GC receipt has a malformed dispositions block".into());
        }
    }
    let id = job
        .get("job_id")
        .and_then(|v| v.as_str())
        .ok_or("GC receipt lacks job ID")?;
    uuid::Uuid::parse_str(id).map_err(|_| "GC receipt has invalid job ID")?;
    if expected_id.is_some_and(|expected| expected != id) {
        return Err("GC receipt refers to a different job".into());
    }
    if job
        .get("dry_run")
        .and_then(serde_json::Value::as_bool)
        .is_none()
    {
        return Err("GC receipt lacks dry_run mode".into());
    }
    for field in [
        "objects_reconciled",
        "delete_acknowledged",
        "acknowledged_bytes",
        "failed_before_dispatch",
        "failed_bytes",
        "unknown",
        "unknown_bytes",
    ] {
        if job.get(field).and_then(serde_json::Value::as_u64).is_none() {
            return Err(format!("GC receipt lacks valid {field}"));
        }
    }
    let state = job
        .get("state")
        .and_then(|v| v.as_str())
        .ok_or("GC receipt lacks state")?;
    if !matches!(
        state,
        "queued"
            | "active"
            | "completed"
            | "failed"
            | "partial_failure"
            | "superseded"
            | "interrupted"
    ) {
        return Err(format!("GC receipt has unknown state {state}"));
    }
    if state == "completed"
        && (job["unknown"] != 0
            || job["failed_before_dispatch"] != 0
            || job.get("pending").is_none_or(|v| !v.is_null())
            || !job.get("report").is_some_and(serde_json::Value::is_object))
    {
        return Err("GC completed receipt has failures, uncertainty, or no report".into());
    }
    if state == "completed" {
        let report = &job["report"];
        if report["failed"] != 0
            || report["superseded"] != false
            || report["deleted"] != job["delete_acknowledged"]
            || job["objects_reconciled"] != job["delete_acknowledged"]
            || report
                .get("orphans_selected")
                .and_then(serde_json::Value::as_u64)
                .is_none()
            || (job["dry_run"] == false && report["orphans_selected"] != job["objects_reconciled"])
        {
            return Err("GC completed receipt has inconsistent report counts".into());
        }
    }
    Ok(Some((id.into(), state.into())))
}

pub(crate) fn backup_gc_wait_result(id: &str, state: &str) -> Result<(), String> {
    if state == "completed" {
        Ok(())
    } else {
        Err(format!(
            "GC job {id} ended {state}; inspect its durable receipt; no automatic replay"
        ))
    }
}

pub(crate) fn parse_backup_gc_response(response: &str) -> Result<serde_json::Value, String> {
    if response.starts_with("HTTP/1.1 202 ") {
        let (_, body) = response
            .split_once("\r\n\r\n")
            .ok_or("GC acceptance response had no body")?;
        serde_json::from_str(body).map_err(|e| format!("invalid GC acceptance JSON: {e}"))
    } else {
        parse_json_response(response, "backup-gc")
    }
}
