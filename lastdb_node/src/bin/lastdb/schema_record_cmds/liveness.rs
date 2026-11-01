use super::*;

/// `lastdb liveness explain blob <ref>`: the counter of one file blob and its
/// dwell queue state. The daemon serves the blob class only.
pub(crate) fn liveness_explain_command(
    data_dir: Option<PathBuf>,
    (class, id): (&str, &str),
    json_only: bool,
) -> Result<(), String> {
    let (_, socket) = resolve_client_home_and_socket(data_dir)?;
    let response = post_json(
        &socket,
        "/api/storage/liveness/explain",
        &serde_json::json!({ "class": class, "id": id }),
    )?;
    let value = parse_json_response(&response, "liveness explain")?;
    let report = value
        .get("liveness")
        .cloned()
        .ok_or_else(|| "response missing liveness field".to_string())?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|error| format!("serialize: {error}"))?
        );
        return Ok(());
    }
    let blobs = report
        .get("blobs")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "response missing liveness.blobs".to_string())?;
    let dwell = report
        .get("dwell_secs")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    crate::liveness_blob_print::print_blob_liveness(blobs, dwell);
    Ok(())
}
