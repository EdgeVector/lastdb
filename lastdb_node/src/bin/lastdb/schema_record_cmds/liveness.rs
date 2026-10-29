use super::*;

pub(crate) fn liveness_explain_command(
    data_dir: Option<PathBuf>,
    (class, id, scope): (&str, &str, Option<&str>),
    json_only: bool,
) -> Result<(), String> {
    let (_, socket) = resolve_client_home_and_socket(data_dir)?;
    let response = post_json(
        &socket,
        "/api/storage/liveness/explain",
        &serde_json::json!({ "class": class, "id": id, "scope": scope }),
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

    if let Some(atoms) = report.get("atoms").and_then(serde_json::Value::as_array) {
        crate::liveness_atom_print::print_atom_liveness(atoms);
        return Ok(());
    }
    if let Some(blobs) = report.get("blobs").and_then(serde_json::Value::as_array) {
        let dwell = report
            .get("dwell_secs")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        crate::liveness_blob_print::print_blob_liveness(blobs, dwell);
        return Ok(());
    }
    if let Some(molecules) = report
        .get("molecules")
        .and_then(serde_json::Value::as_array)
    {
        crate::liveness_molecule_print::print_molecule_liveness(molecules);
        return Ok(());
    }
    let returned_class = report
        .get("class")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(class);
    let returned_id = report
        .get("id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(id);
    let complete = report
        .get("complete")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let reclaim_state = report
        .get("reclaim_state")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    println!("Liveness: {returned_class} {returned_id}");
    println!("  Complete: {complete}");
    println!("  Reclaim state: {reclaim_state}");
    match report
        .get("active_references")
        .and_then(serde_json::Value::as_u64)
    {
        Some(count) => println!("  Active references in this target: {count}"),
        None => println!("  Active references in this target: unknown"),
    }
    if let Some(edges) = report.get("edges").and_then(serde_json::Value::as_array) {
        println!("  Active edges in this target partition: {}", edges.len());
    }
    Ok(())
}
