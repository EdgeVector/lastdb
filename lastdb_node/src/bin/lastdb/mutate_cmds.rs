//! `lastdb mutate`: request body construction, cloud-receipt checks, and the command.

use super::*;

/// Map `lastdb mutate --type` to the wire verb + must_exist flag.
///
/// `purge` is a hidden alias of `delete --must-exist` and never a second
/// eraser. Create/update stay refused until a fields file exists.
pub(super) fn resolve_mutate_type(
    raw: &str,
    must_exist: bool,
) -> Result<(&'static str, bool, Option<&'static str>), String> {
    match raw.to_ascii_lowercase().as_str() {
        "delete" => Ok(("delete", must_exist, None)),
        "purge" => Ok((
            "delete",
            true,
            Some("purge is an alias of delete --must-exist"),
        )),
        "create" | "update" => Err(
            "lastdb mutate is delete-only until a fields file exists (refused: create/update)"
                .to_string(),
        ),
        other => Err(format!(
            "unknown --type {other}; only delete is implemented (purge is a hidden must-exist alias)"
        )),
    }
}

// The arguments mirror the stable CLI wire fields. Keep this helper explicit
// so tests can assert each field without hiding the request shape in a bag.
#[allow(clippy::too_many_arguments)]
pub(super) fn mutate_request_body(
    schema: &str,
    wire_type: &str,
    key_hash: Option<&str>,
    key_range: Option<&str>,
    key_range_prefix: Option<&str>,
    must_exist: bool,
    durable: bool,
    cloud_publication: Option<CloudPublicationArg>,
) -> Result<serde_json::Value, String> {
    if cloud_publication.is_some() && !durable {
        return Err("--cloud-publication requires --durable".to_string());
    }
    if cloud_publication.is_some() && must_exist {
        return Err(
            "--cloud-publication cannot be combined with --must-exist; exact retries must remain idempotent"
                .to_string(),
        );
    }
    if key_range_prefix.is_some() && key_range.is_some() {
        return Err("--key-range-prefix cannot be combined with --key-range".to_string());
    }
    if key_range_prefix.is_some() && key_hash.is_none() {
        return Err("--key-range-prefix requires --key-hash".to_string());
    }

    let mut body = serde_json::json!({
        "type": "mutation",
        "schema": schema,
        "fields_and_values": {},
        "key_value": {
            "hash": key_hash,
            "range": key_range,
        },
        "mutation_type": wire_type,
    });
    if must_exist {
        body["must_exist"] = serde_json::Value::Bool(true);
    }
    if durable {
        body["durability"] = serde_json::Value::String("durable".to_string());
    }
    if let Some(mode) = cloud_publication {
        body["cloud_publication"] = serde_json::Value::String(mode.as_str().to_string());
    }
    if let Some(prefix) = key_range_prefix {
        body["key_range_prefix"] = serde_json::Value::String(prefix.to_string());
    }
    Ok(body)
}

pub(super) fn mutate_client_timeout(
    durable: bool,
    cloud_publication: Option<CloudPublicationArg>,
) -> Duration {
    match (durable, cloud_publication) {
        (true, _) | (_, Some(CloudPublicationArg::Wait)) => {
            mutation_cloud_publication_client_timeout()
        }
        (false, None) => Duration::from_secs(10),
    }
}

pub(super) fn require_published_cloud_receipt(receipt: &serde_json::Value) -> Result<(), String> {
    let success = receipt.get("success").and_then(serde_json::Value::as_bool);
    let local_committed = receipt
        .get("local_committed")
        .and_then(serde_json::Value::as_bool);
    let durability = receipt
        .get("durability")
        .and_then(serde_json::Value::as_str);
    let mutation_id = receipt
        .get("mutation_id")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty());
    let capture = receipt.get("cloud_capture");
    let capture_state = capture
        .and_then(|value| value.get("state"))
        .and_then(serde_json::Value::as_str);
    let capture_durable = capture
        .and_then(|value| value.get("durable"))
        .and_then(serde_json::Value::as_bool);
    let capture_mutation_uuid = capture
        .and_then(|value| value.get("mutation_uuid"))
        .and_then(serde_json::Value::as_str);
    let capture_error_clear = capture
        .and_then(|value| value.get("error"))
        .is_some_and(serde_json::Value::is_null);
    let publication = receipt.get("cloud_publication");
    let state = publication
        .and_then(|value| value.get("state"))
        .and_then(serde_json::Value::as_str);
    let published = publication
        .and_then(|value| value.get("published"))
        .and_then(serde_json::Value::as_bool);
    let publication_mutation_uuid = publication
        .and_then(|value| value.get("mutation_uuid"))
        .and_then(serde_json::Value::as_str);
    let publication_error_clear = publication
        .and_then(|value| value.get("error"))
        .is_some_and(serde_json::Value::is_null);
    let targets = publication
        .and_then(|value| value.get("targets"))
        .and_then(serde_json::Value::as_array);
    let exact_targets = targets.is_some_and(|targets| {
        !targets.is_empty()
            && targets.iter().all(|target| {
                let target_id = target
                    .get("target_id")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|value| !value.is_empty());
                let writer_id = target
                    .get("writer_id")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|value| !value.is_empty());
                let frontier = target
                    .get("frontier")
                    .and_then(serde_json::Value::as_str)
                    .and_then(|value| value.parse::<u64>().ok())
                    .is_some_and(|value| value > 0);
                target_id && writer_id && frontier
            })
    });
    let ids_match = mutation_id.is_some()
        && capture_mutation_uuid == mutation_id
        && publication_mutation_uuid == mutation_id;

    if success == Some(true)
        && local_committed == Some(true)
        && durability == Some("durable")
        && capture_state == Some("durable")
        && capture_durable == Some(true)
        && capture_error_clear
        && state == Some("published")
        && published == Some(true)
        && publication_error_clear
        && ids_match
        && exact_targets
    {
        return Ok(());
    }

    Err(format!(
        "cloud publication wait did not complete (success={}, local_committed={}, durability={}, capture_state={}, capture_durable={}, capture_error_clear={}, state={}, published={}, publication_error_clear={}, ids_match={}, exact_targets={})",
        success.map_or("missing".to_string(), |value| value.to_string()),
        local_committed.map_or("missing".to_string(), |value| value.to_string()),
        durability.unwrap_or("missing"),
        capture_state.unwrap_or("missing"),
        capture_durable.map_or("missing".to_string(), |value| value.to_string()),
        capture_error_clear,
        state.unwrap_or("missing"),
        published.map_or("missing".to_string(), |value| value.to_string()),
        publication_error_clear,
        ids_match,
        exact_targets,
    ))
}

/// Write the exact JSON body before checking the publication outcome. A
/// locally committed but unpublished Delete must leave purge automation with
/// both the full receipt and a nonzero exit.
pub(super) fn write_mutate_receipt<W: Write>(
    output: &mut W,
    response: &str,
    wait_for_cloud_publication: bool,
) -> Result<(), String> {
    let receipt = parse_json_response(response, "mutation")?;
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body)
        .ok_or_else(|| "mutation response had no body".to_string())?;
    output
        .write_all(body.as_bytes())
        .map_err(|e| format!("write mutation receipt: {e}"))?;
    if !body.ends_with('\n') {
        output
            .write_all(b"\n")
            .map_err(|e| format!("write mutation receipt: {e}"))?;
    }
    output
        .flush()
        .map_err(|e| format!("flush mutation receipt: {e}"))?;

    if wait_for_cloud_publication {
        require_published_cloud_receipt(&receipt)?;
    }
    Ok(())
}

pub(super) fn mutate_command(
    data_dir: Option<PathBuf>,
    schema: &str,
    mutation_type: &str,
    key_hash: Option<&str>,
    key_range: Option<&str>,
    key_range_prefix: Option<&str>,
    options: MutateRequestOptions,
) -> Result<(), String> {
    let (wire_type, must_exist, alias_note) =
        resolve_mutate_type(mutation_type, options.must_exist)?;
    if let Some(note) = alias_note {
        eprintln!("{note}");
    }
    if key_hash.is_none() && key_range.is_none() {
        return Err("mutate delete needs --key-hash and/or --key-range".to_string());
    }
    let (_, socket) = resolve_client_home_and_socket(data_dir)?;
    if lastdb_node::health_alert::probe_health(&socket).is_err() {
        return Err(format!(
            "lastdbd not reachable at {} — start the daemon first",
            socket.display()
        ));
    }
    let body = mutate_request_body(
        schema,
        wire_type,
        key_hash,
        key_range,
        key_range_prefix,
        must_exist,
        options.durable,
        options.cloud_publication,
    )?;
    let raw = post_json_with_timeout(
        &socket,
        "/api/mutation",
        &body,
        mutate_client_timeout(options.durable, options.cloud_publication),
    )?;
    let mut stdout = std::io::stdout().lock();
    write_mutate_receipt(&mut stdout, &raw, options.cloud_publication.is_some())
}
