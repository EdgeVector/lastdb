//! `lastdb mutate`: request body construction and the command.

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

pub(super) fn mutate_request_body(
    schema: &str,
    wire_type: &str,
    key_hash: Option<&str>,
    key_range: Option<&str>,
    key_range_prefix: Option<&str>,
    must_exist: bool,
    durable: bool,
) -> Result<serde_json::Value, String> {
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
    if let Some(prefix) = key_range_prefix {
        body["key_range_prefix"] = serde_json::Value::String(prefix.to_string());
    }
    Ok(body)
}

pub(super) fn mutate_client_timeout(durable: bool) -> Duration {
    if durable {
        durable_mutation_client_timeout()
    } else {
        Duration::from_secs(10)
    }
}

/// Write the exact JSON body of the node's mutation receipt.
pub(super) fn write_mutate_receipt<W: Write>(output: &mut W, response: &str) -> Result<(), String> {
    parse_json_response(response, "mutation")?;
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
        .map_err(|e| format!("flush mutation receipt: {e}"))
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
    )?;
    let raw = post_json_with_timeout(
        &socket,
        "/api/mutation",
        &body,
        mutate_client_timeout(options.durable),
    )?;
    let mut stdout = std::io::stdout().lock();
    write_mutate_receipt(&mut stdout, &raw)
}
