use super::*;
use sha2::{Digest, Sha256};
use std::os::unix::fs::OpenOptionsExt;

/// Record-level data commands: mutate, get-keys, get.
pub(super) fn record_command(target: &DatabaseTarget, command: Command) -> Result<(), String> {
    match command {
        Command::Mutate {
            request_file,
            durable,
            mutation_id,
        } => mutate_command(target, &request_file, durable, mutation_id),
        Command::GetKeys(args) => list_record_keys(target, &args),
        Command::Get {
            schema,
            key_hash,
            key_range,
            json,
            hydrate,
            hydrate_inline,
        } => get_record(
            target,
            &schema,
            key_hash.as_deref(),
            key_range.as_deref(),
            json,
            hydrate.as_deref(),
            hydrate_inline.as_deref(),
        ),
        _ => unreachable!("run() routes only record commands here"),
    }
}

pub(super) fn legal_query_shapes(
    schema_type: &str,
    hash_field: Option<&str>,
    range_field: Option<&str>,
) -> Vec<String> {
    let hash = hash_field.unwrap_or("HASH");
    let range = range_field.unwrap_or("RANGE");
    match schema_type {
        "HashRange" => vec![
            format!(
                "point get O(1): lastdb get <schema> --key-hash <{hash}> --key-range <{range}>"
            ),
            format!("range under one hash O(log M): lastdb get-keys <schema> --key-hash <{hash}>"),
            "scan does not exist".to_string(),
        ],
        "Hash" => vec![
            format!("point get O(1): lastdb get <schema> --key-hash <{hash}>"),
            "keys page (not a census): lastdb get-keys <schema>".to_string(),
            "scan does not exist".to_string(),
        ],
        "Range" => vec![
            format!("point get O(1): lastdb get <schema> --key-hash <{range}>"),
            "keys page (not a census): lastdb get-keys <schema>".to_string(),
            "scan does not exist".to_string(),
        ],
        _ => vec![
            "point get: lastdb get <schema> --key-hash <KEY>".to_string(),
            "keys page (not a census): lastdb get-keys <schema>".to_string(),
            "scan does not exist".to_string(),
        ],
    }
}

pub(super) fn schema_is_hash_range(schema: &serde_json::Value) -> bool {
    let typed = schema
        .get("schema_type")
        .and_then(serde_json::Value::as_str)
        == Some("HashRange");
    let both_key_halves = schema
        .pointer("/key/range_field")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|s| !s.is_empty())
        && schema
            .pointer("/key/hash_field")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|s| !s.is_empty());
    typed || both_key_halves
}

pub(super) fn get_query_body(
    schema: &str,
    key_hash: &str,
    key_range: Option<&str>,
) -> serde_json::Value {
    let filter = match key_range.filter(|s| !s.is_empty()) {
        Some(range) => serde_json::json!({
            "HashRangeKey": { "hash": key_hash, "range": range }
        }),
        None => serde_json::json!({ "HashKey": key_hash }),
    };
    serde_json::json!({
        "schema_name": schema,
        "filter": filter,
    })
}

pub(super) fn get_record(
    target: &DatabaseTarget,
    schema_name: &str,
    key_hash: Option<&str>,
    key_range: Option<&str>,
    json_only: bool,
    hydrate: Option<&Path>,
    hydrate_inline: Option<&Path>,
) -> Result<(), String> {
    let Some(hash) = key_hash.map(str::trim).filter(|s| !s.is_empty()) else {
        return Err("get needs --key-hash (and --key-range when the schema is HashRange)".into());
    };
    let schema = fetch_schema(target, schema_name)?;
    if schema_is_hash_range(&schema) && key_range.map(str::trim).filter(|s| !s.is_empty()).is_none()
    {
        return Err("this schema is HashRange; pass --key-range or use get-keys --key-hash".into());
    }
    let body = get_query_body(schema_name, hash, key_range);
    let response = post_json_with_timeout(
        &target.socket,
        &target.path("/api/query"),
        &body,
        Duration::from_secs(lastdb_node::exec::DEFAULT_CLI_UDS_TIMEOUT_SECS),
    )?;
    let mut value = parse_json_response(&response, "query")?;
    if let Some(out) = hydrate {
        let file_required = schema["file_required"].as_bool().unwrap_or(false);
        hydrate_record_file(
            target,
            schema_name,
            &mut value,
            out,
            file_required,
            Placement::Blob,
        )?;
    }
    if let Some(out) = hydrate_inline {
        hydrate_record_file(
            target,
            schema_name,
            &mut value,
            out,
            false,
            Placement::Inline,
        )?;
    }
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&value).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }
    print_get_human(&value);
    Ok(())
}

pub(super) fn print_get_human(value: &serde_json::Value) {
    let results = value
        .get("results")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    if results.is_empty() {
        eprintln!("lastdb get: no row");
        return;
    }
    for (i, row) in results.iter().enumerate() {
        if results.len() > 1 {
            eprintln!("--- row {} ---", i + 1);
        }
        if let Some(key) = row.get("key") {
            match key {
                serde_json::Value::String(s) => println!("key {s}"),
                serde_json::Value::Object(map) => {
                    let hash = map
                        .get("hash")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("");
                    let range = map
                        .get("range")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("");
                    if range.is_empty() {
                        println!("key {hash}");
                    } else {
                        println!("key {hash}\t{range}");
                    }
                }
                _ => {}
            }
        }
        if let Some(fields) = row.get("fields").and_then(serde_json::Value::as_object) {
            let mut names: Vec<&String> = fields.keys().collect();
            names.sort();
            for name in names {
                let rendered = match fields.get(name) {
                    Some(serde_json::Value::String(s)) => s.clone(),
                    Some(other) => other.to_string(),
                    None => String::new(),
                };
                println!("{name} {rendered}");
            }
        }
        if let Some(path) = row.get("hydrated_file").and_then(serde_json::Value::as_str) {
            println!("file {path}");
        } else if let Some(file) = row.pointer("/metadata/file") {
            println!("file {file}");
        }
        if let Some(path) = row
            .get("hydrated_inline_file")
            .and_then(serde_json::Value::as_str)
        {
            println!("inline_file {path}");
        } else if let Some(file) = row.pointer("/metadata/inline_file") {
            println!("inline_file {file}");
        }
    }
}

pub(super) fn get_path(socket: &Path, path: &str, timeout: Duration) -> Result<String, String> {
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: localhost\r\n{}Connection: close\r\n\r\n",
        client_headers()
    );
    request_with_timeout(socket, req.as_bytes(), timeout)
}

#[derive(Clone, Copy)]
enum Placement {
    Blob,
    Inline,
}

impl Placement {
    fn marker(self) -> &'static str {
        match self {
            Self::Blob => "/metadata/file",
            Self::Inline => "/metadata/inline_file",
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Blob => "blob",
            Self::Inline => "inline",
        }
    }

    fn output_field(self) -> &'static str {
        match self {
            Self::Blob => "hydrated_file",
            Self::Inline => "hydrated_inline_file",
        }
    }
}

fn hydrate_record_file(
    target: &DatabaseTarget,
    schema: &str,
    value: &mut serde_json::Value,
    out: &Path,
    file_required: bool,
    placement: Placement,
) -> Result<(), String> {
    let records = value["results"]
        .as_array_mut()
        .ok_or("missing query records")?;
    let [record] = records.as_mut_slice() else {
        return Err("hydration requires exactly one record".into());
    };
    let Some(marker) = record
        .pointer(placement.marker())
        .filter(|value| !value.is_null())
    else {
        return match placement {
            Placement::Blob if !file_required => Ok(()),
            Placement::Blob => Err("record is missing its required file".into()),
            Placement::Inline => Err("record has no inline file".into()),
        };
    };
    let size = marker["size_bytes"]
        .as_u64()
        .ok_or("file marker has no size")?;
    let digest = marker["sha256"]
        .as_str()
        .ok_or("file marker has no digest")?;
    let revision = record["revision"]
        .as_str()
        .ok_or("record has no revision")?;
    let body = serde_json::to_vec(&serde_json::json!({
        "schema": schema, "key": record["key"], "expected_revision": revision,
        "placement": placement.name(),
    }))
    .map_err(|error| format!("encode hydrate request: {error}"))?;
    let path = target.path("/api/db/fetch-blob");
    let mut request = format!(
        "POST {path} HTTP/1.1\r\nHost: localhost\r\n{}Content-Type: application/json\r\nAccept: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        client_headers(),
        body.len(),
    )
    .into_bytes();
    request.extend_from_slice(&body);
    let timeout = admin_scan_client_timeout();
    let response = request_with_timeout_raw_bytes(&target.socket, &request, timeout)
        .map_err(|(op, error, _)| format_socket_io_error(op, &error, timeout))?;
    let bytes = parse_binary_response(&response, "record hydration")?;
    if bytes.len() as u64 != size || hex::encode(Sha256::digest(&bytes)) != digest {
        return Err("hydrated file differs from the queried record version".into());
    }
    publish_hydrated(out, &bytes)?;
    record[placement.output_field()] = serde_json::json!(out);
    Ok(())
}

fn publish_hydrated(out: &Path, bytes: &[u8]) -> Result<(), String> {
    let name = format!(".lastdb-hydrate-{}", uuid::Uuid::new_v4());
    let temp = out.with_file_name(name);
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&temp)
        .map_err(|error| format!("create hydration output: {error}"))?;
    let result = file
        .write_all(bytes)
        .and_then(|()| std::fs::hard_link(&temp, out));
    let cleanup = std::fs::remove_file(&temp);
    result.map_err(|error| format!("publish hydration output: {error}"))?;
    cleanup.map_err(|error| format!("clean up hydration output: {error}"))
}
