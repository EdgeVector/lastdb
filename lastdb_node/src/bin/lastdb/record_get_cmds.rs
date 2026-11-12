use super::*;
use sha2::{Digest, Sha256};
use std::os::unix::fs::OpenOptionsExt;

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

fn schema_is_hash_range(schema: &serde_json::Value) -> bool {
    let has = |p| {
        schema
            .pointer(p)
            .and_then(serde_json::Value::as_str)
            .is_some_and(|s| !s.is_empty())
    };
    schema["schema_type"].as_str() == Some("HashRange")
        || (has("/key/range_field") && has("/key/hash_field"))
}

pub(crate) fn get_query_body(
    schema: &str,
    key_hash: &str,
    key_range: Option<&str>,
) -> serde_json::Value {
    let filter = if let Some(range) = key_range.filter(|s| !s.is_empty()) {
        serde_json::json!({ "HashRangeKey": { "hash": key_hash, "range": range } })
    } else {
        serde_json::json!({ "HashKey": key_hash })
    };
    serde_json::json!({ "schema_name": schema, "filter": filter })
}

fn get_record(
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
    let response = post_json_with_timeout(
        &target.socket,
        &target.path("/api/query"),
        &get_query_body(schema_name, hash, key_range),
        Duration::from_secs(lastdb_node::exec::DEFAULT_CLI_UDS_TIMEOUT_SECS),
    )?;
    let mut value = parse_json_response(&response, "query")?;
    if let Some(out) = hydrate {
        let req = schema["file_required"].as_bool().unwrap_or(false);
        hydrate_record_file(target, schema_name, &mut value, out, req, Placement::Blob)?;
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
    } else {
        print_get_human(&value);
    }
    Ok(())
}

fn print_get_human(value: &serde_json::Value) {
    let Some(results) = value["results"].as_array().filter(|r| !r.is_empty()) else {
        eprintln!("lastdb get: no row");
        return;
    };
    for (i, row) in results.iter().enumerate() {
        if results.len() > 1 {
            eprintln!("--- row {} ---", i + 1);
        }
        match row.get("key") {
            Some(serde_json::Value::String(s)) => println!("key {s}"),
            Some(serde_json::Value::Object(map)) => {
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
        if let Some(fields) = row["fields"].as_object() {
            let mut names: Vec<_> = fields.keys().collect();
            names.sort();
            for name in names {
                let val = &fields[name];
                println!(
                    "{name} {}",
                    val.as_str().map_or_else(|| val.to_string(), str::to_owned)
                );
            }
        }
        for (hydrated, meta_ptr, prefix) in [
            ("hydrated_file", "/metadata/file", "file"),
            (
                "hydrated_inline_file",
                "/metadata/inline_file",
                "inline_file",
            ),
        ] {
            if let Some(path) = row.get(hydrated).and_then(serde_json::Value::as_str) {
                println!("{prefix} {path}");
            } else if let Some(file) = row.pointer(meta_ptr) {
                println!("{prefix} {file}");
            }
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

fn hydrate_record_file(
    target: &DatabaseTarget,
    schema: &str,
    value: &mut serde_json::Value,
    out: &Path,
    file_required: bool,
    placement: Placement,
) -> Result<(), String> {
    let (marker_ptr, name, output_field) = match placement {
        Placement::Blob => ("/metadata/file", "blob", "hydrated_file"),
        Placement::Inline => ("/metadata/inline_file", "inline", "hydrated_inline_file"),
    };
    let records = value["results"]
        .as_array_mut()
        .ok_or("missing query records")?;
    let [record] = records.as_mut_slice() else {
        return Err("hydration requires exactly one record".into());
    };
    let Some(marker) = record.pointer(marker_ptr).filter(|v| !v.is_null()) else {
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
        "schema": schema, "key": record["key"], "expected_revision": revision, "placement": name,
    }))
    .map_err(|e| format!("encode hydrate request: {e}"))?;
    let mut req = format!(
        "POST {} HTTP/1.1\r\nHost: localhost\r\n{}Content-Type: application/json\r\nAccept: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        target.path("/api/db/fetch-blob"),
        client_headers(),
        body.len(),
    )
    .into_bytes();
    req.extend_from_slice(&body);
    let timeout = admin_scan_client_timeout();
    let response = request_with_timeout_raw_bytes(&target.socket, &req, timeout)
        .map_err(|(op, err, _)| format_socket_io_error(op, &err, timeout))?;
    let bytes = parse_binary_response(&response, "record hydration")?;
    if bytes.len() as u64 != size || hex::encode(Sha256::digest(&bytes)) != digest {
        return Err("hydrated file differs from the queried record version".into());
    }
    let temp = out.with_file_name(format!(".lastdb-hydrate-{}", uuid::Uuid::new_v4()));
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&temp)
        .map_err(|e| format!("create hydration output: {e}"))?;
    let result = file
        .write_all(&bytes)
        .and_then(|()| std::fs::hard_link(&temp, out));
    let cleanup = std::fs::remove_file(&temp);
    result.map_err(|e| format!("publish hydration output: {e}"))?;
    cleanup.map_err(|e| format!("clean up hydration output: {e}"))?;
    record[output_field] = serde_json::json!(out);
    Ok(())
}
