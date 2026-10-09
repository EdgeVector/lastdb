use super::*;

pub(crate) fn db_fetch_file_blob(
    socket: &Path,
    pointer_json: &Path,
    out: Option<PathBuf>,
    json_only: bool,
    raw: bool,
) -> Result<(), String> {
    let pointer_text = std::fs::read_to_string(pointer_json)
        .map_err(|e| format!("read pointer JSON {}: {e}", pointer_json.display()))?;
    let pointer: serde_json::Value = serde_json::from_str(&pointer_text)
        .map_err(|e| format!("parse pointer JSON {}: {e}", pointer_json.display()))?;
    let value = if raw {
        let body = serde_json::to_vec(&serde_json::json!({ "pointer": pointer }))
            .map_err(|e| format!("serialize fetch body: {e}"))?;
        let request = format!(
            "POST /api/db/fetch-file-blob HTTP/1.1\r\n\
             Host: localhost\r\n\
             {}Content-Type: application/json\r\n\
             Accept: application/octet-stream\r\n\
             Content-Length: {}\r\n\
             Connection: close\r\n\
             \r\n\
             ",
            client_headers(),
            body.len()
        );
        let mut request = request.into_bytes();
        request.extend_from_slice(&body);
        let response =
            request_with_timeout_raw_bytes(socket, &request, admin_scan_client_timeout())
                .map_err(|(op, error, timeout)| format_socket_io_error(op, &error, timeout))?;
        let bytes = parse_binary_response(&response, "/api/db/fetch-file-blob")?;
        write_blob_bytes(out, &bytes)?;
        return Ok(());
    } else {
        db_post_json(
            socket,
            "/api/db/fetch-file-blob",
            &serde_json::json!({ "pointer": pointer }),
        )?
    };
    let report = value
        .get("file_blob")
        .cloned()
        .ok_or_else(|| "response missing file_blob".to_string())?;

    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }

    let bytes_b64 = report
        .get("bytes_b64")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "file_blob response missing bytes_b64".to_string())?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(bytes_b64)
        .map_err(|e| format!("decode file bytes: {e}"))?;

    if let Some(path) = out {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| format!("create out dir {}: {e}", parent.display()))?;
            }
        }
        std::fs::write(&path, &bytes).map_err(|e| format!("write {}: {e}", path.display()))?;
        let blob_ref = report
            .get("blob_ref")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("?");
        eprintln!(
            "Wrote {} bytes for {} to {}",
            bytes.len(),
            blob_ref,
            path.display()
        );
    } else {
        std::io::stdout()
            .write_all(&bytes)
            .map_err(|e| format!("write stdout: {e}"))?;
    }

    Ok(())
}

pub(crate) fn write_blob_bytes(out: Option<PathBuf>, bytes: &[u8]) -> Result<(), String> {
    if let Some(path) = out {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| format!("create out dir {}: {e}", parent.display()))?;
            }
        }
        std::fs::write(&path, bytes).map_err(|e| format!("write {}: {e}", path.display()))?;
        eprintln!("Wrote {} bytes to {}", bytes.len(), path.display());
    } else {
        std::io::stdout()
            .write_all(bytes)
            .map_err(|e| format!("write stdout: {e}"))?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
// lint:fn-size-ok moved verbatim from db_cmds.rs; splitting these functions is separate work.
pub(crate) fn db_put_file_blob(
    socket: &Path,
    schema: &str,
    field: &str,
    key_hash: &str,
    key_range: Option<&str>,
    bytes_path: &Path,
    mutation_type: &str,
    name: Option<&str>,
    media_type: Option<&str>,
    cache_local_plaintext: bool,
    additional_fields_json: Option<PathBuf>,
    pointer_out: Option<PathBuf>,
    raw: bool,
) -> Result<(), String> {
    let bytes = std::fs::read(bytes_path)
        .map_err(|e| format!("read file bytes {}: {e}", bytes_path.display()))?;
    let additional_fields = match additional_fields_json {
        Some(path) => {
            let raw = std::fs::read_to_string(&path)
                .map_err(|e| format!("read additional fields {}: {e}", path.display()))?;
            let value: serde_json::Value = serde_json::from_str(&raw)
                .map_err(|e| format!("parse additional fields {}: {e}", path.display()))?;
            if !value.is_object() {
                return Err(format!(
                    "additional fields {} must be a JSON object",
                    path.display()
                ));
            }
            value
        }
        None => serde_json::json!({}),
    };
    let metadata = serde_json::json!({
        "schema": schema,
        "field": field,
        "key": {
            "hash": key_hash,
            "range": key_range,
        },
        "mutation_type": mutation_type,
        "name": name,
        "media_type": media_type,
        "cache_local_plaintext": cache_local_plaintext,
        "additional_fields": additional_fields,
    });
    let value = if raw {
        let metadata_bytes = serde_json::to_vec(&metadata)
            .map_err(|e| format!("serialize file-blob metadata: {e}"))?;
        let body_len = bytes.len();
        let metadata_header = String::from_utf8(metadata_bytes)
            .map_err(|e| format!("file-blob metadata is not UTF-8: {e}"))?;
        let header = format!(
            "POST /api/db/file-blob HTTP/1.1\r\n\
             Host: localhost\r\n\
             {}Content-Type: application/octet-stream\r\n\
             X-LastDB-File-Blob-Metadata: {}\r\n\
             Content-Length: {}\r\n\
             Connection: close\r\n\
             \r\n\
             ",
            client_headers(),
            metadata_header,
            body_len
        );
        let mut request = header.into_bytes();
        request.extend_from_slice(&bytes);
        let response = request_with_timeout(socket, &request, admin_scan_client_timeout())?;
        parse_json_response(&response, "/api/db/file-blob")?
    } else {
        let body = serde_json::json!({
            "schema": schema,
            "field": field,
            "key": {
                "hash": key_hash,
                "range": key_range,
            },
            "bytes_b64": base64::engine::general_purpose::STANDARD.encode(bytes),
            "mutation_type": mutation_type,
            "name": name,
            "media_type": media_type,
            "cache_local_plaintext": cache_local_plaintext,
            "additional_fields": additional_fields,
        });
        db_post_json(socket, "/api/db/file-blob", &body)?
    };

    let report = value
        .get("file_blob")
        .cloned()
        .ok_or_else(|| "response missing file_blob".to_string())?;

    if let Some(path) = pointer_out {
        let pointer = report
            .get("pointer")
            .ok_or_else(|| "file_blob response missing pointer".to_string())?;
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| format!("create pointer dir {}: {e}", parent.display()))?;
            }
        }
        let pretty = serde_json::to_string_pretty(pointer)
            .map_err(|e| format!("serialize pointer JSON: {e}"))?;
        std::fs::write(&path, pretty).map_err(|e| format!("write {}: {e}", path.display()))?;
    }

    println!(
        "{}",
        serde_json::to_string_pretty(&report).map_err(|e| format!("serialize: {e}"))?
    );
    Ok(())
}

pub(crate) struct ForkFileBlobArgs {
    pub(crate) schema: String,
    pub(crate) field: String,
    pub(crate) key: Option<String>,
    pub(crate) key_json: Option<PathBuf>,
    pub(crate) pointer_json: PathBuf,
    pub(crate) name: Option<String>,
    pub(crate) media_type: Option<String>,
    pub(crate) cache_local_plaintext: bool,
    pub(crate) json: bool,
}

pub(crate) fn db_fork_file_blob(socket: &Path, args: ForkFileBlobArgs) -> Result<(), String> {
    let pointer = read_json_file(&args.pointer_json, "pointer JSON")?;
    let key = match (args.key, args.key_json) {
        (Some(hash), None) => serde_json::json!({ "hash": hash, "range": null }),
        (None, Some(path)) => read_json_file(&path, "key JSON")?,
        (None, None) => {
            return Err(
                "provide --key for hash-only schemas or --key-json for full KeyValue".into(),
            )
        }
        (Some(_), Some(_)) => return Err("provide only one of --key or --key-json".into()),
    };

    let mut body = serde_json::json!({
        "schema": args.schema,
        "field": args.field,
        "key": key,
        "pointer": pointer,
        "cache_local_plaintext": args.cache_local_plaintext,
    });
    if let Some(name) = args.name {
        body["name"] = serde_json::Value::String(name);
    }
    if let Some(media_type) = args.media_type {
        body["media_type"] = serde_json::Value::String(media_type);
    }

    let value = db_post_json(socket, "/api/db/fork-file-blob", &body)?;
    let report = value
        .get("file_blob")
        .cloned()
        .ok_or_else(|| "response missing file_blob".to_string())?;

    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }

    let source = report
        .get("source_blob_ref")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("?");
    let blob_ref = report
        .get("blob_ref")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("?");
    let bytes = report
        .get("bytes")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let mutations = report
        .get("mutation_ids")
        .and_then(serde_json::Value::as_array)
        .map_or(0, std::vec::Vec::len);
    println!(
        "Forked {source} into {blob_ref}; rewrote field with {bytes} bytes ({mutations} mutation id(s))"
    );
    Ok(())
}

pub(crate) fn read_json_file(path: &Path, label: &str) -> Result<serde_json::Value, String> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| format!("read {label} {}: {e}", path.display()))?;
    serde_json::from_str(&raw).map_err(|e| format!("parse {label} {}: {e}", path.display()))
}

pub(crate) fn db_gc_file_blobs(
    socket: &Path,
    execute: bool,
    json_only: bool,
) -> Result<(), String> {
    let value = db_post_json(
        socket,
        "/api/db/gc-file-blobs",
        &serde_json::json!({ "dry_run": !execute }),
    )?;
    let report = value
        .get("gc_file_blobs")
        .cloned()
        .ok_or_else(|| "response missing gc_file_blobs".to_string())?;
    if json_only {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }
    let mode = if execute { "EXECUTED" } else { "DRY RUN" };
    let count = |key: &str| {
        report
            .get(key)
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    };
    println!("Orphan file-blob GC — {mode}");
    println!("  atoms_scanned:            {}", count("atoms_scanned"));
    println!(
        "  blob_refs_referenced:     {}",
        count("blob_refs_referenced")
    );
    println!(
        "  file_blobs_scanned:       {}",
        count("file_blobs_scanned")
    );
    println!(
        "  file_blobs_referenced:    {}",
        count("file_blobs_referenced")
    );
    println!(
        "  file_blobs_deleted:       {}",
        count("file_blobs_deleted")
    );
    println!(
        "  file_blobs_skipped_recent: {}",
        count("file_blobs_skipped_recent")
    );
    println!(
        "  file_blobs_stamped:       {}",
        count("file_blobs_stamped")
    );
    println!(
        "  file_blobs_unreadable_retained: {}",
        count("file_blobs_unreadable_retained")
    );
    println!(
        "  bytes_freed_approx:       {}",
        format_bytes(count("bytes_freed_approx"))
    );
    if !execute {
        println!("  (dry run — pass --execute to delete; undated rows are stamped on execute)");
    }
    Ok(())
}
