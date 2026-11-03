use super::*;

/// The JSON that names one record: `{ schema, key: { hash, range } }`.
pub(crate) fn blob_target_json(target: &BlobTargetArgs) -> serde_json::Value {
    serde_json::json!({
        "schema": target.schema,
        "key": { "hash": target.hash, "range": target.range },
    })
}

/// `lastdb db fetch-blob`: the verified bytes of one record's file.
pub(crate) fn db_fetch_blob(
    socket: &Path,
    target: &BlobTargetArgs,
    out: Option<PathBuf>,
    json_only: bool,
) -> Result<(), String> {
    let body = blob_target_json(target);
    if json_only {
        let value = db_post_json(socket, "/api/db/fetch-blob", &body)?;
        println!(
            "{}",
            serde_json::to_string_pretty(&value).map_err(|e| format!("serialize: {e}"))?
        );
        return Ok(());
    }
    let body = serde_json::to_vec(&body).map_err(|e| format!("serialize fetch body: {e}"))?;
    let request = format!(
        "POST /api/db/fetch-blob HTTP/1.1\r\n\
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
    let response = request_with_timeout_raw_bytes(socket, &request, admin_scan_client_timeout())
        .map_err(|(op, error, timeout)| format_socket_io_error(op, &error, timeout))?;
    let bytes = parse_binary_response(&response, "/api/db/fetch-blob")?;
    write_blob_bytes(out, &bytes)
}

/// `lastdb db delete-blob`: remove one record's file and its marker.
pub(crate) fn db_delete_blob(socket: &Path, target: &BlobTargetArgs) -> Result<(), String> {
    let value = db_post_json(socket, "/api/db/delete-blob", &blob_target_json(target))?;
    println!(
        "{}",
        serde_json::to_string_pretty(&value).map_err(|e| format!("serialize: {e}"))?
    );
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
