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
