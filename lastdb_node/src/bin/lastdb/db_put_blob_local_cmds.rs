//! `lastdb db put-blob-local`: store bytes in the node's own `cas_blobs` plane.

use super::*;

const PUT_BLOB_LOCAL_PATH: &str = "/api/db/put-blob-local";

/// Arguments of `lastdb db put-blob-local`.
#[derive(clap::Args, Debug)]
pub(crate) struct PutBlobLocalArgs {
    /// File with the bytes to store. Omit it to read stdin.
    #[arg(long)]
    pub(crate) file: Option<PathBuf>,
    /// Optional file name for the pointer. Leave it out to keep the pointer
    /// identical for identical bytes.
    #[arg(long)]
    pub(crate) name: Option<String>,
    /// Optional media type for the pointer. Leave it out for the same reason.
    #[arg(long)]
    pub(crate) media_type: Option<String>,
}

/// The raw HTTP request: the bytes are the body, so there is no base64 tax.
/// `name` and `media_type` ride as compact JSON in a header, only when given.
fn put_blob_local_request(
    bytes: &[u8],
    name: Option<&str>,
    media_type: Option<&str>,
) -> Result<Vec<u8>, String> {
    let mut metadata = serde_json::Map::new();
    if let Some(name) = name {
        metadata.insert("name".into(), name.into());
    }
    if let Some(media_type) = media_type {
        metadata.insert("media_type".into(), media_type.into());
    }
    let metadata_header = if metadata.is_empty() {
        String::new()
    } else {
        let json = serde_json::to_string(&metadata)
            .map_err(|e| format!("serialize blob metadata: {e}"))?;
        format!("X-LastDB-File-Blob-Metadata: {json}\r\n")
    };
    let head = format!(
        "POST {PUT_BLOB_LOCAL_PATH} HTTP/1.1\r\n\
         Host: localhost\r\n\
         {}Content-Type: application/octet-stream\r\n\
         {metadata_header}Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n",
        client_headers(),
        bytes.len()
    );
    let mut request = Vec::with_capacity(head.len() + bytes.len());
    request.extend_from_slice(head.as_bytes());
    request.extend_from_slice(bytes);
    Ok(request)
}

/// Send the bytes and return the daemon's `file_blob` report
/// (`pointer`, `blob_ref`, `file_hash`, `bytes`, `stored`).
pub(crate) fn put_blob_local(
    socket: &Path,
    bytes: &[u8],
    name: Option<&str>,
    media_type: Option<&str>,
) -> Result<serde_json::Value, String> {
    if bytes.len() > lastdb_uds::uds_http::MAX_BODY_LEN {
        return Err(format!(
            "{} bytes is over the {} byte request cap of the daemon socket",
            bytes.len(),
            lastdb_uds::uds_http::MAX_BODY_LEN
        ));
    }
    let request = put_blob_local_request(bytes, name, media_type)?;
    let response = request_with_timeout(socket, &request, admin_scan_client_timeout())?;
    parse_json_response(&response, PUT_BLOB_LOCAL_PATH)?
        .get("file_blob")
        .cloned()
        .ok_or_else(|| "response missing file_blob".to_string())
}

pub(crate) fn read_put_blob_local_input(file: Option<&Path>) -> Result<Vec<u8>, String> {
    use std::io::{IsTerminal, Read};

    if let Some(path) = file {
        return std::fs::read(path).map_err(|e| format!("read {}: {e}", path.display()));
    }
    let stdin = std::io::stdin();
    if stdin.is_terminal() {
        return Err("provide --file PATH, or pipe the bytes on stdin".to_string());
    }
    let mut bytes = Vec::new();
    stdin
        .lock()
        .read_to_end(&mut bytes)
        .map_err(|e| format!("read stdin: {e}"))?;
    Ok(bytes)
}

pub(crate) fn db_put_blob_local(socket: &Path, args: &PutBlobLocalArgs) -> Result<(), String> {
    let bytes = read_put_blob_local_input(args.file.as_deref())?;
    let report = put_blob_local(
        socket,
        &bytes,
        args.name.as_deref(),
        args.media_type.as_deref(),
    )?;
    let pointer = report
        .get("pointer")
        .ok_or_else(|| "file_blob response missing pointer".to_string())?;
    println!(
        "{}",
        serde_json::to_string_pretty(pointer).map_err(|e| format!("serialize pointer: {e}"))?
    );
    let text = |key: &str| {
        report
            .get(key)
            .and_then(serde_json::Value::as_str)
            .unwrap_or("?")
    };
    let verb = if report.get("stored") == Some(&serde_json::Value::Bool(false)) {
        "already stored"
    } else {
        "stored"
    };
    eprintln!("{verb} {} bytes as {}", bytes.len(), text("blob_ref"));
    Ok(())
}
