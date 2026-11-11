//! `lastdb mutate`: request body construction, file input, and submission.

use super::*;

const PUT_BLOB_PATH: &str = "/api/db/put-blob";

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordInput {
    mutation: serde_json::Value,
    file: Option<InputFile>,
    #[serde(default)]
    placement: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct InputFile {
    source: PathBuf,
    name: Option<String>,
    media_type: Option<String>,
    #[serde(default)]
    local: bool,
    thumbnail: Option<InputThumbnail>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct InputThumbnail {
    source: PathBuf,
    name: Option<String>,
    media_type: Option<String>,
}

fn thumbnail_metadata(
    base: &Path,
    thumbnail: Option<&InputThumbnail>,
) -> Result<serde_json::Value, String> {
    use base64::Engine as _;
    let Some(thumbnail) = thumbnail else {
        return Ok(serde_json::Value::Null);
    };
    let path = base.join(&thumbnail.source);
    let bytes =
        std::fs::read(&path).map_err(|error| format!("read {}: {error}", path.display()))?;
    Ok(serde_json::json!({
        "bytes_b64": base64::engine::general_purpose::STANDARD.encode(bytes),
        "name": thumbnail.name,
        "media_type": thumbnail.media_type,
    }))
}

fn placement(value: Option<&str>) -> Result<&'static str, String> {
    match value {
        None | Some("blob") => Ok("blob"),
        Some("inline") => Ok("inline"),
        Some(other) => Err(format!(
            "record input placement must be blob or inline, not {other}"
        )),
    }
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
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body)
        .ok_or_else(|| "mutation response had no body".to_string())?;
    if response.starts_with("HTTP/1.1 202 ") {
        let receipt: serde_json::Value =
            serde_json::from_str(body).map_err(|_| "invalid mutation submission receipt")?;
        if receipt["status"] != "submitted" || receipt["mutation_id"].as_str().is_none() {
            return Err("invalid mutation submission receipt".into());
        }
    } else {
        parse_json_response(response, "mutation")?;
    }
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
    target: &DatabaseTarget,
    request_file: &Path,
    durable: bool,
    mutation_id: Option<uuid::Uuid>,
) -> Result<(), String> {
    if mutation_id.is_some() && !target.is_explicit() {
        return Err("--mutation-id requires --db".into());
    }
    let text = if request_file == Path::new("-") {
        let mut text = String::new();
        std::io::Read::read_to_string(&mut std::io::stdin().lock(), &mut text)
            .map_err(|e| format!("read mutation: {e}"))?;
        text
    } else {
        std::fs::read_to_string(request_file).map_err(|e| format!("read mutation: {e}"))?
    };
    let mut body: serde_json::Value =
        serde_json::from_str(&text).map_err(|e| format!("invalid mutation JSON: {e}"))?;
    let file = if body.get("mutation").is_some() {
        if body.get("file").is_none() {
            return Err("record input requires an explicit file object or null".into());
        }
        let input: RecordInput = serde_json::from_value(body)
            .map_err(|error| format!("invalid record input: {error}"))?;
        body = input.mutation;
        Some((input.file, placement(input.placement.as_deref())?))
    } else {
        None
    };
    if file.is_some() && mutation_id.is_some() {
        return Err("--mutation-id applies to record submissions without file changes".into());
    }
    if let Some(object) = body.as_object_mut().filter(|_| durable) {
        object.insert("durability".into(), serde_json::json!("durable"));
    }
    lastdb_node::exec::parse_mutation_body(
        serde_json::to_vec(&body)
            .map_err(|e| e.to_string())?
            .as_slice(),
    )
    .map_err(|reject| format!("invalid complete mutation: {reject:?}"))?;
    let socket = &target.socket;
    let base = request_file.parent().unwrap_or_else(|| Path::new(""));
    let (route, body) = match file {
        Some((Some(file), placement)) => {
            if placement == "inline" && file.thumbnail.is_some() {
                return Err("an inline file input takes no thumbnail".into());
            }
            if file.local && placement == "inline" {
                return Err("a local file reference cannot be an inline file".into());
            }
            let source = base.join(&file.source);
            if file.local {
                let source = std::fs::canonicalize(&source)
                    .map_err(|error| format!("open {}: {error}", source.display()))?;
                let metadata = serde_json::json!({
                    "mutation": body,
                    "name": file.name,
                    "media_type": file.media_type,
                    "placement": placement,
                    "local_path": source,
                    "thumbnail": thumbnail_metadata(base, file.thumbnail.as_ref())?,
                });
                let raw = post_json_with_timeout(
                    socket,
                    &target.path("/api/db/put-blob"),
                    &metadata,
                    admin_scan_client_timeout(),
                )?;
                let receipt = parse_json_response(&raw, "/api/db/put-blob")?;
                println!("{receipt}");
                return Ok(());
            }
            let metadata = serde_json::json!({
                "mutation": body, "name": file.name, "media_type": file.media_type,
                "placement": placement,
                "thumbnail": thumbnail_metadata(base, file.thumbnail.as_ref())?,
            });
            let receipt = put_record_file(target, &metadata, &source)?;
            println!("{receipt}");
            return Ok(());
        }
        Some((None, placement)) => (
            "/api/db/delete-blob",
            serde_json::json!({ "mutation": body, "placement": placement }),
        ),
        None => ("/api/mutation", body),
    };
    send_mutation(target, route, &body, durable, mutation_id)
}

fn send_mutation(
    target: &DatabaseTarget,
    route: &str,
    body: &serde_json::Value,
    durable: bool,
    mutation_id: Option<uuid::Uuid>,
) -> Result<(), String> {
    let id = (target.is_explicit() && route == "/api/mutation")
        .then(|| mutation_id.unwrap_or_else(uuid::Uuid::new_v4));
    let describe_error = |error| match id {
        Some(id) => format!("{error} (mutation ID {id})"),
        None => error,
    };
    let raw = post_json_with_id(
        &target.socket,
        &target.path(route),
        body,
        mutate_client_timeout(durable),
        id,
    )
    .map_err(describe_error)?;
    let mut stdout = std::io::stdout().lock();
    write_mutate_receipt(&mut stdout, &raw).map_err(describe_error)
}

fn put_blob_request(
    path: &str,
    bytes: &[u8],
    metadata: &serde_json::Value,
) -> Result<Vec<u8>, String> {
    admit_local_file_input(bytes.len() as u64)?;
    let json = serde_json::to_vec(metadata).map_err(|e| format!("serialize file metadata: {e}"))?;
    let body_length = json
        .len()
        .checked_add(bytes.len())
        .ok_or("local file request length overflow")?;
    let head = format!(
        "POST {path} HTTP/1.1\r\n\
         Host: localhost\r\n\
         {}Content-Type: application/octet-stream\r\n\
         X-LastDB-File-Blob-Metadata-Length: {}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n",
        client_headers(),
        json.len(),
        body_length
    );
    let capacity = head
        .len()
        .checked_add(body_length)
        .ok_or_else(|| "local file request length overflow".to_string())?;
    admit_local_file_input(capacity as u64)?;
    let mut request = Vec::new();
    request
        .try_reserve_exact(capacity)
        .map_err(|_| "local file request allocation refused".to_string())?;
    request.extend_from_slice(head.as_bytes());
    request.extend_from_slice(&json);
    request.extend_from_slice(bytes);
    Ok(request)
}

fn admit_local_file_input(bytes: u64) -> Result<(), String> {
    if bytes > fold_db::storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES {
        return Err("local file input exceeds native pending memory allowance".into());
    }
    Ok(())
}

fn read_local_file_input(mut input: impl std::io::Read) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let count = input
            .read(&mut buffer)
            .map_err(|e| format!("read local file input: {e}"))?;
        if count == 0 {
            return Ok(bytes);
        }
        let length = bytes
            .len()
            .checked_add(count)
            .ok_or_else(|| "local file input length overflow".to_string())?;
        admit_local_file_input(length as u64)?;
        if length > bytes.capacity() {
            let maximum =
                fold_db::storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES as usize;
            let capacity = bytes.capacity().saturating_mul(2).max(length).min(maximum);
            bytes
                .try_reserve_exact(capacity - bytes.len())
                .map_err(|_| "local file input allocation refused".to_string())?;
        }
        bytes.extend_from_slice(&buffer[..count]);
    }
}

fn put_record_file(
    target: &DatabaseTarget,
    metadata: &serde_json::Value,
    source: &Path,
) -> Result<serde_json::Value, String> {
    let input = std::fs::File::open(source)
        .map_err(|error| format!("open {}: {error}", source.display()))?;
    admit_local_file_input(
        input
            .metadata()
            .map_err(|error| format!("file metadata: {error}"))?
            .len(),
    )?;
    let bytes = read_local_file_input(input)?;
    let request = put_blob_request(&target.path(PUT_BLOB_PATH), &bytes, metadata)?;
    drop(bytes);
    let response = request_with_timeout(&target.socket, &request, admin_scan_client_timeout())?;
    parse_json_response(&response, PUT_BLOB_PATH)
}
