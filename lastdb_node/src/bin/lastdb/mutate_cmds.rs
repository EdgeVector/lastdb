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
    thumbnail: Option<ThumbFile>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ThumbFile {
    source: PathBuf,
    name: Option<String>,
    media_type: Option<String>,
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

fn write_mutate_receipt<W: Write>(output: &mut W, response: &str) -> Result<(), String> {
    let (_, body) = response
        .split_once("\r\n\r\n")
        .ok_or("mutation response had no body")?;
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
        .and_then(|()| {
            if body.ends_with('\n') {
                Ok(())
            } else {
                output.write_all(b"\n")
            }
        })
        .and_then(|()| output.flush())
        .map_err(|e| format!("write mutation receipt: {e}"))
}

fn submit_input_file(
    target: &DatabaseTarget,
    base: &Path,
    body: &serde_json::Value,
    file: &InputFile,
    placement: &str,
) -> Result<(), String> {
    use base64::Engine as _;
    if placement == "inline" && file.thumbnail.is_some() {
        return Err("an inline file input takes no thumbnail".into());
    }
    if file.local && placement == "inline" {
        return Err("a local file reference cannot be an inline file".into());
    }
    let source = base.join(&file.source);
    let thumb = match &file.thumbnail {
        Some(t) => {
            let p = base.join(&t.source);
            let bytes = std::fs::read(&p).map_err(|e| format!("read {}: {e}", p.display()))?;
            serde_json::json!({
                "bytes_b64": base64::engine::general_purpose::STANDARD.encode(bytes),
                "name": t.name, "media_type": t.media_type,
            })
        }
        None => serde_json::Value::Null,
    };
    let mut metadata = serde_json::json!({
        "mutation": body, "name": file.name, "media_type": file.media_type,
        "placement": placement, "thumbnail": thumb,
    });
    let receipt = if file.local {
        metadata["local_path"] = serde_json::json!(std::fs::canonicalize(&source)
            .map_err(|e| format!("open {}: {e}", source.display()))?);
        let raw = post_json_with_timeout(
            &target.socket,
            &target.path(PUT_BLOB_PATH),
            &metadata,
            admin_scan_client_timeout(),
        )?;
        parse_json_response(&raw, PUT_BLOB_PATH)?
    } else {
        put_record_file(target, &metadata, &source)?
    };
    println!("{receipt}");
    Ok(())
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
        std::io::stdin()
            .lock()
            .read_to_string(&mut text)
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
        let input: RecordInput =
            serde_json::from_value(body).map_err(|e| format!("invalid record input: {e}"))?;
        body = input.mutation;
        Some((input.file, placement(input.placement.as_deref())?))
    } else {
        None
    };
    if file.is_some() && mutation_id.is_some() {
        return Err("--mutation-id applies to record submissions without file changes".into());
    }
    if let Some(obj) = body.as_object_mut().filter(|_| durable) {
        obj.insert("durability".into(), serde_json::json!("durable"));
    }
    lastdb_node::exec::parse_mutation_body(&serde_json::to_vec(&body).map_err(|e| e.to_string())?)
        .map_err(|reject| format!("invalid complete mutation: {reject:?}"))?;
    let base = request_file.parent().unwrap_or_else(|| Path::new(""));
    let (route, body) = match file {
        Some((Some(f), p)) => return submit_input_file(target, base, &body, &f, p),
        Some((None, p)) => (
            "/api/db/delete-blob",
            serde_json::json!({ "mutation": body, "placement": p }),
        ),
        None => ("/api/mutation", body),
    };
    let id = (target.is_explicit() && route == "/api/mutation")
        .then(|| mutation_id.unwrap_or_else(uuid::Uuid::new_v4));
    let with_id = |err: String| id.map_or_else(|| err, |id| format!("{err} (mutation ID {id})"));
    let timeout = if durable {
        durable_mutation_client_timeout()
    } else {
        Duration::from_secs(10)
    };
    let raw = post_json_with_id(&target.socket, &target.path(route), &body, timeout, id)
        .map_err(with_id)?;
    write_mutate_receipt(&mut std::io::stdout().lock(), &raw).map_err(with_id)
}

fn admit_local(bytes: u64) -> Result<(), String> {
    (bytes <= fold_db::storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES)
        .then_some(())
        .ok_or_else(|| "local file input exceeds native pending memory allowance".into())
}

fn put_record_file(
    target: &DatabaseTarget,
    metadata: &serde_json::Value,
    source: &Path,
) -> Result<serde_json::Value, String> {
    let max_bytes = fold_db::storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES;
    let input =
        std::fs::File::open(source).map_err(|e| format!("open {}: {e}", source.display()))?;
    let len = input
        .metadata()
        .map_err(|e| format!("file metadata: {e}"))?
        .len();
    admit_local(len)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(usize::try_from(len).map_err(|_| "local file input length overflow")?)
        .map_err(|_| "local file input allocation refused")?;
    input
        .take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|e| format!("read local file input: {e}"))?;
    admit_local(bytes.len() as u64)?;
    let json = serde_json::to_vec(metadata).map_err(|e| format!("serialize file metadata: {e}"))?;
    let body_len = json
        .len()
        .checked_add(bytes.len())
        .ok_or("local file request length overflow")?;
    let head = format!(
        "POST {} HTTP/1.1\r\nHost: localhost\r\n{}Content-Type: application/octet-stream\r\nX-LastDB-File-Blob-Metadata-Length: {}\r\nContent-Length: {body_len}\r\nConnection: close\r\n\r\n",
        target.path(PUT_BLOB_PATH),
        client_headers(),
        json.len(),
    );
    let capacity = head
        .len()
        .checked_add(body_len)
        .ok_or("local file request length overflow")?;
    admit_local(capacity as u64)?;
    let mut request = Vec::new();
    request
        .try_reserve_exact(capacity)
        .map_err(|_| "local file request allocation refused")?;
    request.extend_from_slice(head.as_bytes());
    request.extend_from_slice(&json);
    request.extend_from_slice(&bytes);
    drop(bytes);
    let response = request_with_timeout(&target.socket, &request, admin_scan_client_timeout())?;
    parse_json_response(&response, PUT_BLOB_PATH)
}
