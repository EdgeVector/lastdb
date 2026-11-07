//! `lastdb mutate`: request body construction and the command.

use super::*;

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordInput {
    mutation: serde_json::Value,
    file: Option<InputFile>,
    /// `blob` (the default) or `inline`: which file of the record `file`
    /// writes, or which file `file: null` removes.
    #[serde(default)]
    placement: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct InputFile {
    source: PathBuf,
    name: Option<String>,
    media_type: Option<String>,
    /// Keep the file at `source`. Backup seals it into the cloud file format.
    #[serde(default)]
    local: bool,
    /// A small file stored as the record's inline file next to a new blob.
    thumbnail: Option<InputThumbnail>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct InputThumbnail {
    source: PathBuf,
    name: Option<String>,
    media_type: Option<String>,
}

/// The `thumbnail` part of a put-blob request: the inline file bytes ride in
/// the JSON metadata, so they are read and encoded here.
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
    request_file: &std::path::Path,
    durable: bool,
) -> Result<(), String> {
    let text = if request_file == std::path::Path::new("-") {
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
    if let Some(object) = body.as_object_mut().filter(|_| durable) {
        object.insert("durability".into(), serde_json::json!("durable"));
    }
    lastdb_host::wire::parse_mutation_body(
        serde_json::to_vec(&body)
            .map_err(|e| e.to_string())?
            .as_slice(),
    )
    .map_err(|reject| format!("invalid complete mutation: {reject:?}"))?;
    let (_, socket) = resolve_client_home_and_socket(data_dir)?;
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
                    &socket,
                    "/api/db/put-blob",
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
            let receipt = put_record_file(&socket, &metadata, &source)?;
            println!("{receipt}");
            return Ok(());
        }
        Some((None, placement)) => (
            "/api/db/delete-blob",
            serde_json::json!({ "mutation": body, "placement": placement }),
        ),
        None => ("/api/mutation", body),
    };
    let raw = post_json_with_timeout(&socket, route, &body, mutate_client_timeout(durable))?;
    let mut stdout = std::io::stdout().lock();
    write_mutate_receipt(&mut stdout, &raw)
}
