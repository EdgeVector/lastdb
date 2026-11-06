//! `lastdb mutate`: request body construction and the command.

use super::*;

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordInput {
    mutation: serde_json::Value,
    file: InputFile,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct InputFile {
    source: PathBuf,
    name: Option<String>,
    media_type: Option<String>,
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
        let input: RecordInput = serde_json::from_value(body)
            .map_err(|error| format!("invalid record input: {error}"))?;
        body = input.mutation;
        Some(input.file)
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
    if let Some(file) = file {
        let source = request_file
            .parent()
            .unwrap_or_else(|| Path::new(""))
            .join(file.source);
        let metadata = serde_json::json!({
            "mutation": body, "name": file.name, "media_type": file.media_type,
        });
        let receipt = put_record_file(&socket, &metadata, Some(&source))?;
        println!("{receipt}");
        return Ok(());
    }
    let raw = post_json_with_timeout(
        &socket,
        "/api/mutation",
        &body,
        mutate_client_timeout(durable),
    )?;
    let mut stdout = std::io::stdout().lock();
    write_mutate_receipt(&mut stdout, &raw)
}
