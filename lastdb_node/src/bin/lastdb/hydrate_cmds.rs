use super::*;
use sha2::{Digest, Sha256};
use std::os::unix::fs::OpenOptionsExt;

/// Which file of a record `hydrate` writes: the blob file or the inline file.
#[derive(Clone, Copy)]
pub(super) enum Placement {
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

/// Fetch one file of the queried record at the same revision and write it,
/// checked against the marker in the query reply, to a new path.
pub(super) fn hydrate(
    socket: &Path,
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
    let mut request = format!(
        "POST /api/db/fetch-blob HTTP/1.1\r\nHost: localhost\r\n{}Content-Type: application/json\r\nAccept: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        client_headers(), body.len(),
    ).into_bytes();
    request.extend_from_slice(&body);
    let timeout = admin_scan_client_timeout();
    let response = request_with_timeout_raw_bytes(socket, &request, timeout)
        .map_err(|(op, error, _)| format_socket_io_error(op, &error, timeout))?;
    let bytes = parse_binary_response(&response, "record hydration")?;
    if bytes.len() as u64 != size || hex::encode(Sha256::digest(&bytes)) != digest {
        return Err("hydrated file differs from the queried record version".into());
    }
    publish(out, &bytes)?;
    record[placement.output_field()] = serde_json::json!(out);
    Ok(())
}

fn publish(out: &Path, bytes: &[u8]) -> Result<(), String> {
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
