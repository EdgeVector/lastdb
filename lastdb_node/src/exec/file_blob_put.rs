//! `POST /api/db/put-blob`: store the file of one record with the record.
//!
//! One native write puts the sealed bytes at the record's file path, sets the
//! record's file marker and writes the optional fields. A file therefore never
//! exists without its record. A new file on a record replaces the old bytes
//! at the same path in the same write.
//!
//! Native format, pending memory and disk-space checks admit the stored row.
//! JSON requests retain the socket's general request-body allowance.

use super::*;
use fold_db::schema::types::{FileWrite, KeyValue};
use std::borrow::Cow;

/// The record and the description of the file. With the raw form it rides as
/// compact JSON in `X-LastDB-File-Blob-Metadata`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PutBlobMetadata {
    schema: String,
    key: KeyValue,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    media_type: Option<String>,
    /// Other fields of the record, written in the same native write.
    #[serde(default)]
    fields: HashMap<String, Value>,
}

/// JSON form of the request.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JsonBody<'a> {
    schema: String,
    key: KeyValue,
    /// Borrowed from the request body, so the base64 text is not copied.
    #[serde(borrow)]
    bytes_b64: Cow<'a, str>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    media_type: Option<String>,
    #[serde(default)]
    fields: HashMap<String, Value>,
}

/// Read the plaintext and the metadata from either request form.
///
/// - `Content-Type: application/octet-stream`: the body IS the plaintext,
///   borrowed without a copy. The metadata rides in
///   `X-LastDB-File-Blob-Metadata`.
/// - Otherwise a JSON body `{ schema, key, bytes_b64, name?, media_type?,
///   fields? }`.
fn parse_put_blob(req: &UdsRequest) -> Result<(Cow<'_, [u8]>, PutBlobMetadata), HostError> {
    if is_media_type(req.header("content-type"), "application/octet-stream") {
        let raw = req
            .header(FILE_BLOB_METADATA_HEADER)
            .ok_or_else(|| HostError::new(400, "a raw put-blob requires the metadata header"))?;
        let metadata = serde_json::from_str(raw)
            .map_err(|e| HostError::new(400, format!("invalid file blob metadata header: {e}")))?;
        return Ok((Cow::Borrowed(req.body.as_slice()), metadata));
    }
    if req.body.len() > lastdb_uds::uds_http::MAX_BODY_LEN {
        return Err(HostError::new(
            413,
            "JSON request exceeds the request body allowance",
        ));
    }
    let body: JsonBody<'_> = serde_json::from_slice(&req.body)
        .map_err(|e| HostError::new(400, format!("invalid /api/db/put-blob body: {e}")))?;
    let capacity = base64::decoded_len_estimate(body.bytes_b64.len());
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .map_err(|_| HostError::new(503, "local file decode allocation refused"))?;
    bytes.resize(capacity, 0);
    let decoded = base64::engine::general_purpose::STANDARD
        .decode_slice(body.bytes_b64.as_bytes(), &mut bytes)
        .map_err(|e| HostError::new(400, format!("bytes_b64 is invalid base64: {e}")))?;
    bytes.truncate(decoded);
    Ok((
        Cow::Owned(bytes),
        PutBlobMetadata {
            schema: body.schema,
            key: body.key,
            name: body.name,
            media_type: body.media_type,
            fields: body.fields,
        },
    ))
}

/// `POST /api/db/put-blob` — store the file of one record. OWNER socket only.
///
/// The reply carries the marker that the record now holds:
/// `{ file: { name?, media_type?, size_bytes, sha256 }, mutation_id }`.
pub(super) async fn execute_db_put_blob_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    // The route table already keeps this verb off the app socket. The check
    // keeps it true for any other caller of the executor.
    if !ctx.is_owner {
        return error_response(403, "file put is owner-only", ctx);
    }
    let put = async {
        let (plaintext, metadata) = parse_put_blob(req)?;
        let target = FileTarget {
            schema: metadata.schema,
            key: metadata.key,
        };
        target.require_hash()?;
        let schema = target.canonical(host)?;
        let file_write = host
            .db
            .seal_record_file(&plaintext, metadata.name, metadata.media_type)
            .map_err(HostError::from)?;
        drop(plaintext);
        let FileWrite::Put { marker, .. } = &file_write else {
            return Err(HostError::new(500, "a sealed file is a put"));
        };
        let marker = marker.clone();
        let components = file_mutation(schema, target.key, metadata.fields, file_write, None);
        let value = commit_mutation_components(host, components).await?;
        Ok(serde_json::json!({
            "file": marker,
            "mutation_id": value.get("mutation_id").cloned().unwrap_or(Value::Null),
        }))
    };
    render(put.await, ctx)
}
