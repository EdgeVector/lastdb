//! Local file-blob routes: store a blob in this node's own `cas_blobs` plane
//! and read it back, with no sync engine and no cloud call.
//!
//! `POST /api/db/put-blob-local` makes the bytes durable and returns the
//! `$lastdb_file` pointer. It writes no record: the caller puts the pointer in
//! a record of its choice, in a batch it controls. No cloud call occurs.
//!
//! Native format, pending memory and disk-space checks admit the stored row.
//! JSON requests retain the socket's general request-body allowance.

use super::*;
use std::borrow::Cow;

/// Optional pointer metadata. Without it, identical bytes in one schema give
/// the same atom, so a second record that holds the pointer adds no atom.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct LocalBlobMetadata {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    media_type: Option<String>,
}

/// JSON form of the request. `deny_unknown_fields` makes a caller that sends
/// the cloud route's `schema` / `field` / `key` fail with 400, instead of
/// believing that a record was written.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JsonBody<'a> {
    /// Borrowed from the request body, so the base64 text is not copied.
    #[serde(borrow)]
    bytes_b64: Cow<'a, str>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    media_type: Option<String>,
}

/// A request the route refuses: the status and the message.
type Refusal = (u16, String);

/// Read the plaintext and the optional metadata from either request form.
///
/// - `Content-Type: application/octet-stream`: the body IS the plaintext,
///   borrowed without a copy. Optional `name` / `media_type` ride as compact
///   JSON in `X-LastDB-File-Blob-Metadata`.
/// - Otherwise a JSON body `{ "bytes_b64": ..., "name"?, "media_type"? }`.
///
fn parse_put_blob_local(req: &UdsRequest) -> Result<(Cow<'_, [u8]>, LocalBlobMetadata), Refusal> {
    if is_media_type(req.header("content-type"), "application/octet-stream") {
        let metadata = match req.header(FILE_BLOB_METADATA_HEADER) {
            Some(raw) => serde_json::from_str(raw)
                .map_err(|e| (400, format!("invalid local blob metadata header: {e}")))?,
            None => LocalBlobMetadata::default(),
        };
        return Ok((Cow::Borrowed(req.body.as_slice()), metadata));
    }
    if req.body.len() > lastdb_uds::uds_http::MAX_BODY_LEN {
        return Err((
            413,
            "JSON request exceeds the request body allowance".into(),
        ));
    }
    let body: JsonBody<'_> = serde_json::from_slice(&req.body)
        .map_err(|e| (400, format!("invalid /api/db/put-blob-local body: {e}")))?;
    let capacity = base64::decoded_len_estimate(body.bytes_b64.len());
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .map_err(|_| (503, "local file decode allocation refused".into()))?;
    bytes.resize(capacity, 0);
    let decoded = base64::engine::general_purpose::STANDARD
        .decode_slice(body.bytes_b64.as_bytes(), &mut bytes)
        .map_err(|e| (400, format!("bytes_b64 is invalid base64: {e}")))?;
    bytes.truncate(decoded);
    Ok((
        Cow::Owned(bytes),
        LocalBlobMetadata {
            name: body.name,
            media_type: body.media_type,
        },
    ))
}

/// `POST /api/db/put-blob-local` — store one blob in the local `cas_blobs`
/// plane and return its pointer. OWNER socket only.
///
/// The row is flushed before the answer, so a caller that writes a record that
/// names the blob afterwards cannot outlive the blob in a crash.
///
/// WARNING: nothing here writes a record or checks that one exists. The blob is
/// kept only while a record holds the pointer as the WHOLE value of a field of
/// type `Any`. A pointer inside a JSON string is not a reference, so the dwell
/// pass deletes that blob once its dwell (`LASTDB_BLOB_DWELL_SECS`, one hour by
/// default) has passed with no reference.
pub(super) async fn execute_db_put_blob_local_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    // The route table already keeps this verb off the app socket. The check
    // keeps it true for any other caller of the executor, and for a future
    // verified-app posture on the owner socket.
    if !ctx.is_owner {
        return error_response(403, "local blob put is owner-only", ctx);
    }
    {
        let (plaintext, metadata) = match parse_put_blob_local(req) {
            Ok(parsed) => parsed,
            Err((status, message)) => return error_response(status, &message, ctx),
        };
        let put = match crate::maintenance_write_gate::run(
            host,
            host.db.put_local_file_blob(
                &plaintext,
                metadata.name.as_deref(),
                metadata.media_type.as_deref(),
            ),
        )
        .await
        {
            Some(Ok(put)) => put,
            Some(Err(e)) => return mapped_error_response("local blob put failed", e, ctx),
            None => {
                return error_response(
                    409,
                    "local writes are paused for the selected cloud source",
                    ctx,
                )
            }
        };
        json_ok(&envelope(
            &serde_json::json!({
                "file_blob": {
                    "pointer": put.pointer,
                    "blob_ref": put.blob_ref,
                    "file_hash": put.file_hash,
                    "bytes": put.bytes,
                    "stored": put.stored(),
                }
            }),
            ctx.user_id.as_str(),
        ))
    }
}
