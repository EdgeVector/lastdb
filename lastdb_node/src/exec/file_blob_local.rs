//! Local file-blob routes: store a blob in this node's own `cas_blobs` plane
//! and read it back, with no sync engine and no cloud call.
//!
//! `POST /api/db/put-blob-local` makes the bytes durable and returns the
//! `$lastdb_file` pointer. It writes no record: the caller puts the pointer in
//! a record of its choice, in a batch it controls. Cloud mode keeps its own
//! route (`POST /api/db/file-blob`) and still answers 409 without an engine.
//!
//! A blob is bounded by `LASTDB_LOCAL_FILE_BLOB_MAX_BYTES` (16 MiB unless the
//! owner raises it). The route answers 413 above it, before it decodes or
//! copies the body. Store larger files as slabs.

use super::*;
#[cfg(feature = "sharing")]
use std::borrow::Cow;

/// Optional pointer metadata. Without it, identical bytes in one schema give
/// the same atom, so a second record that holds the pointer adds no atom.
#[cfg(feature = "sharing")]
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
#[cfg(feature = "sharing")]
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
#[cfg(feature = "sharing")]
type Refusal = (u16, String);

#[cfg(feature = "sharing")]
fn too_large(bytes: usize, max: usize) -> Refusal {
    (
        413,
        format!(
            "blob is {bytes} bytes, over the {max} byte limit for one local blob \
             ({}); store large files as slabs",
            fold_db::fold_db_core::fold_db::LOCAL_FILE_BLOB_MAX_BYTES_ENV
        ),
    )
}

/// Read the plaintext and the optional metadata from either request form.
///
/// - `Content-Type: application/octet-stream`: the body IS the plaintext,
///   borrowed without a copy. Optional `name` / `media_type` ride as compact
///   JSON in `X-LastDB-File-Blob-Metadata`.
/// - Otherwise a JSON body `{ "bytes_b64": ..., "name"?, "media_type"? }`.
///
/// `max` is the blob limit. Each form checks it before it copies or decodes.
#[cfg(feature = "sharing")]
fn parse_put_blob_local(
    req: &UdsRequest,
    max: usize,
) -> Result<(Cow<'_, [u8]>, LocalBlobMetadata), Refusal> {
    if is_media_type(req, "content-type", "application/octet-stream") {
        if req.body.len() > max {
            return Err(too_large(req.body.len(), max));
        }
        let metadata = match req.header(FILE_BLOB_METADATA_HEADER) {
            Some(raw) => serde_json::from_str(raw)
                .map_err(|e| (400, format!("invalid local blob metadata header: {e}")))?,
            None => LocalBlobMetadata::default(),
        };
        return Ok((Cow::Borrowed(req.body.as_slice()), metadata));
    }
    let body: JsonBody<'_> = serde_json::from_slice(&req.body)
        .map_err(|e| (400, format!("invalid /api/db/put-blob-local body: {e}")))?;
    // Base64 text is 4 characters for each 3 bytes, so text longer than this
    // cannot decode to a blob within the limit. Refuse it before the decode.
    let max_text = max.div_ceil(3).saturating_mul(4);
    if body.bytes_b64.len() > max_text {
        return Err(too_large(body.bytes_b64.len() / 4 * 3, max));
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(body.bytes_b64.as_bytes())
        .map_err(|e| (400, format!("bytes_b64 is invalid base64: {e}")))?;
    if bytes.len() > max {
        return Err(too_large(bytes.len(), max));
    }
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
/// type `Any`. A pointer inside a JSON string is not a reference, so
/// `gc-file-blobs` reclaims that blob once its row is older than 600 s.
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
    #[cfg(not(feature = "sharing"))]
    {
        let _ = (req, host);
        error_response(
            501,
            "local blob put needs the `sharing` feature, which is not compiled into this daemon",
            ctx,
        )
    }

    #[cfg(feature = "sharing")]
    {
        let max = fold_db::fold_db_core::fold_db::local_file_blob_max_bytes();
        let (plaintext, metadata) = match parse_put_blob_local(req, max) {
            Ok(parsed) => parsed,
            Err((status, message)) => return error_response(status, &message, ctx),
        };
        // The socket already refuses a body over this cap; the check keeps the
        // limit true for any other caller of the executor.
        if plaintext.len() > lastdb_uds::uds_http::MAX_BODY_LEN {
            return error_response(413, "blob is larger than the request body cap", ctx);
        }
        let put = match host
            .db
            .put_local_file_blob(
                &plaintext,
                metadata.name.as_deref(),
                metadata.media_type.as_deref(),
            )
            .await
        {
            Ok(put) => put,
            Err(e) => return mapped_error_response("local blob put failed", e, ctx),
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

/// Read the bytes a `$lastdb_file` pointer names: the local `cas_blobs` row
/// first. Only a node that has a sync engine then asks the cloud on a miss.
#[cfg(feature = "sharing")]
pub(super) async fn resolve_pointer_bytes(
    host: &Host,
    pointer: &Value,
) -> Result<Option<Vec<u8>>, fold_db::error::FoldDbError> {
    #[cfg(feature = "cloud-sync")]
    if let Some(engine) = host.db.sync_engine() {
        return fold_db::sharing::query_slice::resolve_file_bytes_on_demand(
            &host.db,
            engine.as_ref(),
            pointer,
        )
        .await;
    }
    fold_db::sharing::query_slice::resolve_file_bytes(&host.db, pointer).await
}

/// The 404 text for a pointer whose blob is not on this node.
#[cfg(feature = "sharing")]
pub(super) fn fetch_miss_message(host: &Host) -> &'static str {
    #[cfg(feature = "cloud-sync")]
    if host.db.sync_engine().is_some() {
        return "file blob was not found locally or in remote CAS";
    }
    #[cfg(not(feature = "cloud-sync"))]
    let _ = host;
    "file blob was not found locally, and this node has no cloud sync engine to ask"
}
