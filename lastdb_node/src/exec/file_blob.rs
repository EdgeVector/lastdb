//! Local file fetch flow and data-only pointer/media-type rules.
use super::*;

pub(super) const FILE_BLOB_METADATA_HEADER: &str = "x-lastdb-file-blob-metadata";

/// Match one media type in the caller-supplied header value.
pub(super) fn is_media_type(header: Option<&str>, expected: &str) -> bool {
    header.is_some_and(|value| {
        value.split(',').any(|item| {
            item.split(';')
                .next()
                .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case(expected))
        })
    })
}

/// Validated pointer and public reply metadata. No host access or byte reads.
struct FetchFileBlobRequest {
    pointer: Value,
    blob_ref: String,
    file_hash: String,
}

impl FetchFileBlobRequest {
    fn parse(body: &[u8]) -> Result<Self, (u16, String)> {
        use fold_db::sharing::delivery_wire::{
            blob_ref_from_atom_value, file_blob_access_from_atom_value,
        };

        #[derive(Deserialize)]
        struct Body {
            pointer: Value,
        }

        let Body { pointer } = serde_json::from_slice(body).map_err(|error| {
            (
                400,
                format!("invalid /api/db/fetch-file-blob body: {error}"),
            )
        })?;
        let blob_ref = blob_ref_from_atom_value(&pointer)
            .map(str::to_string)
            .ok_or_else(|| {
                (
                    400,
                    "pointer must be a $lastdb_file value with blob_ref".into(),
                )
            })?;
        let access = file_blob_access_from_atom_value(&pointer)
            .map_err(|error| (422, format!("invalid file blob access: {error}")))?
            .ok_or_else(|| (422, "pointer is missing file blob access metadata".into()))?;
        Ok(Self {
            pointer,
            blob_ref,
            file_hash: access.file_hash,
        })
    }
}

/// `POST /api/db/fetch-file-blob` — explicit single-pointer local file fetch.
///
/// JSON clients receive the historical base64 envelope. Clients that send
/// `Accept: application/octet-stream` receive the verified plaintext bytes
/// directly, with the blob reference and hash in response headers.
pub(super) async fn execute_db_fetch_file_blob_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let FetchFileBlobRequest {
        pointer,
        blob_ref,
        file_hash,
    } = match FetchFileBlobRequest::parse(&req.body) {
        Ok(request) => request,
        Err((status, message)) => return error_response(status, &message, ctx),
    };

    // Resolve and verify the local blob. No remote fallback is admitted.
    let bytes = match fold_db::sharing::query_slice::resolve_file_bytes(&host.db, &pointer).await {
        Ok(Some(bytes)) => bytes,
        Ok(None) => return error_response(404, "file blob was not found locally", ctx),
        Err(fold_db::error::FoldDbError::SecurityError(msg)) => {
            return error_response(422, &format!("file blob verification failed: {msg}"), ctx)
        }
        Err(e) => return mapped_error_response("file blob fetch failed", e, ctx),
    };

    if is_media_type(req.header("accept"), "application/octet-stream") {
        let byte_count = bytes.len();
        return UdsResponse::new(200, "OK", bytes)
            .with_header("Content-Type", "application/octet-stream")
            .with_header("X-LastDB-File-Blob-Ref", blob_ref)
            .with_header("X-LastDB-File-Hash", file_hash)
            .with_header("X-LastDB-File-Blob-Bytes", byte_count.to_string());
    }

    let bytes_b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
    json_ok(&envelope(
        &serde_json::json!({
            "file_blob": {
                "blob_ref": blob_ref,
                "file_hash": file_hash,
                "bytes_b64": bytes_b64,
                "bytes": bytes.len(),
                "cached": true,
            }
        }),
        ctx.user_id.as_str(),
    ))
}
