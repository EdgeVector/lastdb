//! Local file fetch and shared binary media-type parsing.
use super::*;

pub(super) const FILE_BLOB_METADATA_HEADER: &str = "x-lastdb-file-blob-metadata";

pub(super) fn is_media_type(req: &UdsRequest, header: &str, expected: &str) -> bool {
    req.header(header).is_some_and(|value| {
        value.split(',').any(|item| {
            item.split(';')
                .next()
                .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case(expected))
        })
    })
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
    #[derive(Deserialize)]
    struct Body {
        pointer: Value,
    }

    let body: Body = match serde_json::from_slice(&req.body) {
        Ok(b) => b,
        Err(e) => {
            return error_response(
                400,
                &format!("invalid /api/db/fetch-file-blob body: {e}"),
                ctx,
            )
        }
    };

    // Resolve and verify the local blob. No remote fallback is admitted.
    {
        use fold_db::sharing::delivery_wire::{
            blob_ref_from_atom_value, file_blob_access_from_atom_value,
        };

        let Some(blob_ref) = blob_ref_from_atom_value(&body.pointer).map(str::to_string) else {
            return error_response(
                400,
                "pointer must be a $lastdb_file value with blob_ref",
                ctx,
            );
        };
        let access = match file_blob_access_from_atom_value(&body.pointer) {
            Ok(Some(access)) => access,
            Ok(None) => {
                return error_response(422, "pointer is missing file blob access metadata", ctx)
            }
            Err(e) => return error_response(422, &format!("invalid file blob access: {e}"), ctx),
        };
        let bytes = match resolve_pointer_bytes(host, &body.pointer).await {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return error_response(404, fetch_miss_message(), ctx),
            Err(fold_db::error::FoldDbError::SecurityError(msg)) => {
                return error_response(422, &format!("file blob verification failed: {msg}"), ctx)
            }
            Err(e) => return mapped_error_response("file blob fetch failed", e, ctx),
        };

        if is_media_type(req, "accept", "application/octet-stream") {
            let byte_count = bytes.len();
            return UdsResponse::new(200, "OK", bytes)
                .with_header("Content-Type", "application/octet-stream")
                .with_header("X-LastDB-File-Blob-Ref", blob_ref)
                .with_header("X-LastDB-File-Hash", access.file_hash)
                .with_header("X-LastDB-File-Blob-Bytes", byte_count.to_string());
        }

        let bytes_b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
        json_ok(&envelope(
            &serde_json::json!({
                "file_blob": {
                    "blob_ref": blob_ref,
                    "file_hash": access.file_hash,
                    "bytes_b64": bytes_b64,
                    "bytes": bytes.len(),
                    "cached": true,
                }
            }),
            ctx.user_id.as_str(),
        ))
    }
}
