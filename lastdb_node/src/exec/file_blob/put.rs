//! `POST /api/db/file-blob`: upload one file blob in the JSON or binary form.

use super::*;
use std::borrow::Cow;

/// `application/octet-stream` metadata is a compact JSON object in this
/// header. Keeping metadata out of the body removes base64 expansion while
/// preserving the existing request fields and response shape.
pub(in crate::exec) const FILE_BLOB_METADATA_HEADER: &str = "x-lastdb-file-blob-metadata";

pub(in crate::exec) fn is_media_type(req: &UdsRequest, header: &str, expected: &str) -> bool {
    req.header(header).is_some_and(|value| {
        value.split(',').any(|item| {
            item.split(';')
                .next()
                .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case(expected))
        })
    })
}

fn binary_file_blob_metadata(
    req: &UdsRequest,
    ctx: &AccessContext,
) -> Result<FileBlobWriteMetadata, UdsResponse> {
    let raw = req
        .header(FILE_BLOB_METADATA_HEADER)
        .or_else(|| req.header("x-lastdb-blob-metadata"))
        .ok_or_else(|| {
            error_response(
                400,
                "binary file-blob uploads require X-LastDB-File-Blob-Metadata JSON",
                ctx,
            )
        })?;
    serde_json::from_str(raw)
        .map_err(|e| error_response(400, &format!("invalid binary file-blob metadata: {e}"), ctx))
}

/// Decode the historical JSON body: metadata plus base64 `bytes_b64`.
fn parse_json_put(
    req: &UdsRequest,
    ctx: &AccessContext,
) -> Result<(FileBlobWriteMetadata, Vec<u8>), UdsResponse> {
    #[derive(Deserialize)]
    struct Body {
        schema: String,
        field: String,
        key: KeyValue,
        bytes_b64: String,
        #[serde(default)]
        thumbnail: Option<ThumbnailBody>,
        #[serde(default)]
        writer_pubkey: Option<String>,
        #[serde(default)]
        mutation_type: Option<MutationType>,
        #[serde(default)]
        name: Option<String>,
        #[serde(default)]
        media_type: Option<String>,
        #[serde(default)]
        cache_local_plaintext: bool,
        #[serde(default)]
        additional_fields: HashMap<String, Value>,
    }

    let body: Body = serde_json::from_slice(&req.body)
        .map_err(|e| error_response(400, &format!("invalid /api/db/file-blob body: {e}"), ctx))?;
    let plaintext = base64::engine::general_purpose::STANDARD
        .decode(&body.bytes_b64)
        .map_err(|e| error_response(400, &format!("bytes_b64 is invalid base64: {e}"), ctx))?;
    let metadata = FileBlobWriteMetadata {
        schema: body.schema,
        field: body.field,
        key: body.key,
        thumbnail: body.thumbnail,
        writer_pubkey: body.writer_pubkey,
        mutation_type: body.mutation_type,
        name: body.name,
        media_type: body.media_type,
        cache_local_plaintext: body.cache_local_plaintext,
        additional_fields: body.additional_fields,
    };
    Ok((metadata, plaintext))
}

/// Read the metadata and the plaintext bytes from either upload form.
fn parse_put_request<'a>(
    req: &'a UdsRequest,
    ctx: &AccessContext,
) -> Result<(FileBlobWriteMetadata, Cow<'a, [u8]>), UdsResponse> {
    if is_media_type(req, "content-type", "application/octet-stream") {
        let metadata = binary_file_blob_metadata(req, ctx)?;
        // Borrow the raw body: a clone would hold a second copy of up to
        // 128 MiB for the whole upload.
        Ok((metadata, Cow::Borrowed(req.body.as_slice())))
    } else {
        let (metadata, plaintext) = parse_json_put(req, ctx)?;
        Ok((metadata, Cow::Owned(plaintext)))
    }
}

/// `POST /api/db/file-blob` — upload one file blob and persist its
/// `$lastdb_file` pointer into the requested schema field.
///
/// JSON clients keep the historical `bytes_b64` body. Binary clients send
/// `Content-Type: application/octet-stream`, the raw bytes as the body, and
/// the other fields as compact JSON in `X-LastDB-File-Blob-Metadata`.
pub(in crate::exec) async fn execute_db_put_file_blob_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let (metadata, plaintext) = match parse_put_request(req, ctx) {
        Ok(parsed) => parsed,
        Err(response) => return response,
    };
    if let Some(response) = reject_blank_schema_field(&metadata, ctx) {
        return response;
    }
    let Some(sync_engine) = host.db.sync_engine() else {
        return error_response(409, "cloud sync is not configured on this node", ctx);
    };
    let request = match metadata.into_write(host) {
        Ok(request) => request,
        Err(e) => return error_response(400, &e, ctx),
    };

    let result = match crate::maintenance_write_gate::run(
        host,
        host.db
            .put_personal_file_blob(sync_engine.as_ref(), request, &plaintext),
    )
    .await
    {
        Some(Ok(result)) => result,
        // Permanent cloud cap: HTTP 429 (not 500) so handlers skip ERROR
        // telemetry storms and clients treat it as non-retryable.
        Some(Err(fold_db::error::FoldDbError::QuotaExceeded(msg))) => {
            return quota_exceeded_response("file blob upload failed", &msg, ctx)
        }
        Some(Err(e)) => return mapped_error_response("file blob upload failed", e, ctx),
        None => return writes_paused_response(ctx),
    };
    let access = match file_blob_access(&result.pointer, "uploaded", ctx) {
        Ok(access) => access,
        Err(response) => return response,
    };

    json_ok(&envelope(
        &serde_json::json!({
            "file_blob": {
                "pointer": result.pointer,
                "access": access,
                "blob_ref": result.blob_ref,
                "file_hash": result.file_hash,
                "mutation_ids": result.mutation_ids,
                "bytes": plaintext.len(),
            }
        }),
        ctx.user_id.as_str(),
    ))
}
