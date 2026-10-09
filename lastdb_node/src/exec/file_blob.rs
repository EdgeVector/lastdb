//! File-blob routes: put, fetch and fork, with the caller-supplied thumbnail and write-metadata wire types.

use super::*;
use std::borrow::Cow;

/// Caller-supplied thumbnail derivative on a file-blob write.
///
/// The kernel does not decode or resize images (fold #1354) — whatever uploads
/// the file owns producing its preview. Bytes ride as base64 in the same JSON
/// body as the file's own `bytes_b64`, so this needs no new transport. Optional
/// and `#[serde(default)]`: a body that omits it behaves exactly as before, the
/// pointer simply carries no thumbnail.
#[derive(Deserialize)]
pub(super) struct ThumbnailBody {
    bytes_b64: String,
    media_type: String,
    width: u32,
    height: u32,
    /// `image` or `video-poster` — validated by the sync engine on upload.
    kind: String,
}

#[cfg(feature = "cloud-sync")]
impl ThumbnailBody {
    /// Decode into the engine's upload type. Only base64 is validated here;
    /// dimension/kind/media-type rules and the 64 KiB tier cap stay with
    /// `upload_file_thumbnail` so every caller hits the same gate.
    pub(super) fn into_upload(self) -> Result<fold_db::sync::engine::FileThumbnailUpload, String> {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(&self.bytes_b64)
            .map_err(|e| format!("thumbnail.bytes_b64 is invalid base64: {e}"))?;
        Ok(fold_db::sync::engine::FileThumbnailUpload {
            bytes,
            media_type: self.media_type,
            width: self.width,
            height: self.height,
            kind: self.kind,
        })
    }
}

/// `POST /api/db/fork-file-blob` — fetch a shared pointer's bytes, upload them
/// into this node's personal file-blob scope, and rewrite the local pointer.
pub(super) async fn execute_db_fork_file_blob_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    struct Body {
        schema: String,
        field: String,
        key: KeyValue,
        pointer: Value,
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

    let body: Body = match serde_json::from_slice(&req.body) {
        Ok(b) => b,
        Err(e) => {
            return error_response(
                400,
                &format!("invalid /api/db/fork-file-blob body: {e}"),
                ctx,
            )
        }
    };
    if body.schema.trim().is_empty() {
        return error_response(400, "schema must not be empty", ctx);
    }
    if body.field.trim().is_empty() {
        return error_response(400, "field must not be empty", ctx);
    }

    #[cfg(not(feature = "cloud-sync"))]
    {
        let _ = (body, host);
        return error_response(
            501,
            "cloud-sync support is not compiled into this daemon",
            ctx,
        );
    }

    #[cfg(feature = "cloud-sync")]
    {
        use fold_db::fold_db_core::fold_db::{PersonalFileBlobFork, PersonalFileBlobWrite};
        use fold_db::sharing::delivery_wire::file_blob_access_from_atom_value;

        let Some(sync_engine) = host.db.sync_engine() else {
            return error_response(409, "cloud sync is not configured on this node", ctx);
        };

        let thumbnail = match body.thumbnail.map(ThumbnailBody::into_upload).transpose() {
            Ok(t) => t,
            Err(e) => return error_response(400, &e, ctx),
        };

        let request = PersonalFileBlobFork {
            source_pointer: body.pointer,
            write: PersonalFileBlobWrite {
                schema_name: body.schema,
                field_name: body.field,
                key_value: body.key,
                writer_pubkey: body.writer_pubkey.unwrap_or_else(|| host.public_key()),
                mutation_type: body.mutation_type.unwrap_or(MutationType::Update),
                name: body.name,
                media_type: body.media_type,
                thumbnail,
                cache_local_plaintext: body.cache_local_plaintext,
                additional_fields: body.additional_fields,
            },
        };

        let result = match host
            .db
            .fork_personal_file_blob(sync_engine.as_ref(), request)
            .await
        {
            Ok(result) => result,
            Err(fold_db::error::FoldDbError::SecurityError(msg)) => {
                return error_response(422, &format!("file blob verification failed: {msg}"), ctx)
            }
            Err(fold_db::error::FoldDbError::QuotaExceeded(msg)) => {
                return error_response(
                    429,
                    &format!(
                        "file blob fork failed: quota exceeded: {msg}; free space or run `lastdb cloud upgrade`, then `lastdb cloud status`"
                    ),
                    ctx,
                )
            }
            Err(e) => return mapped_error_response("file blob fork failed", e, ctx),
        };
        let access = match file_blob_access_from_atom_value(&result.pointer) {
            Ok(Some(access)) => access,
            Ok(None) => {
                return error_response(
                    500,
                    "forked file blob pointer is missing access metadata",
                    ctx,
                )
            }
            Err(e) => {
                return error_response(
                    500,
                    &format!("forked file blob pointer has invalid access metadata: {e}"),
                    ctx,
                )
            }
        };

        json_ok(&envelope(
            &serde_json::json!({
                "file_blob": {
                    "pointer": result.pointer,
                    "access": access,
                    "source_blob_ref": result.source_blob_ref,
                    "blob_ref": result.blob_ref,
                    "file_hash": result.file_hash,
                    "mutation_ids": result.mutation_ids,
                    "bytes": result.bytes,
                }
            }),
            ctx.user_id.as_str(),
        ))
    }
}

/// Metadata shared by the JSON and binary file-blob upload forms.
#[derive(Deserialize)]
pub(super) struct FileBlobWriteMetadata {
    pub(super) schema: String,
    pub(super) field: String,
    pub(super) key: KeyValue,
    #[serde(default)]
    pub(super) thumbnail: Option<ThumbnailBody>,
    #[serde(default)]
    pub(super) writer_pubkey: Option<String>,
    #[serde(default)]
    pub(super) mutation_type: Option<MutationType>,
    #[serde(default)]
    pub(super) name: Option<String>,
    #[serde(default)]
    pub(super) media_type: Option<String>,
    #[serde(default)]
    pub(super) cache_local_plaintext: bool,
    #[serde(default)]
    pub(super) additional_fields: HashMap<String, Value>,
}

/// `application/octet-stream` metadata is a compact JSON object in this
/// header. Keeping metadata out of the body removes base64 expansion while
/// preserving the existing request fields and response shape.
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

pub(super) fn binary_file_blob_metadata(
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

/// `POST /api/db/file-blob` — upload one file blob and persist its
/// `$lastdb_file` pointer into the requested schema field.
///
/// JSON clients keep the historical `bytes_b64` body. Binary clients send
/// `Content-Type: application/octet-stream`, the raw bytes as the body, and
/// the other fields as compact JSON in `X-LastDB-File-Blob-Metadata`.
pub(super) async fn execute_db_put_file_blob_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
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

    let binary = is_media_type(req, "content-type", "application/octet-stream");
    let (metadata, plaintext): (FileBlobWriteMetadata, Cow<'_, [u8]>) = if binary {
        let metadata = match binary_file_blob_metadata(req, ctx) {
            Ok(metadata) => metadata,
            Err(response) => return response,
        };
        // Borrow the raw body: a clone would hold a second copy of up to
        // 128 MiB for the whole upload.
        (metadata, Cow::Borrowed(req.body.as_slice()))
    } else {
        let body: Body = match serde_json::from_slice(&req.body) {
            Ok(b) => b,
            Err(e) => {
                return error_response(400, &format!("invalid /api/db/file-blob body: {e}"), ctx)
            }
        };
        let plaintext = match base64::engine::general_purpose::STANDARD.decode(&body.bytes_b64) {
            Ok(bytes) => bytes,
            Err(e) => {
                return error_response(400, &format!("bytes_b64 is invalid base64: {e}"), ctx)
            }
        };
        (
            FileBlobWriteMetadata {
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
            },
            Cow::Owned(plaintext),
        )
    };
    if metadata.schema.trim().is_empty() {
        return error_response(400, "schema must not be empty", ctx);
    }
    if metadata.field.trim().is_empty() {
        return error_response(400, "field must not be empty", ctx);
    }

    #[cfg(not(feature = "cloud-sync"))]
    {
        let _ = (metadata, plaintext, host);
        return error_response(
            501,
            "cloud-sync support is not compiled into this daemon",
            ctx,
        );
    }

    #[cfg(feature = "cloud-sync")]
    {
        use fold_db::fold_db_core::fold_db::PersonalFileBlobWrite;
        use fold_db::sharing::delivery_wire::file_blob_access_from_atom_value;

        let Some(sync_engine) = host.db.sync_engine() else {
            return error_response(409, "cloud sync is not configured on this node", ctx);
        };

        let thumbnail = match metadata
            .thumbnail
            .map(ThumbnailBody::into_upload)
            .transpose()
        {
            Ok(t) => t,
            Err(e) => return error_response(400, &e, ctx),
        };

        let request = PersonalFileBlobWrite {
            schema_name: metadata.schema,
            field_name: metadata.field,
            key_value: metadata.key,
            writer_pubkey: metadata.writer_pubkey.unwrap_or_else(|| host.public_key()),
            mutation_type: metadata.mutation_type.unwrap_or(MutationType::Update),
            name: metadata.name,
            media_type: metadata.media_type,
            cache_local_plaintext: metadata.cache_local_plaintext,
            additional_fields: metadata.additional_fields,
            thumbnail,
        };

        let result = match host
            .db
            .put_personal_file_blob(sync_engine.as_ref(), request, &plaintext)
            .await
        {
            Ok(result) => result,
            // Permanent cloud cap: HTTP 429 (not 500) so handlers skip ERROR
            // telemetry storms and clients treat it as non-retryable.
            Err(fold_db::error::FoldDbError::QuotaExceeded(msg)) => {
                return error_response(
                    429,
                    &format!(
                        "file blob upload failed: quota exceeded: {msg}; free space or run `lastdb cloud upgrade`, then `lastdb cloud status`"
                    ),
                    ctx,
                )
            }
            Err(e) => return mapped_error_response("file blob upload failed", e, ctx),
        };
        let access = match file_blob_access_from_atom_value(&result.pointer) {
            Ok(Some(access)) => access,
            Ok(None) => {
                return error_response(
                    500,
                    "uploaded file blob pointer is missing access metadata",
                    ctx,
                )
            }
            Err(e) => {
                return error_response(
                    500,
                    &format!("uploaded file blob pointer has invalid access metadata: {e}"),
                    ctx,
                )
            }
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
}

/// `POST /api/db/fetch-file-blob` — explicit single-pointer remote CAS fetch.
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

    #[cfg(not(feature = "sharing"))]
    {
        let _ = (body, host);
        return error_response(
            501,
            "file-blob support (the `sharing` feature) is not compiled into this daemon",
            ctx,
        );
    }

    // The local `cas_blobs` read needs no sync engine: only the cloud fallback
    // on a local miss does, and `resolve_pointer_bytes` takes it when present.
    #[cfg(feature = "sharing")]
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
            Ok(None) => return error_response(404, fetch_miss_message(host), ctx),
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
