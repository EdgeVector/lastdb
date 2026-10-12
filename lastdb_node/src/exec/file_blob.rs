//! File-blob routes: put, fetch and fork, with the caller-supplied thumbnail and write-metadata wire types.

use super::*;
use fold_db::fold_db_core::fold_db::{PersonalFileBlobFork, PersonalFileBlobWrite};

mod put;
pub(super) use self::put::{
    execute_db_put_file_blob_route, is_media_type, FILE_BLOB_METADATA_HEADER,
};

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
    if let Some(response) = reject_blank_schema_field(&metadata, ctx) {
        return response;
    }
    let Some(sync_engine) = host.db.sync_engine() else {
        return error_response(409, "cloud sync is not configured on this node", ctx);
    };
    let write = match metadata.into_write(host) {
        Ok(write) => write,
        Err(e) => return error_response(400, &e, ctx),
    };
    let request = PersonalFileBlobFork {
        source_pointer: body.pointer,
        write,
    };

    let result = match crate::maintenance_write_gate::run(
        host,
        host.db
            .fork_personal_file_blob(sync_engine.as_ref(), request),
    )
    .await
    {
        Some(Ok(result)) => result,
        Some(Err(fold_db::error::FoldDbError::SecurityError(msg))) => {
            return error_response(422, &format!("file blob verification failed: {msg}"), ctx)
        }
        Some(Err(fold_db::error::FoldDbError::QuotaExceeded(msg))) => {
            return quota_exceeded_response("file blob fork failed", &msg, ctx)
        }
        Some(Err(e)) => return mapped_error_response("file blob fork failed", e, ctx),
        None => return writes_paused_response(ctx),
    };
    let access = match file_blob_access(&result.pointer, "forked", ctx) {
        Ok(access) => access,
        Err(response) => return response,
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

impl FileBlobWriteMetadata {
    /// Build the engine write request, filling the node's key and `Update` as defaults.
    /// The error text is the client-facing 400 message for a bad thumbnail.
    fn into_write(self, host: &Host) -> Result<PersonalFileBlobWrite, String> {
        let thumbnail = self.thumbnail.map(ThumbnailBody::into_upload).transpose()?;
        Ok(PersonalFileBlobWrite {
            schema_name: self.schema,
            field_name: self.field,
            key_value: self.key,
            writer_pubkey: self.writer_pubkey.unwrap_or_else(|| host.public_key()),
            mutation_type: self.mutation_type.unwrap_or(MutationType::Update),
            name: self.name,
            media_type: self.media_type,
            thumbnail,
            cache_local_plaintext: self.cache_local_plaintext,
            additional_fields: self.additional_fields,
        })
    }
}

/// 400 when the target schema or field name is blank.
fn reject_blank_schema_field(
    metadata: &FileBlobWriteMetadata,
    ctx: &AccessContext,
) -> Option<UdsResponse> {
    if metadata.schema.trim().is_empty() {
        return Some(error_response(400, "schema must not be empty", ctx));
    }
    if metadata.field.trim().is_empty() {
        return Some(error_response(400, "field must not be empty", ctx));
    }
    None
}

/// 429 for a permanent cloud quota cap, so clients treat it as non-retryable.
fn quota_exceeded_response(action: &str, msg: &str, ctx: &AccessContext) -> UdsResponse {
    error_response(
        429,
        &format!(
            "{action}: quota exceeded: {msg}; free space or run `lastdb cloud upgrade`, then `lastdb cloud status`"
        ),
        ctx,
    )
}

fn writes_paused_response(ctx: &AccessContext) -> UdsResponse {
    error_response(
        409,
        "local writes are paused for the selected cloud source",
        ctx,
    )
}

/// Read the access metadata off a freshly written pointer; `verb` names it in the 500 text.
fn file_blob_access(
    pointer: &Value,
    verb: &str,
    ctx: &AccessContext,
) -> Result<fold_db::sharing::delivery_wire::FileBlobAccess, UdsResponse> {
    match fold_db::sharing::delivery_wire::file_blob_access_from_atom_value(pointer) {
        Ok(Some(access)) => Ok(access),
        Ok(None) => Err(error_response(
            500,
            &format!("{verb} file blob pointer is missing access metadata"),
            ctx,
        )),
        Err(e) => Err(error_response(
            500,
            &format!("{verb} file blob pointer has invalid access metadata: {e}"),
            ctx,
        )),
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

    // The local `cas_blobs` read needs no sync engine: only the cloud fallback
    // on a local miss does, and `resolve_pointer_bytes` takes it when present.
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
