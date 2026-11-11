//! `POST /api/db/put-blob`: store a blob, inline, or local-reference file with its record.

use std::borrow::Cow;
use std::path::Path;

use super::file_blob::Placement;
use super::*;
use fold_db::record_value::{FileMarker, InlineFile, InlineWrite};
use fold_db::schema::types::operations::Operation;
use fold_db::schema::types::{FileWrite, MutationType};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Thumbnail {
    bytes_b64: String,
    name: Option<String>,
    media_type: Option<String>,
}

impl Thumbnail {
    fn into_inline(self) -> Result<InlineFile, HostError> {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(self.bytes_b64.as_bytes())
            .map_err(|e| {
                HostError::new(400, format!("thumbnail.bytes_b64 is invalid base64: {e}"))
            })?;
        inline_file(&bytes, self.name, self.media_type)
    }
}

fn inline_file(
    bytes: &[u8],
    name: Option<String>,
    media_type: Option<String>,
) -> Result<InlineFile, HostError> {
    InlineFile::new(bytes, name, media_type).map_err(|message| HostError::new(413, message))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PutBlobBody<'a> {
    mutation: Operation,
    #[serde(default, borrow)]
    bytes_b64: Option<Cow<'a, str>>,
    name: Option<String>,
    media_type: Option<String>,
    #[serde(default)]
    placement: Placement,
    thumbnail: Option<Thumbnail>,
    #[serde(default)]
    local_path: Option<String>,
}

fn parse_put_blob(req: &UdsRequest) -> Result<(Cow<'_, [u8]>, PutBlobBody<'_>), HostError> {
    let max_len = crate::lastdb_uds::uds_http::MAX_BODY_LEN;
    if is_media_type(req.header("content-type"), "application/octet-stream") {
        let length = req
            .header("x-lastdb-file-blob-metadata-length")
            .ok_or_else(|| HostError::new(400, "a raw put-blob requires the metadata length"))?
            .parse::<usize>()
            .ok()
            .filter(|len| *len <= max_len)
            .ok_or_else(|| HostError::new(400, "invalid file metadata length"))?;
        let (json, bytes) = req
            .body
            .split_at_checked(length)
            .ok_or_else(|| HostError::new(400, "file metadata exceeds the request body"))?;
        let metadata: PutBlobBody<'_> = serde_json::from_slice(json)
            .map_err(|e| HostError::new(400, format!("invalid file metadata: {e}")))?;
        if metadata.bytes_b64.is_some() {
            return Err(HostError::new(
                400,
                "invalid file metadata: unknown field `bytes_b64`",
            ));
        }
        if metadata.local_path.is_some() && !bytes.is_empty() {
            return Err(HostError::new(
                400,
                "a local file reference has an empty body",
            ));
        }
        return Ok((Cow::Borrowed(bytes), metadata));
    }
    if req.body.len() > max_len {
        return Err(HostError::new(
            413,
            "JSON request exceeds the request body allowance",
        ));
    }
    let mut body: PutBlobBody<'_> = serde_json::from_slice(&req.body)
        .map_err(|e| HostError::new(400, format!("invalid /api/db/put-blob body: {e}")))?;
    if body.local_path.is_some() {
        if body.bytes_b64.as_ref().is_some_and(|b| !b.is_empty()) {
            return Err(HostError::new(
                400,
                "a local file reference has an empty body",
            ));
        }
        body.bytes_b64 = None;
        return Ok((Cow::Borrowed(&[]), body));
    }
    let Some(bytes_b64) = body.bytes_b64.take() else {
        return Err(HostError::new(
            400,
            "put-blob requires bytes_b64 or local_path",
        ));
    };
    let capacity = base64::decoded_len_estimate(bytes_b64.len());
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .map_err(|_| HostError::new(503, "local file decode allocation refused"))?;
    bytes.resize(capacity, 0);
    let decoded = base64::engine::general_purpose::STANDARD
        .decode_slice(bytes_b64.as_bytes(), &mut bytes)
        .map_err(|e| HostError::new(400, format!("bytes_b64 is invalid base64: {e}")))?;
    bytes.truncate(decoded);
    Ok((Cow::Owned(bytes), body))
}

pub(super) async fn execute_db_put_blob_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    if !ctx.is_owner {
        return error_response(403, "file put is owner-only", ctx);
    }
    let put = async {
        let (plaintext, mut body) = parse_put_blob(req)?;
        let parsed = ParsedMutation::from_operation(body.mutation, None);
        if !matches!(
            parsed.mutation.mutation_type,
            MutationType::Create | MutationType::Replace
        ) {
            return Err(HostError::new(
                400,
                "put-blob requires create or complete replacement",
            ));
        }
        if body.local_path.is_some() && body.placement == Placement::Inline {
            return Err(HostError::new(
                400,
                "an inline file stores its bytes; it has no local path",
            ));
        }
        if let Some(path) = body.local_path.take() {
            return put_local_reference(
                host,
                parsed,
                (&path, plaintext),
                (body.name, body.media_type, body.placement, body.thumbnail),
            )
            .await;
        }
        if body.placement == Placement::Inline {
            if body.thumbnail.is_some() {
                return Err(HostError::new(
                    400,
                    "an inline put takes no thumbnail; the bytes are the inline file",
                ));
            }
            return put_inline_file(host, parsed, &plaintext, body.name, body.media_type).await;
        }
        put_blob_file(
            host,
            parsed,
            &plaintext,
            (body.name, body.media_type, body.placement, body.thumbnail),
        )
        .await
    };
    render(put.await, ctx)
}

type BlobMeta = (Option<String>, Option<String>, Placement, Option<Thumbnail>);

fn attach_blob_and_thumbnail(
    parsed: &mut ParsedMutation,
    marker: &FileMarker,
    file_write: FileWrite,
    thumbnail: Option<Thumbnail>,
    mismatch_msg: &'static str,
) -> Result<Option<FileMarker>, HostError> {
    if parsed
        .mutation
        .file
        .as_ref()
        .is_some_and(|supplied| supplied != marker)
    {
        return Err(HostError::new(400, mismatch_msg));
    }
    parsed.mutation.file = Some(marker.clone());
    parsed.mutation.file_write = Some(file_write);
    let thumb = thumbnail.map(Thumbnail::into_inline).transpose()?;
    let thumb_marker = thumb.as_ref().map(|f| f.marker.clone());
    if let Some(file) = thumb {
        parsed.mutation.inline_write = Some(InlineWrite::Put(file));
    }
    Ok(thumb_marker)
}

async fn put_local_reference(
    host: &Host,
    mut parsed: ParsedMutation,
    (path, plaintext): (&str, Cow<'_, [u8]>),
    (name, media_type, placement, thumbnail): BlobMeta,
) -> Result<serde_json::Value, HostError> {
    if !plaintext.is_empty() {
        return Err(HostError::new(
            400,
            "a local file reference has an empty body",
        ));
    }
    drop(plaintext);
    let file_write = host
        .db
        .reference_local_file(Path::new(path), name, media_type)
        .await
        .map_err(HostError::from)?;
    let FileWrite::LocalPath(local) = &file_write else {
        return Err(HostError::new(
            500,
            "a local file reference is a local path",
        ));
    };
    let marker = local.marker().clone();
    let source = local.path().to_path_buf();
    let schema = parsed.mutation.schema_name.clone();
    let key = parsed.mutation.key_value.clone();
    let thumbnail_marker = attach_blob_and_thumbnail(
        &mut parsed,
        &marker,
        file_write,
        thumbnail,
        "file marker does not match the local file",
    )?;
    let value = mutation::commit_parsed_mutation(host, parsed).await?;
    host.db
        .remember_local_file(&schema, &key, &source, &marker)
        .await
        .map_err(HostError::from)?;
    Ok(serde_json::json!({
        "file": marker,
        "placement": placement,
        "local": true,
        "thumbnail": thumbnail_marker,
        "mutation_id": value.get("mutation_id"),
        "record_revision": value.get("record_revision"),
    }))
}

async fn put_inline_file(
    host: &Host,
    mut parsed: ParsedMutation,
    plaintext: &[u8],
    name: Option<String>,
    media_type: Option<String>,
) -> Result<serde_json::Value, HostError> {
    let file = inline_file(plaintext, name, media_type)?;
    let marker = file.marker.clone();
    parsed.mutation.inline_write = Some(InlineWrite::Put(file));
    let value = mutation::commit_parsed_mutation(host, parsed).await?;
    Ok(serde_json::json!({
        "file": marker,
        "placement": Placement::Inline,
        "mutation_id": value.get("mutation_id"),
        "record_revision": value.get("record_revision"),
    }))
}

async fn put_blob_file(
    host: &Host,
    mut parsed: ParsedMutation,
    plaintext: &[u8],
    (name, media_type, placement, thumbnail): BlobMeta,
) -> Result<serde_json::Value, HostError> {
    let file_write = host
        .db
        .seal_record_file(plaintext, name, media_type)
        .map_err(HostError::from)?;
    let FileWrite::Put(file) = &file_write else {
        return Err(HostError::new(500, "a sealed file is a put"));
    };
    let marker = file.marker().clone();
    let thumbnail_marker = attach_blob_and_thumbnail(
        &mut parsed,
        &marker,
        file_write,
        thumbnail,
        "file marker does not match the supplied bytes",
    )?;
    let value = mutation::commit_parsed_mutation(host, parsed).await?;
    Ok(serde_json::json!({
        "file": marker,
        "placement": placement,
        "thumbnail": thumbnail_marker,
        "mutation_id": value.get("mutation_id"),
        "record_revision": value.get("record_revision"),
    }))
}
