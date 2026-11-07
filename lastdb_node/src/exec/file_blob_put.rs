//! `POST /api/db/put-blob`: store the file of one record with the record.
//!
//! One native write puts the sealed bytes at the record's file path, sets the
//! record's file marker and writes the optional fields. A file therefore never
//! exists without its record. A new file on a record replaces the old bytes
//! at the same path in the same write.
//!
//! With `placement: "inline"` the bytes become the record's inline file
//! instead (`design-lastdb-inline-files-20261107`): at most 64 KiB, kept in
//! its own row next to the record, backed up with the records. The record
//! holds only the marker. An optional `thumbnail` puts
//! an inline file next to a new blob file in the same write.
//!
//! Native format, pending memory and disk-space checks admit the stored row.
//! JSON requests retain the socket's general request-body allowance.

use super::file_blob::Placement;
use super::*;
use fold_db::record_value::{InlineFile, InlineWrite};
use fold_db::schema::types::FileWrite;
use std::borrow::Cow;

/// A small image or preview stored as the inline file next to a blob file.
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

/// The complete mutation accompanies the new bytes. No existing fields are merged.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PutBlobMetadata {
    mutation: fold_db::schema::types::operations::Operation,
    name: Option<String>,
    media_type: Option<String>,
    #[serde(default)]
    placement: Placement,
    thumbnail: Option<Thumbnail>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JsonBody<'a> {
    mutation: fold_db::schema::types::operations::Operation,
    #[serde(borrow)]
    bytes_b64: Cow<'a, str>,
    name: Option<String>,
    media_type: Option<String>,
    #[serde(default)]
    placement: Placement,
    thumbnail: Option<Thumbnail>,
}

/// Read the plaintext and the metadata from either request form.
///
/// - `Content-Type: application/octet-stream`: the body starts with compact
///   JSON of the length in `X-LastDB-File-Blob-Metadata-Length`, then raw bytes.
/// - Otherwise a JSON body `{ mutation, bytes_b64, name?, media_type?,
///   placement?, thumbnail? }`.
fn parse_put_blob(req: &UdsRequest) -> Result<(Cow<'_, [u8]>, PutBlobMetadata), HostError> {
    if is_media_type(req.header("content-type"), "application/octet-stream") {
        let length = req
            .header("x-lastdb-file-blob-metadata-length")
            .ok_or_else(|| HostError::new(400, "a raw put-blob requires the metadata length"))?;
        let length = length
            .parse::<usize>()
            .ok()
            .filter(|length| *length <= lastdb_uds::uds_http::MAX_BODY_LEN)
            .ok_or_else(|| HostError::new(400, "invalid file metadata length"))?;
        let (json, bytes) = req
            .body
            .split_at_checked(length)
            .ok_or_else(|| HostError::new(400, "file metadata exceeds the request body"))?;
        let metadata = serde_json::from_slice(json)
            .map_err(|error| HostError::new(400, format!("invalid file metadata: {error}")))?;
        return Ok((Cow::Borrowed(bytes), metadata));
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
            mutation: body.mutation,
            name: body.name,
            media_type: body.media_type,
            placement: body.placement,
            thumbnail: body.thumbnail,
        },
    ))
}

/// `POST /api/db/put-blob` — store the file of one record. OWNER socket only.
///
/// The reply carries the marker that the record now holds:
/// `{ file: { name?, media_type?, size_bytes, sha256 }, placement,
/// thumbnail?, mutation_id }`. `thumbnail` is the inline file marker that a
/// blob put stored with its `thumbnail`.
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
        let PutBlobMetadata {
            mutation,
            name,
            media_type,
            placement,
            thumbnail,
        } = metadata;
        let mut components = handlers::MutationComponents::from_operation(mutation, None);
        if !matches!(
            components.mutation_type,
            fold_db::schema::types::MutationType::Create
                | fold_db::schema::types::MutationType::Replace
        ) {
            return Err(HostError::new(
                400,
                "put-blob requires create or complete replacement",
            ));
        }
        if placement == Placement::Inline {
            if thumbnail.is_some() {
                return Err(HostError::new(
                    400,
                    "an inline put takes no thumbnail; the bytes are the inline file",
                ));
            }
            // The blob file and its marker stay as they are.
            let file = inline_file(&plaintext, name, media_type)?;
            drop(plaintext);
            let marker = file.marker.clone();
            components.inline_write = Some(InlineWrite::Put(file));
            let value = commit_mutation_components(host, components).await?;
            return Ok(serde_json::json!({
                "file": marker,
                "placement": placement,
                "mutation_id": value.get("mutation_id"),
                "record_revision": value.get("record_revision"),
            }));
        }
        let thumbnail = thumbnail.map(Thumbnail::into_inline).transpose()?;
        let file_write = host
            .db
            .seal_record_file(&plaintext, name, media_type)
            .map_err(HostError::from)?;
        drop(plaintext);
        let FileWrite::Put(file) = &file_write else {
            return Err(HostError::new(500, "a sealed file is a put"));
        };
        let marker = file.marker().clone();
        if components
            .file
            .as_ref()
            .is_some_and(|supplied| supplied != &marker)
        {
            return Err(HostError::new(
                400,
                "file marker does not match the supplied bytes",
            ));
        }
        components.file = Some(marker.clone());
        components.file_write = Some(file_write);
        let thumbnail_marker = thumbnail.as_ref().map(|file| file.marker.clone());
        if let Some(file) = thumbnail {
            components.inline_write = Some(InlineWrite::Put(file));
        }
        let value = commit_mutation_components(host, components).await?;
        Ok(serde_json::json!({
            "file": marker,
            "placement": placement,
            "thumbnail": thumbnail_marker,
            "mutation_id": value.get("mutation_id"),
            "record_revision": value.get("record_revision"),
        }))
    };
    render(put.await, ctx)
}
