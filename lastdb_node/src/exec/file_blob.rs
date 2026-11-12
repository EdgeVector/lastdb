//! Put and fetch routes for a record's blob, inline, or local-reference file.

use std::borrow::Cow;
use std::path::Path;

use super::*;
use fold_db::record_value::{FileMarker, InlineFile, InlineWrite};
use fold_db::schema::types::operations::Operation;
use fold_db::schema::types::{FileWrite, KeyValue, MutationType};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Placement {
    #[default]
    Blob,
    Inline,
}

fn is_media_type(header: Option<&str>, expected: &str) -> bool {
    header.is_some_and(|value| {
        value.split(',').any(|item| {
            item.split(';')
                .next()
                .is_some_and(|m| m.trim().eq_ignore_ascii_case(expected))
        })
    })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FileTarget {
    pub(super) schema: String,
    pub(super) key: KeyValue,
    pub(super) expected_revision: Option<String>,
    #[serde(default)]
    pub(super) placement: Placement,
}

impl FileTarget {
    pub(super) fn parse(body: &[u8], route: &str) -> Result<Self, HostError> {
        let target: Self = serde_json::from_slice(body)
            .map_err(|error| HostError::new(400, format!("invalid {route} body: {error}")))?;
        if target.key.hash.as_deref().is_none_or(str::is_empty) {
            return Err(HostError::new(400, "a file route requires key.hash"));
        }
        Ok(target)
    }
}

pub(super) async fn execute_db_fetch_blob_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let read = async {
        let target = FileTarget::parse(&req.body, "/api/db/fetch-blob")?;
        let schema = schema_resolve::resolve_schema_name(host, &target.schema)?
            .ok_or_else(|| HostError::new(404, format!("schema '{}' not found", target.schema)))?;
        let rev = target.expected_revision.as_deref();
        let (res, missing) = match target.placement {
            Placement::Inline => (
                host.records()
                    .read_record_inline_file(&schema, &target.key, rev)
                    .await,
                "the record has no inline file",
            ),
            Placement::Blob => (
                host.records()
                    .read_record_file(&schema, &target.key, rev)
                    .await,
                "the record has no file",
            ),
        };
        res.map_err(HostError::from)?
            .ok_or_else(|| HostError::new(404, missing))
    };
    render_file(req, ctx, read.await)
}

pub(super) fn render_file(
    req: &UdsRequest,
    ctx: &AccessContext,
    file: Result<(FileMarker, Vec<u8>), HostError>,
) -> UdsResponse {
    let (marker, bytes) = match file {
        Ok(file) => file,
        Err(error) => return render(Err(error), ctx),
    };
    if is_media_type(req.header("accept"), "application/octet-stream") {
        return UdsResponse::new(200, "OK", bytes)
            .with_header("Content-Type", "application/octet-stream")
            .with_header("X-LastDB-File-Sha256", marker.sha256)
            .with_header("X-LastDB-File-Size", marker.size_bytes.to_string());
    }
    let mut file = serde_json::to_value(&marker).unwrap_or(Value::Null);
    if let Some(object) = file.as_object_mut() {
        object.insert(
            "bytes_b64".into(),
            Value::String(base64::engine::general_purpose::STANDARD.encode(&bytes)),
        );
    }
    json_ok(&envelope(
        &serde_json::json!({ "file": file }),
        ctx.user_id.as_str(),
    ))
}

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
    InlineFile::new(bytes, name, media_type).map_err(|msg| HostError::new(413, msg))
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
        let meta: PutBlobBody<'_> = serde_json::from_slice(json)
            .map_err(|e| HostError::new(400, format!("invalid file metadata: {e}")))?;
        if meta.bytes_b64.is_some() {
            return Err(HostError::new(
                400,
                "invalid file metadata: unknown field `bytes_b64`",
            ));
        }
        if meta.local_path.is_some() && !bytes.is_empty() {
            return Err(HostError::new(
                400,
                "a local file reference has an empty body",
            ));
        }
        return Ok((Cow::Borrowed(bytes), meta));
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
    let cap = base64::decoded_len_estimate(bytes_b64.len());
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(cap)
        .map_err(|_| HostError::new(503, "local file decode allocation refused"))?;
    bytes.resize(cap, 0);
    let n = base64::engine::general_purpose::STANDARD
        .decode_slice(bytes_b64.as_bytes(), &mut bytes)
        .map_err(|e| HostError::new(400, format!("bytes_b64 is invalid base64: {e}")))?;
    bytes.truncate(n);
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
    render(put_blob_inner(req, host).await, ctx)
}

async fn put_blob_inner(req: &UdsRequest, host: &Host) -> Result<Value, HostError> {
    let (plaintext, body) = parse_put_blob(req)?;
    let mut parsed = ParsedMutation::from_operation(body.mutation, None);
    if !matches!(
        parsed.mutation.mutation_type,
        MutationType::Create | MutationType::Replace
    ) {
        return Err(HostError::new(
            400,
            "put-blob requires create or complete replacement",
        ));
    }
    if let Some(path) = body.local_path {
        if body.placement == Placement::Inline {
            return Err(HostError::new(
                400,
                "an inline file stores its bytes; it has no local path",
            ));
        }
        return put_local_reference(
            host,
            parsed,
            &path,
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
        let file = inline_file(&plaintext, body.name, body.media_type)?;
        let marker = file.marker.clone();
        parsed.mutation.inline_write = Some(InlineWrite::Put(file));
        let value = mutation::commit_parsed_mutation(host, parsed).await?;
        return Ok(serde_json::json!({
            "file": marker, "placement": Placement::Inline,
            "mutation_id": value.get("mutation_id"), "record_revision": value.get("record_revision"),
        }));
    }
    let fw = host
        .records()
        .db_ops()
        .records()
        .seal_record_file(&plaintext, body.name, body.media_type)
        .map_err(HostError::from)?;
    let FileWrite::Put(file) = &fw else {
        return Err(HostError::new(500, "a sealed file is a put"));
    };
    let marker = file.marker().clone();
    let thumb = attach_blob_and_thumbnail(
        &mut parsed,
        &marker,
        fw,
        body.thumbnail,
        "file marker does not match the supplied bytes",
    )?;
    let value = mutation::commit_parsed_mutation(host, parsed).await?;
    Ok(serde_json::json!({
        "file": marker, "placement": body.placement, "thumbnail": thumb,
        "mutation_id": value.get("mutation_id"), "record_revision": value.get("record_revision"),
    }))
}

fn attach_blob_and_thumbnail(
    parsed: &mut ParsedMutation,
    marker: &FileMarker,
    file_write: FileWrite,
    thumbnail: Option<Thumbnail>,
    mismatch_msg: &'static str,
) -> Result<Option<FileMarker>, HostError> {
    if parsed.mutation.file.as_ref().is_some_and(|s| s != marker) {
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
    path: &str,
    (name, media_type, placement, thumbnail): (
        Option<String>,
        Option<String>,
        Placement,
        Option<Thumbnail>,
    ),
) -> Result<Value, HostError> {
    let fw = host
        .records()
        .db_ops()
        .records()
        .reference_local_file(Path::new(path), name, media_type)
        .await
        .map_err(HostError::from)?;
    let FileWrite::LocalPath(local) = &fw else {
        return Err(HostError::new(
            500,
            "a local file reference is a local path",
        ));
    };
    let (marker, source) = (local.marker().clone(), local.path().to_path_buf());
    let (schema, key) = (
        parsed.mutation.schema_name.clone(),
        parsed.mutation.key_value.clone(),
    );
    let thumb = attach_blob_and_thumbnail(
        &mut parsed,
        &marker,
        fw,
        thumbnail,
        "file marker does not match the local file",
    )?;
    let value = mutation::commit_parsed_mutation(host, parsed).await?;
    host.records()
        .remember_local_file(&schema, &key, &source, &marker)
        .await
        .map_err(HostError::from)?;
    Ok(serde_json::json!({
        "file": marker, "placement": placement, "local": true, "thumbnail": thumb,
        "mutation_id": value.get("mutation_id"), "record_revision": value.get("record_revision"),
    }))
}
