//! Put, fetch, and delete routes for a record's blob, inline, or local-reference file.

use std::borrow::Cow;
use std::path::{Path, PathBuf};

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
    header.is_some_and(|v| {
        v.split(',').any(|i| {
            i.split(';')
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
        let t: Self = serde_json::from_slice(body)
            .map_err(|e| HostError::new(400, format!("invalid {route} body: {e}")))?;
        if t.key.hash.as_deref().is_none_or(str::is_empty) {
            return Err(HostError::new(400, "a file route requires key.hash"));
        }
        Ok(t)
    }
}

pub(super) async fn db_fetch_blob(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let read = async {
        let t = FileTarget::parse(&req.body, "/api/db/fetch-blob")?;
        let s = schema_resolve::resolve_schema_name(host, &t.schema)?
            .ok_or_else(|| HostError::new(404, format!("schema '{}' not found", t.schema)))?;
        let rev = t.expected_revision.as_deref();
        match t.placement {
            Placement::Inline => host
                .records()
                .read_record_inline_file(&s, &t.key, rev)
                .await?
                .ok_or_else(|| HostError::new(404, "the record has no inline file")),
            Placement::Blob => host
                .records()
                .read_record_file(&s, &t.key, rev)
                .await?
                .ok_or_else(|| HostError::new(404, "the record has no file")),
        }
    };
    render_file(req, ctx, read.await)
}

pub(super) fn render_file(
    req: &UdsRequest,
    ctx: &AccessContext,
    file: Result<(FileMarker, Vec<u8>), HostError>,
) -> UdsResponse {
    let (marker, bytes) = match file {
        Ok(f) => f,
        Err(e) => return render(Err(e), ctx),
    };
    if is_media_type(req.header("accept"), "application/octet-stream") {
        return UdsResponse::new(200, "OK", bytes)
            .with_header("Content-Type", "application/octet-stream")
            .with_header("X-LastDB-File-Sha256", marker.sha256)
            .with_header("X-LastDB-File-Size", marker.size_bytes.to_string());
    }
    let mut file = serde_json::to_value(&marker).unwrap_or(Value::Null);
    if let Some(obj) = file.as_object_mut() {
        obj.insert(
            "bytes_b64".into(),
            Value::String(base64::engine::general_purpose::STANDARD.encode(&bytes)),
        );
    }
    json_ok(&envelope(&json!({ "file": file }), ctx.user_id.as_str()))
}

pub(super) async fn db_delete_blob(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    if !ctx.is_owner {
        return error_response(403, "file delete is owner-only", ctx);
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Body {
        mutation: Operation,
        #[serde(default)]
        placement: Placement,
    }
    let res = async {
        let b: Body = serde_json::from_slice(&req.body)
            .map_err(|e| HostError::new(400, format!("invalid delete-blob request: {e}")))?;
        let mut p = ParsedMutation::from_operation(b.mutation, None);
        if p.mutation.mutation_type != MutationType::Replace {
            return Err(HostError::new(
                400,
                "delete-blob requires a complete replacement",
            ));
        }
        match b.placement {
            Placement::Inline => p.mutation.inline_write = Some(InlineWrite::Clear),
            Placement::Blob if p.mutation.file.is_some() => {
                return Err(HostError::new(
                    400,
                    "delete-blob requires a complete replacement without a file marker",
                ));
            }
            Placement::Blob => p.mutation.file_write = Some(FileWrite::Clear),
        }
        mutation::commit_parsed_mutation(host, p).await
    }
    .await;
    render(res, ctx)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Thumbnail {
    bytes_b64: String,
    name: Option<String>,
    media_type: Option<String>,
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
        let len = req
            .header("x-lastdb-file-blob-metadata-length")
            .ok_or_else(|| HostError::new(400, "a raw put-blob requires the metadata length"))?
            .parse::<usize>()
            .ok()
            .filter(|&n| n <= max_len)
            .ok_or_else(|| HostError::new(400, "invalid file metadata length"))?;
        let (json, bytes) = req
            .body
            .split_at_checked(len)
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
    let b64 = body
        .bytes_b64
        .take()
        .ok_or_else(|| HostError::new(400, "put-blob requires bytes_b64 or local_path"))?;
    let cap = base64::decoded_len_estimate(b64.len());
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(cap)
        .map_err(|_| HostError::new(503, "local file decode allocation refused"))?;
    bytes.resize(cap, 0);
    let n = base64::engine::general_purpose::STANDARD
        .decode_slice(b64.as_bytes(), &mut bytes)
        .map_err(|e| HostError::new(400, format!("bytes_b64 is invalid base64: {e}")))?;
    bytes.truncate(n);
    Ok((Cow::Owned(bytes), body))
}

pub(super) async fn db_put_blob(req: &UdsRequest, ctx: &AccessContext, host: &Host) -> UdsResponse {
    if !ctx.is_owner {
        return error_response(403, "file put is owner-only", ctx);
    }
    render(put_blob_inner(req, host).await, ctx)
}

async fn prepare_blob_write(
    host: &Host,
    plaintext: &[u8],
    local_path: Option<&str>,
    name: Option<String>,
    media_type: Option<String>,
) -> Result<(FileWrite, FileMarker, Option<PathBuf>, &'static str), HostError> {
    let records = host.records();
    let recs = records.db_ops().records();
    if let Some(path) = local_path {
        let fw = recs
            .reference_local_file(Path::new(path), name, media_type)
            .await?;
        let FileWrite::LocalPath(l) = &fw else {
            return Err(HostError::internal(
                "a local file reference is a local path",
            ));
        };
        let (m, src) = (l.marker().clone(), Some(l.path().to_path_buf()));
        Ok((fw, m, src, "file marker does not match the local file"))
    } else {
        let fw = recs.seal_record_file(plaintext, name, media_type)?;
        let FileWrite::Put(f) = &fw else {
            return Err(HostError::internal("a sealed file is a put"));
        };
        let m = f.marker().clone();
        Ok((fw, m, None, "file marker does not match the supplied bytes"))
    }
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
    if body.placement == Placement::Inline {
        if body.local_path.is_some() {
            return Err(HostError::new(
                400,
                "an inline file stores its bytes; it has no local path",
            ));
        }
        if body.thumbnail.is_some() {
            return Err(HostError::new(
                400,
                "an inline put takes no thumbnail; the bytes are the inline file",
            ));
        }
        let f = InlineFile::new(&plaintext, body.name, body.media_type)
            .map_err(|m| HostError::new(413, m))?;
        let m = f.marker.clone();
        parsed.mutation.inline_write = Some(InlineWrite::Put(f));
        let val = mutation::commit_parsed_mutation(host, parsed).await?;
        return Ok(json!({
            "file": m, "placement": Placement::Inline,
            "mutation_id": val.get("mutation_id"), "record_revision": val.get("record_revision"),
        }));
    }
    let (fw, marker, local_src, mismatch) = prepare_blob_write(
        host,
        &plaintext,
        body.local_path.as_deref(),
        body.name,
        body.media_type,
    )
    .await?;
    if parsed.mutation.file.as_ref().is_some_and(|s| s != &marker) {
        return Err(HostError::new(400, mismatch));
    }
    parsed.mutation.file = Some(marker.clone());
    parsed.mutation.file_write = Some(fw);
    let mut thumb = None;
    if let Some(t) = body.thumbnail {
        let b = base64::engine::general_purpose::STANDARD
            .decode(t.bytes_b64.as_bytes())
            .map_err(|e| {
                HostError::new(400, format!("thumbnail.bytes_b64 is invalid base64: {e}"))
            })?;
        let f = InlineFile::new(&b, t.name, t.media_type).map_err(|m| HostError::new(413, m))?;
        thumb = Some(f.marker.clone());
        parsed.mutation.inline_write = Some(InlineWrite::Put(f));
    }
    let (schema, key) = (
        parsed.mutation.schema_name.clone(),
        parsed.mutation.key_value.clone(),
    );
    let val = mutation::commit_parsed_mutation(host, parsed).await?;
    let mut out = json!({
        "file": marker, "placement": body.placement, "thumbnail": thumb,
        "mutation_id": val.get("mutation_id"), "record_revision": val.get("record_revision"),
    });
    if let Some(src) = local_src {
        host.records()
            .remember_local_file(&schema, &key, &src, &marker)
            .await?;
        out["local"] = Value::Bool(true);
    }
    Ok(out)
}
