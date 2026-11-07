//! The file routes of a record: read and remove one record's file by schema
//! and key (`put-blob` is in `file_blob_put.rs`). No request names a pointer,
//! a path or a key: the node derives them from the record.
use super::*;
use fold_db::record_value::InlineWrite;
use fold_db::schema::types::operations::Operation;
use fold_db::schema::types::{FileWrite, KeyValue, MutationType};

/// Which file of a record a route acts on. A record can own one blob file
/// (B2) and one inline file kept in the record value (R2).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Placement {
    #[default]
    Blob,
    Inline,
}

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

/// The record a file route names: its schema and its key.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FileTarget {
    pub(super) schema: String,
    pub(super) key: KeyValue,
    expected_revision: Option<String>,
    #[serde(default)]
    placement: Placement,
}

impl FileTarget {
    fn parse(body: &[u8], route: &str) -> Result<Self, HostError> {
        let target: Self = serde_json::from_slice(body)
            .map_err(|error| HostError::new(400, format!("invalid {route} body: {error}")))?;
        target.require_hash()?;
        Ok(target)
    }

    pub(super) fn require_hash(&self) -> Result<(), HostError> {
        if self.key.hash.as_deref().is_none_or(str::is_empty) {
            return Err(HostError::new(400, "a file route requires key.hash"));
        }
        Ok(())
    }

    /// The canonical schema name.
    pub(super) fn canonical(&self, host: &Host) -> Result<String, HostError> {
        handlers::resolve_schema_name(host, &self.schema)?
            .ok_or_else(|| HostError::new(404, format!("schema '{}' not found", self.schema)))
    }
}

/// `POST /api/db/fetch-blob` — the file of one record, checked against the
/// record's marker. OWNER socket only.
///
/// JSON clients receive `{ file: { name?, media_type?, size_bytes, sha256,
/// bytes_b64 } }`. `placement: "inline"` reads the record's inline file. Clients that send `Accept: application/octet-stream`
/// receive the plaintext bytes, with the hash and size in headers.
pub(super) async fn execute_db_fetch_blob_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let read = async {
        let target = FileTarget::parse(&req.body, "/api/db/fetch-blob")?;
        let schema = target.canonical(host)?;
        let revision = target.expected_revision.as_deref();
        let result = match (target.placement, revision) {
            (Placement::Inline, revision) => {
                host.db
                    .read_record_inline_file(&schema, &target.key, revision)
                    .await
            }
            (Placement::Blob, Some(revision)) => {
                host.db
                    .read_record_file_at_revision(&schema, &target.key, revision)
                    .await
            }
            (Placement::Blob, None) => host.db.read_record_file(&schema, &target.key).await,
        };
        result.map_err(HostError::from)?.ok_or_else(|| {
            HostError::new(
                404,
                match target.placement {
                    Placement::Blob => "the record has no file",
                    Placement::Inline => "the record has no inline file",
                },
            )
        })
    };
    let (marker, bytes) = match read.await {
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

/// `POST /api/db/delete-blob` — remove the file and the marker of one record
/// in one native write. The record stays. `placement: "inline"` removes the
/// inline file and keeps the blob file. OWNER socket only.
pub(super) async fn execute_db_delete_blob_route(
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
    let delete = async {
        let body: Body = serde_json::from_slice(&req.body).map_err(|error| {
            HostError::new(400, format!("invalid delete-blob request: {error}"))
        })?;
        let mut components = handlers::MutationComponents::from_operation(body.mutation, None);
        if components.mutation_type != MutationType::Replace {
            return Err(HostError::new(
                400,
                "delete-blob requires a complete replacement",
            ));
        }
        match body.placement {
            Placement::Inline => components.inline_write = Some(InlineWrite::Clear),
            Placement::Blob if components.file.is_some() => {
                return Err(HostError::new(
                    400,
                    "delete-blob requires a complete replacement without a file marker",
                ));
            }
            Placement::Blob => components.file_write = Some(FileWrite::Clear),
        }
        commit_mutation_components(host, components).await
    };
    render(delete.await, ctx)
}
