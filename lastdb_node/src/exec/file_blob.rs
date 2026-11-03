//! The file routes of a record: read and remove one record's file by schema
//! and key (`put-blob` is in `file_blob_put.rs`). No request names a pointer,
//! a path or a key: the node derives them from the record.
use super::*;
use fold_db::fold_db_core::fold_db::RecordFileState;
use fold_db::schema::types::operations::{MutationConvergence, MutationDurability};
use fold_db::schema::types::{FileWrite, KeyValue, MutationType};

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

/// The record a file route names: its schema and its key.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FileTarget {
    pub(super) schema: String,
    pub(super) key: KeyValue,
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

/// The native write of a file route: an update of the record at `key` that
/// carries only the file write. The record's key fields are written as for
/// any mutation.
pub(super) fn file_mutation(
    schema: String,
    key: KeyValue,
    fields: HashMap<String, Value>,
    file_write: FileWrite,
    must_exist: Option<bool>,
) -> handlers::MutationComponents {
    handlers::MutationComponents {
        metadata: None,
        schema,
        fields_and_values: fields,
        key_value: key,
        mutation_type: MutationType::Update,
        source_file_name: None,
        expected: None,
        convergence: MutationConvergence::default(),
        durability: MutationDurability::Durable,
        must_exist,
        key_range_prefix: None,
        file_write: Some(file_write),
    }
}

/// `POST /api/db/fetch-blob` — the file of one record, checked against the
/// record's marker. OWNER socket only.
///
/// JSON clients receive `{ file: { name?, media_type?, size_bytes, sha256,
/// bytes_b64 } }`. Clients that send `Accept: application/octet-stream`
/// receive the plaintext bytes, with the hash and size in headers.
pub(super) async fn execute_db_fetch_blob_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    let read = async {
        let target = FileTarget::parse(&req.body, "/api/db/fetch-blob")?;
        let schema = target.canonical(host)?;
        host.db
            .read_record_file(&schema, &target.key)
            .await
            .map_err(HostError::from)?
            .ok_or_else(|| HostError::new(404, "the record has no file"))
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
/// in one native write. The record stays. OWNER socket only.
pub(super) async fn execute_db_delete_blob_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    if !ctx.is_owner {
        return error_response(403, "file delete is owner-only", ctx);
    }
    let delete = async {
        let target = FileTarget::parse(&req.body, "/api/db/delete-blob")?;
        let schema = target.canonical(host)?;
        let (_, state) = host
            .db
            .record_file_state(&schema, &target.key)
            .await
            .map_err(HostError::from)?;
        match state {
            RecordFileState::NoRecord => Err(HostError::new(404, "record not found")),
            RecordFileState::NoFile => Ok(serde_json::json!({ "deleted": false })),
            RecordFileState::File(_) => {
                let components = file_mutation(
                    schema,
                    target.key,
                    HashMap::new(),
                    FileWrite::Clear,
                    Some(true),
                );
                let value = commit_mutation_components(host, components).await?;
                Ok(serde_json::json!({
                    "deleted": true,
                    "mutation_id": value.get("mutation_id").cloned().unwrap_or(Value::Null),
                }))
            }
        }
    };
    render(delete.await, ctx)
}
