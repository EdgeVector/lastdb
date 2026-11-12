//! Fetch and delete routes for a record's blob or inline file (`put-blob` is in `file_blob_put.rs`).

use super::*;
use fold_db::record_value::InlineWrite;
use fold_db::schema::types::operations::Operation;
use fold_db::schema::types::{FileWrite, KeyValue, MutationType};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Placement {
    #[default]
    Blob,
    Inline,
}

pub(super) fn is_media_type(header: Option<&str>, expected: &str) -> bool {
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
        let (result, missing) = match target.placement {
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
        result
            .map_err(HostError::from)?
            .ok_or_else(|| HostError::new(404, missing))
    };
    render_file(req, ctx, read.await)
}

pub(super) fn render_file(
    req: &UdsRequest,
    ctx: &AccessContext,
    file: Result<(fold_db::record_value::FileMarker, Vec<u8>), HostError>,
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
        let body: Body = serde_json::from_slice(&req.body)
            .map_err(|e| HostError::new(400, format!("invalid delete-blob request: {e}")))?;
        let mut parsed = ParsedMutation::from_operation(body.mutation, None);
        if parsed.mutation.mutation_type != MutationType::Replace {
            return Err(HostError::new(
                400,
                "delete-blob requires a complete replacement",
            ));
        }
        match body.placement {
            Placement::Inline => parsed.mutation.inline_write = Some(InlineWrite::Clear),
            Placement::Blob if parsed.mutation.file.is_some() => {
                return Err(HostError::new(
                    400,
                    "delete-blob requires a complete replacement without a file marker",
                ));
            }
            Placement::Blob => parsed.mutation.file_write = Some(FileWrite::Clear),
        }
        mutation::commit_parsed_mutation(host, parsed).await
    };
    render(delete.await, ctx)
}
