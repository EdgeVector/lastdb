//! Canonical plaintext inside one `log_envelope`.
//!
//! E5's accepted stream seals this body. E publishes those same bytes.
//! Empty optional fields are omitted. The cloud file section omits the
//! local sealed-row hash. E sets `object` only after the upload receipt.
//! A schema id is `s` plus 40 hex characters of SHA-256(`name` || 0x00 ||
//! tag). The tag is the definition identity hash, `none`, or `drop`.

use std::collections::BTreeMap;

use cloud_backup_protocol::canonical_bytes;
use cloud_backup_protocol::v2::blobs::BlobPath;
use cloud_backup_protocol::CloudScope;
use fold_db::record_value::{FileMarker, InlineWrite};
use fold_db::schema::types::{KeyValue, Mutation, Schema};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::accepted_record;
use super::RestoreError;

const HEX: &[u8; 16] = b"0123456789abcdef";

/// One accepted mutation after the envelope is open.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptedBody {
    /// Body version. Only `1` is accepted.
    pub version: u16,
    /// Durable mutation id. A schema id starts with `s`.
    pub mutation_id: String,
    /// `record`, `file`, `inline`, `schema`, or `contribution`.
    pub kind: String,
    /// `namespace/Name`.
    pub schema: String,
    /// `create`, `replace`, `delete`, `purge`, `put`, or `drop`.
    pub action: String,
    /// Record key. Absent on a schema change.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<KeyValue>,
    /// Record fields. An empty object is present when the record has none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fields: Option<BTreeMap<String, Value>>,
    /// Alternate keys. A null value clears that alternate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alternates: Option<BTreeMap<String, Option<KeyValue>>>,
    /// External file marker or clear. A sealed row is not these bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<FileSection>,
    /// Inline file, at most 64 KiB, standard base64.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inline_file: Option<InlineSection>,
    /// Schema definition id carried by a record or contribution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub definition_id: Option<String>,
    /// Schema wire JSON. Present only on schema put.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub definition: Option<Value>,
    /// Share contribution admission. Present only for that kind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contribution: Option<ContributionSection>,
}

/// External file section of one cloud accepted body.
///
/// The local sealed-row hash is not a field. The publisher sets `object`
/// only after the ordinary upload receipt. A content hash without `object`
/// is not a restored file.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileSection {
    /// SHA-256 of the plaintext. Absent on a clear.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_sha256: Option<String>,
    /// Plaintext size. Absent on a clear.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    /// File name, when the marker has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Media type, when the marker has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    /// True when the mutation clears the external file.
    pub cleared: bool,
    /// Uploaded envelope address. Absent on a clear and on a local marker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object: Option<FileObject>,
}

/// Authenticated address of one uploaded file envelope.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileObject {
    /// Publisher account that owns the upload receipt.
    pub scope: CloudScope,
    /// Blob path written on that receipt.
    pub path: BlobPath,
}

/// Inline file section of an accepted body.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InlineSection {
    /// True when the mutation clears the inline file.
    pub cleared: bool,
    /// Marker for a put. Absent on a clear.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub marker: Option<FileMarker>,
    /// Standard base64 plaintext. Absent on a clear.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,
}

/// Contribution admission carried in the accepted body.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContributionSection {
    /// Installed schema definition id.
    pub definition_id: String,
    /// Contributor writer id.
    pub writer: String,
    /// SHA-256 of the contribution receipt.
    pub receipt_sha256: String,
    /// SHA-256 of the contribution intent.
    pub intent_sha256: String,
}

pub(super) enum Change {
    Schema(Box<SchemaChange>),
    Record(Box<RecordChange>),
}

pub(super) struct SchemaChange {
    pub name: String,
    pub definition: Option<Schema>,
}

pub(super) struct RecordChange {
    pub mutation: Mutation,
    pub file: RecordFile,
    pub inline: Option<InlineWrite>,
}

pub(super) enum RecordFile {
    Keep,
    Clear,
    Fetch(Box<FileFetch>),
}

pub(super) struct FileFetch {
    pub marker: FileMarker,
    pub object: FileObject,
}

/// Parse one canonical accepted body. The bytes must already be open.
pub fn parse_accepted(bytes: &[u8]) -> Result<AcceptedBody, RestoreError> {
    let body: AcceptedBody =
        serde_json::from_slice(bytes).map_err(|_| RestoreError::Entry("snapshot entry"))?;
    let again = canonical_bytes(&body).map_err(|_| RestoreError::Entry("snapshot entry"))?;
    if again != bytes || body.version != 1 {
        return Err(RestoreError::Entry("snapshot entry"));
    }
    if mutation_label(&body.mutation_id) {
        Ok(body)
    } else {
        Err(RestoreError::Entry("mutation id"))
    }
}

pub(super) fn classify(body: &AcceptedBody, database_id: &str) -> Result<Change, RestoreError> {
    match body.kind.as_str() {
        "schema" => Ok(Change::Schema(Box::new(schema_change(body)?))),
        "record" | "file" | "inline" | "contribution" => Ok(Change::Record(Box::new(
            accepted_record::record_change(body, database_id)?,
        ))),
        _ => Err(RestoreError::Entry("snapshot entry")),
    }
}

/// Marker names: 1 to 128 bytes of letters, digits, `-`, or `_`.
pub(super) fn mutation_label(value: &str) -> bool {
    (1..=128).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn schema_change(body: &AcceptedBody) -> Result<SchemaChange, RestoreError> {
    if body.key.is_some()
        || body.fields.is_some()
        || body.alternates.is_some()
        || body.file.is_some()
        || body.inline_file.is_some()
        || body.contribution.is_some()
        || body.definition_id.is_some()
        || !qualified(&body.schema)
    {
        return Err(RestoreError::Entry("snapshot entry"));
    }
    match (body.action.as_str(), &body.definition) {
        ("put", Some(definition)) => put_schema(body, definition),
        ("drop", None) => drop_schema(body),
        _ => Err(RestoreError::Entry("snapshot entry")),
    }
}

fn put_schema(body: &AcceptedBody, definition: &Value) -> Result<SchemaChange, RestoreError> {
    let schema: Schema = serde_json::from_value(definition.clone())
        .map_err(|_| RestoreError::Entry("snapshot entry"))?;
    if schema.name != body.schema {
        return Err(RestoreError::Entry("snapshot entry"));
    }
    let tag = schema.get_identity_hash().map_or("none", String::as_str);
    if schema_id(&body.schema, tag) != body.mutation_id {
        return Err(RestoreError::Entry("mutation id"));
    }
    Ok(SchemaChange {
        name: body.schema.clone(),
        definition: Some(schema),
    })
}

fn drop_schema(body: &AcceptedBody) -> Result<SchemaChange, RestoreError> {
    if schema_id(&body.schema, "drop") != body.mutation_id {
        return Err(RestoreError::Entry("mutation id"));
    }
    Ok(SchemaChange {
        name: body.schema.clone(),
        definition: None,
    })
}

fn schema_id(name: &str, tag: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(name.as_bytes());
    hasher.update([0]);
    hasher.update(tag.as_bytes());
    let digest = hasher.finalize();
    let mut id = String::with_capacity(41);
    id.push('s');
    for byte in digest.iter().take(20) {
        id.push(HEX[(byte >> 4) as usize] as char);
        id.push(HEX[(byte & 0x0f) as usize] as char);
    }
    id
}

pub(super) fn qualified(schema: &str) -> bool {
    match schema.split_once('/') {
        Some((namespace, name)) => !namespace.is_empty() && !name.is_empty(),
        None => false,
    }
}
