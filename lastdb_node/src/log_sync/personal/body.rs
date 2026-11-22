//! The accepted body inside a sealed entry, and the change it describes.
//!
//! The spool of accepted writes records each commit as one canonical JSON body:
//! a record write with its alternate keys, its file or inline file, or a schema
//! put or drop. The sender seals those bytes, and a receiving device opens them
//! and reads the same body here.

use std::collections::BTreeMap;

use fold_db::record_replacement::RecordTarget;
use fold_db::record_value::{FileMarker, InlineFile, InlineWrite};
use fold_db::schema::types::{CasExpectation, KeyValue, Mutation, MutationType, Schema};
use serde::Deserialize;
use serde_json::Value;

/// One accepted write after the envelope is open.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AcceptedBody {
    version: u16,
    pub(super) mutation_id: String,
    kind: String,
    schema: String,
    action: String,
    #[serde(default)]
    key: Option<KeyValue>,
    #[serde(default)]
    fields: Option<BTreeMap<String, Value>>,
    #[serde(default)]
    alternates: Option<BTreeMap<String, Option<KeyValue>>>,
    #[serde(default)]
    pub(super) file: Option<FileSection>,
    #[serde(default)]
    inline_file: Option<InlineSection>,
    #[serde(default)]
    definition_id: Option<String>,
    #[serde(default)]
    definition: Option<Value>,
    #[serde(default)]
    contribution: Option<Value>,
}

/// The external file of a record: its marker, or a clear.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FileSection {
    #[serde(default)]
    content_sha256: Option<String>,
    /// Hash of the sealed row in the sending device's store. It names nothing on
    /// another device, so it is read and dropped.
    #[serde(default)]
    sealed_sha256: Option<String>,
    #[serde(default)]
    size: Option<u64>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    media_type: Option<String>,
    pub(super) cleared: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InlineSection {
    cleared: bool,
    #[serde(default)]
    marker: Option<FileMarker>,
    #[serde(default)]
    data: Option<String>,
}

/// What one body changes.
pub(super) enum Change {
    Schema(Box<SchemaChange>),
    Record(Box<RecordChange>),
}

pub(super) struct SchemaChange {
    pub(super) name: String,
    pub(super) mutation_id: String,
    /// The definition of a put, or `None` for a drop.
    pub(super) definition: Option<Schema>,
}

pub(super) struct RecordChange {
    pub(super) mutation: Mutation,
    pub(super) file: RecordFile,
    pub(super) inline: Option<InlineWrite>,
}

/// What a record write does to the external file.
pub(super) enum RecordFile {
    /// The write does not touch the file.
    Keep,
    /// The write removes the file.
    Clear,
    /// The write sets a file. The bytes travel as a sealed blob.
    Put(FileMarker),
}

const BAD: &str = "a sealed entry holds a body that is not an accepted write";

/// Read a canonical body.
pub(super) fn parse(bytes: &[u8]) -> Result<AcceptedBody, String> {
    let body: AcceptedBody = serde_json::from_slice(bytes).map_err(|_| BAD.to_owned())?;
    if body.version != 1 || body.mutation_id.is_empty() {
        return Err(BAD.into());
    }
    Ok(body)
}

/// The change of a body: a schema change or a record write.
pub(super) fn classify(body: &AcceptedBody) -> Result<Change, String> {
    if body.schema.is_empty() || body.contribution.is_some() {
        return Err(BAD.into());
    }
    match body.kind.as_str() {
        "schema" => schema_change(body).map(|change| Change::Schema(Box::new(change))),
        "record" | "file" | "inline" => {
            record_change(body).map(|change| Change::Record(Box::new(change)))
        }
        _ => Err(BAD.into()),
    }
}

fn schema_change(body: &AcceptedBody) -> Result<SchemaChange, String> {
    if body.key.is_some()
        || body.fields.is_some()
        || body.alternates.is_some()
        || body.file.is_some()
        || body.inline_file.is_some()
        || body.definition_id.is_some()
    {
        return Err(BAD.into());
    }
    let definition = match (body.action.as_str(), &body.definition) {
        ("put", Some(definition)) => {
            let schema: Schema =
                serde_json::from_value(definition.clone()).map_err(|_| BAD.to_owned())?;
            if schema.name != body.schema {
                return Err(BAD.into());
            }
            Some(schema)
        }
        ("drop", None) => None,
        _ => return Err(BAD.into()),
    };
    Ok(SchemaChange {
        name: body.schema.clone(),
        mutation_id: body.mutation_id.clone(),
        definition,
    })
}

fn record_change(body: &AcceptedBody) -> Result<RecordChange, String> {
    if body.definition.is_some() {
        return Err(BAD.into());
    }
    let key = body.key.clone().ok_or(BAD)?;
    let fields = body.fields.clone().ok_or(BAD)?;
    let alternates = body.alternates.clone().ok_or(BAD)?;
    let action = match body.action.as_str() {
        "create" => MutationType::Create,
        "replace" => MutationType::Replace,
        "delete" => MutationType::Delete,
        "purge" => MutationType::Purge,
        _ => return Err(BAD.into()),
    };
    let file = record_file(body)?;
    let inline = record_inline(body, &file)?;
    let removal = matches!(action, MutationType::Delete | MutationType::Purge);
    if removal && (!fields.is_empty() || !matches!(file, RecordFile::Keep) || inline.is_some()) {
        return Err(BAD.into());
    }
    let target = RecordTarget {
        schema: body.schema.clone(),
        key,
        alternates,
    };
    let mut mutation = Mutation::with_uuid(
        body.mutation_id.clone(),
        target,
        fields.into_iter().collect(),
        action,
        CasExpectation::Absent,
    );
    mutation.definition_id.clone_from(&body.definition_id);
    Ok(RecordChange {
        mutation,
        file,
        inline,
    })
}

fn record_file(body: &AcceptedBody) -> Result<RecordFile, String> {
    let Some(section) = &body.file else {
        return if body.kind == "file" {
            Err(BAD.into())
        } else {
            Ok(RecordFile::Keep)
        };
    };
    if section.cleared {
        let bare = section.content_sha256.is_none()
            && section.sealed_sha256.is_none()
            && section.size.is_none()
            && section.name.is_none()
            && section.media_type.is_none();
        return if bare && body.kind != "file" {
            Ok(RecordFile::Clear)
        } else {
            Err(BAD.into())
        };
    }
    if body.kind != "file" {
        return Err(BAD.into());
    }
    let marker = FileMarker {
        name: section.name.clone(),
        media_type: section.media_type.clone(),
        size_bytes: section.size.ok_or(BAD)?,
        sha256: section.content_sha256.clone().ok_or(BAD)?,
    };
    if marker.has_valid_hash()
        && marker.size_bytes <= fold_db::storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES
    {
        Ok(RecordFile::Put(marker))
    } else {
        Err(BAD.into())
    }
}

fn record_inline(body: &AcceptedBody, file: &RecordFile) -> Result<Option<InlineWrite>, String> {
    let Some(section) = &body.inline_file else {
        return if body.kind == "inline" {
            Err(BAD.into())
        } else {
            Ok(None)
        };
    };
    if body.kind == "record" {
        return Err(BAD.into());
    }
    if section.cleared {
        return if section.marker.is_none() && section.data.is_none() {
            Ok(Some(InlineWrite::Clear))
        } else {
            Err(BAD.into())
        };
    }
    if matches!(file, RecordFile::Put(_)) {
        return Err(BAD.into());
    }
    let marker = section.marker.clone().ok_or(BAD)?;
    let data = section.data.clone().ok_or(BAD)?;
    let inline = InlineFile { marker, data };
    inline.bytes().map_err(|_| BAD.to_owned())?;
    Ok(Some(InlineWrite::Put(inline)))
}
