//! The accepted body inside a sealed entry, and the change it describes.
//!
//! Each commit is recorded in its sealed entry as one canonical JSON body: a
//! record write with its alternate keys, its file and/or inline file, or a
//! schema put or drop. A receiving device opens the envelope and reads that
//! body here.

use fold_db::record_replacement::RecordTarget;
use fold_db::record_value::{FileMarker, InlineFile, InlineWrite};
use fold_db::schema::types::{CasExpectation, Mutation, MutationType, Schema};

type AcceptedBody = fold_db::storage::disk_backup::accepted::AcceptedBody<'static>;

/// What one body changes.
pub(crate) enum Change {
    Schema(Box<SchemaChange>),
    Record(Box<RecordChange>),
}

pub(crate) struct SchemaChange {
    pub(crate) name: String,
    /// The definition of a put, or `None` for a drop.
    pub(crate) definition: Option<Schema>,
}

pub(crate) struct RecordChange {
    pub(crate) mutation: Mutation,
    pub(crate) file: RecordFile,
    pub(crate) inline: Option<InlineWrite>,
}

/// What a record write does to the external file.
pub(crate) enum RecordFile {
    /// The write does not touch the file.
    Keep,
    /// The write removes the file.
    Clear,
    /// The write sets a file. The bytes travel as a sealed blob.
    Put(FileMarker),
}

const BAD: &str = "a sealed entry holds a body that is not an accepted write";

/// Read a canonical body.
pub(crate) fn parse(bytes: &[u8]) -> Result<AcceptedBody, String> {
    let body: AcceptedBody = serde_json::from_slice(bytes).map_err(|_| BAD.to_owned())?;
    if body.version != 1 || body.mutation_id.is_empty() {
        return Err(BAD.into());
    }
    Ok(body)
}

/// The change of a body: a schema change or a record write.
pub(crate) fn classify(body: &AcceptedBody) -> Result<Change, String> {
    if body.schema.is_empty() || body.contribution.is_some() {
        return Err(BAD.into());
    }
    match body.kind.as_ref() {
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
    let definition = match (body.action.as_ref(), &body.definition) {
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
        name: body.schema.to_string(),
        definition,
    })
}

fn record_change(body: &AcceptedBody) -> Result<RecordChange, String> {
    if body.definition.is_some() {
        return Err(BAD.into());
    }
    let key = body.key.as_deref().ok_or(BAD)?.clone();
    let fields = body.fields.as_deref().ok_or(BAD)?.clone();
    let alternates = body.alternates.as_deref().ok_or(BAD)?.clone();
    let action = match body.action.as_ref() {
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
        schema: body.schema.to_string(),
        key,
        alternates,
    };
    let mut mutation = Mutation::with_uuid(
        body.mutation_id.to_string(),
        target,
        fields.into_iter().collect(),
        action,
        CasExpectation::Absent,
    );
    mutation.definition_id = body.definition_id.as_deref().map(str::to_owned);
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
        name: section.name.as_deref().map(str::to_owned),
        media_type: section.media_type.as_deref().map(str::to_owned),
        size_bytes: section.size.ok_or(BAD)?,
        sha256: section.content_sha256.as_deref().ok_or(BAD)?.to_owned(),
    };
    if marker.has_valid_hash()
        && marker.size_bytes <= fold_db::storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES
    {
        Ok(RecordFile::Put(marker))
    } else {
        Err(BAD.into())
    }
}

fn record_inline(body: &AcceptedBody, _file: &RecordFile) -> Result<Option<InlineWrite>, String> {
    let Some(section) = &body.inline_file else {
        return if body.kind == "inline" {
            Err(BAD.into())
        } else {
            Ok(None)
        };
    };
    // Inline may ride with a file write (kind "file") or stand alone (kind
    // "inline"). A plain record write does not carry an inline section.
    match body.kind.as_ref() {
        "file" | "inline" => {}
        _ => return Err(BAD.into()),
    }
    if section.cleared {
        return if section.marker.is_none() && section.data.is_none() {
            Ok(Some(InlineWrite::Clear))
        } else {
            Err(BAD.into())
        };
    }
    let marker = section.marker.as_deref().ok_or(BAD)?.clone();
    let data = section.data.as_deref().ok_or(BAD)?.to_owned();
    let inline = InlineFile { marker, data };
    inline.bytes().map_err(|_| BAD.to_owned())?;
    Ok(Some(InlineWrite::Put(inline)))
}

/// Project an accepted record for the grant and proposal history while it tracks record winners.
pub(crate) fn history_record(plain: &[u8]) -> Option<(String, Vec<u8>)> {
    let Change::Record(change) = classify(&parse(plain).ok()?).ok()? else {
        return None;
    };
    let m = change.mutation;
    let key =
        mutation_log::join_record_key(m.key_value.hash.as_deref()?, m.key_value.range.as_deref())?;
    let value = if matches!(m.mutation_type, MutationType::Delete | MutationType::Purge) {
        Vec::new()
    } else {
        let file = match change.file {
            RecordFile::Keep => signed_history_materialize::FileSection::Keep,
            RecordFile::Clear => signed_history_materialize::FileSection::Clear,
            RecordFile::Put(marker) => signed_history_materialize::FileSection::Put(marker),
        };
        signed_history_materialize::encode_record_value(
            &serde_json::to_value(&m.record_fields).ok()?,
            &m.alternates,
            &file,
            change.inline.as_ref(),
        )
        .ok()?
    };
    Some((
        m.schema_name,
        mutation_log::encode_record_payload(&key, &value),
    ))
}

pub(crate) fn history_schema(plain: &[u8]) -> Option<(String, Vec<u8>)> {
    let mut body = parse(plain).ok()?;
    let Change::Schema(_) = classify(&body).ok()? else {
        return None;
    };
    Some((
        body.schema.into_owned(),
        serde_json::to_vec(&body.definition.take()?).ok()?,
    ))
}
