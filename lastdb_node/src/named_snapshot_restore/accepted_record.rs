//! Record, file, inline, and contribution changes inside one accepted body.
//!
//! A file clear stays on kind `record` or `inline`. Kind `file` names an
//! uploaded object. A content hash without that object is not a present
//! file. An inline put together with an uploaded file is refused.

use std::collections::{BTreeMap, HashMap};

use fold_db::record_value::{FileMarker, InlineFile, InlineWrite};
use fold_db::schema::types::mutation::ContributionAdmission;
use fold_db::schema::types::{KeyValue, Mutation, MutationType};
use serde_json::Value;

use super::payload::{
    self, AcceptedBody, FileFetch, FileObject, FileSection, RecordChange, RecordFile,
};
use super::RestoreError;

pub(super) fn record_change(
    body: &AcceptedBody,
    database_id: &str,
) -> Result<RecordChange, RestoreError> {
    if body.definition.is_some() || !payload::qualified(&body.schema) {
        return Err(RestoreError::Entry("snapshot entry"));
    }
    let key = body
        .key
        .clone()
        .ok_or(RestoreError::Entry("snapshot entry"))?;
    let fields = body
        .fields
        .clone()
        .ok_or(RestoreError::Entry("snapshot entry"))?;
    let alternates = body
        .alternates
        .clone()
        .ok_or(RestoreError::Entry("snapshot entry"))?;
    let mutation_type = record_action(&body.action)?;
    let file = record_file(body, database_id)?;
    let inline = record_inline(body, &file)?;
    let contribution = record_contribution(body)?;
    if matches!(mutation_type, MutationType::Delete | MutationType::Purge)
        && (!fields.is_empty() || !matches!(file, RecordFile::Keep) || inline.is_some())
    {
        return Err(RestoreError::Entry("snapshot entry"));
    }
    Ok(RecordChange {
        mutation: mutation_from(body, key, fields, alternates, mutation_type, contribution),
        file,
        inline,
    })
}

fn record_action(action: &str) -> Result<MutationType, RestoreError> {
    match action {
        "create" => Ok(MutationType::Create),
        "replace" => Ok(MutationType::Replace),
        "delete" => Ok(MutationType::Delete),
        "purge" => Ok(MutationType::Purge),
        _ => Err(RestoreError::Entry("snapshot entry")),
    }
}

fn record_file(body: &AcceptedBody, database_id: &str) -> Result<RecordFile, RestoreError> {
    let Some(section) = &body.file else {
        return if body.kind == "file" {
            Err(RestoreError::Entry("snapshot entry"))
        } else {
            Ok(RecordFile::Keep)
        };
    };
    if section.cleared {
        cleared_file(body, section)
    } else {
        uploaded_file(body, section, database_id)
    }
}

fn cleared_file(body: &AcceptedBody, section: &FileSection) -> Result<RecordFile, RestoreError> {
    if body.kind == "file"
        || section.object.is_some()
        || section.content_sha256.is_some()
        || section.size.is_some()
        || section.name.is_some()
        || section.media_type.is_some()
    {
        Err(RestoreError::Entry("snapshot file"))
    } else {
        Ok(RecordFile::Clear)
    }
}

fn uploaded_file(
    body: &AcceptedBody,
    section: &FileSection,
    database_id: &str,
) -> Result<RecordFile, RestoreError> {
    if matches!(body.kind.as_str(), "record" | "inline") {
        return Err(RestoreError::Entry("snapshot entry"));
    }
    let marker = file_marker(section)?;
    let Some(object) = section.object.clone() else {
        return Err(RestoreError::File("snapshot file"));
    };
    if !same_database(&object, database_id) {
        return Err(RestoreError::File("snapshot file"));
    }
    Ok(RecordFile::Fetch(FileFetch { marker, object }))
}

fn file_marker(section: &FileSection) -> Result<FileMarker, RestoreError> {
    let sha256 = section
        .content_sha256
        .clone()
        .ok_or(RestoreError::Entry("snapshot file"))?;
    let marker = FileMarker {
        name: section.name.clone(),
        media_type: section.media_type.clone(),
        size_bytes: section.size.ok_or(RestoreError::Entry("snapshot file"))?,
        sha256,
    };
    if marker.has_valid_hash() {
        Ok(marker)
    } else {
        Err(RestoreError::Entry("snapshot file"))
    }
}

fn same_database(object: &FileObject, database_id: &str) -> bool {
    object.path.database.as_str() == database_id
        && object.path.validate().is_ok()
        && object.scope.validate().is_ok()
}

fn record_inline(
    body: &AcceptedBody,
    file: &RecordFile,
) -> Result<Option<InlineWrite>, RestoreError> {
    let Some(section) = &body.inline_file else {
        return if body.kind == "inline" {
            Err(RestoreError::Entry("snapshot entry"))
        } else {
            Ok(None)
        };
    };
    if body.kind == "record" {
        return Err(RestoreError::Entry("snapshot entry"));
    }
    if section.cleared {
        return if section.marker.is_none() && section.data.is_none() {
            Ok(Some(InlineWrite::Clear))
        } else {
            Err(RestoreError::Entry("snapshot file"))
        };
    }
    if matches!(file, RecordFile::Fetch(_)) {
        return Err(RestoreError::Entry("snapshot entry"));
    }
    let marker = section
        .marker
        .clone()
        .ok_or(RestoreError::Entry("snapshot file"))?;
    let data = section
        .data
        .clone()
        .ok_or(RestoreError::Entry("snapshot file"))?;
    let inline = InlineFile { marker, data };
    inline
        .bytes()
        .map_err(|_| RestoreError::Entry("snapshot file"))?;
    Ok(Some(InlineWrite::Put(inline)))
}

fn record_contribution(body: &AcceptedBody) -> Result<Option<ContributionAdmission>, RestoreError> {
    let Some(section) = &body.contribution else {
        return if body.kind == "contribution" {
            Err(RestoreError::Entry("snapshot entry"))
        } else {
            Ok(None)
        };
    };
    if body.kind != "contribution" || section.writer.is_empty() {
        return Err(RestoreError::Entry("snapshot entry"));
    }
    if body.definition_id.as_deref() != Some(section.definition_id.as_str()) {
        return Err(RestoreError::Entry("snapshot entry"));
    }
    Ok(Some(ContributionAdmission {
        definition_id: section.definition_id.clone(),
        receipt_id: hex_32(&section.receipt_sha256)?,
        intent_digest: hex_32(&section.intent_sha256)?,
        writer: section.writer.clone(),
    }))
}

fn mutation_from(
    body: &AcceptedBody,
    key: KeyValue,
    fields: BTreeMap<String, Value>,
    alternates: BTreeMap<String, Option<KeyValue>>,
    mutation_type: MutationType,
    contribution: Option<ContributionAdmission>,
) -> Mutation {
    let mut record_fields = HashMap::with_capacity(fields.len());
    for (name, value) in fields {
        record_fields.insert(name, value);
    }
    Mutation {
        uuid: body.mutation_id.clone(),
        schema_name: body.schema.clone(),
        record_fields,
        key_value: key,
        alternates,
        file: None,
        pub_key: String::new(),
        mutation_type,
        synchronous: None,
        source_file_name: None,
        metadata: None,
        imported_written_at: None,
        logical_counter: 0,
        author_clock_writer_id: String::new(),
        author_clock_signature: String::new(),
        author_clock_signature_version: 0,
        expected: fold_db::schema::types::CasExpectation::Absent,
        erase_caller: None,
        erase_written_before_nanos: None,
        file_write: None,
        inline_write: None,
        definition_id: body.definition_id.clone(),
        contribution,
    }
}

fn hex_32(text: &str) -> Result<[u8; 32], RestoreError> {
    if text.len() != 64 {
        return Err(RestoreError::Entry("snapshot entry"));
    }
    let bytes = text.as_bytes();
    let mut out = [0u8; 32];
    for (index, slot) in out.iter_mut().enumerate() {
        let high = hex_nibble(bytes[index * 2])?;
        let low = hex_nibble(bytes[index * 2 + 1])?;
        *slot = (high << 4) | low;
    }
    Ok(out)
}

fn hex_nibble(byte: u8) -> Result<u8, RestoreError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(RestoreError::Entry("snapshot entry")),
    }
}
