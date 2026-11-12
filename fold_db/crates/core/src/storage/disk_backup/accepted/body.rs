//! Canonical plaintext for one accepted logical write.

use super::disk::Note;
use crate::crypto::envelope::encrypt_envelope;
use crate::schema::types::{KeyValue, Mutation, MutationType};
use crate::storage::error::{StorageError, StorageResult};
use lastdb_model::hex::hex_lower;
use lastdb_model::record_value::{FileMarker, InlineFile};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

const BODY_MAX: usize = 1024 * 1024;

#[derive(Serialize)]
pub(super) struct Body<'a> {
    version: u16,
    mutation_id: &'a str,
    kind: &'a str,
    schema: &'a str,
    action: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    key: Option<&'a KeyValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fields: Option<&'a BTreeMap<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    alternates: Option<&'a BTreeMap<String, Option<KeyValue>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    file: Option<FileSection<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    inline_file: Option<InlineSection<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    definition_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    definition: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    contribution: Option<ContributionSection<'a>>,
}

#[derive(Serialize)]
pub(super) struct FileSection<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    content_sha256: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sealed_sha256: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    media_type: Option<&'a str>,
    cleared: bool,
}

#[derive(Serialize)]
pub(super) struct InlineSection<'a> {
    cleared: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    marker: Option<&'a FileMarker>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<&'a str>,
}

#[derive(Serialize)]
pub(super) struct ContributionSection<'a> {
    definition_id: &'a str,
    writer: &'a str,
    receipt_sha256: &'a str,
    intent_sha256: &'a str,
}

pub(super) fn mutation_id_ok(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

pub(super) fn schema_id(name: &str, tag: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(name.as_bytes());
    hasher.update([0]);
    hasher.update(tag.as_bytes());
    format!("s{}", &hex_lower(hasher.finalize())[..40])
}

pub(super) fn sha256_hex(bytes: &[u8]) -> String {
    hex_lower(Sha256::digest(bytes))
}

pub(super) fn action_name(kind: MutationType) -> &'static str {
    match kind {
        MutationType::Create => "create",
        MutationType::Replace => "replace",
        MutationType::Delete => "delete",
        MutationType::Purge => "purge",
    }
}

pub(super) fn kind_name(mutation: &Mutation, file: bool, inline: bool) -> &'static str {
    if mutation.contribution.is_some() {
        "contribution"
    } else if file {
        "file"
    } else if inline {
        "inline"
    } else {
        "record"
    }
}

pub(super) struct RecordParts<'a> {
    pub kind: &'a str,
    pub action: &'a str,
    pub fields: BTreeMap<String, Value>,
    pub file: Option<FileSection<'a>>,
    pub inline_file: Option<InlineSection<'a>>,
    pub definition_id: Option<&'a str>,
    pub contribution: Option<ContributionSection<'a>>,
}

pub(super) fn seal(content_key: &[u8; 32], body: &impl Serialize) -> StorageResult<Note> {
    let plain = cloud_backup_protocol::v2::hash::canonical_bytes(body)
        .map_err(|_| StorageError::SerializationError("accepted body is not canonical".into()))?;
    if plain.len() > BODY_MAX {
        return Err(StorageError::MutationLimit {
            detail: "accepted body exceeds 1 MiB".into(),
        });
    }
    let plaintext_sha256 = sha256_hex(&plain);
    let envelope = encrypt_envelope(content_key, &plain)
        .map_err(|_| StorageError::EncryptionError("accepted envelope seal failed".into()))?;
    let mutation_id = mutation_id_of(body)?;
    Ok(Note {
        mutation_id,
        plaintext_sha256,
        envelope,
        file: None,
    })
}

pub(super) fn file_section<'a>(
    marker: Option<&'a FileMarker>,
    sealed: Option<&'a str>,
    cleared: bool,
) -> FileSection<'a> {
    FileSection {
        content_sha256: marker.map(|marker| marker.sha256.as_str()),
        sealed_sha256: sealed,
        size: marker.map(|marker| marker.size_bytes),
        name: marker.and_then(|marker| marker.name.as_deref()),
        media_type: marker.and_then(|marker| marker.media_type.as_deref()),
        cleared,
    }
}

pub(super) fn inline_put(file: &InlineFile) -> StorageResult<InlineSection<'_>> {
    if !file.is_well_formed() {
        return Err(StorageError::InvalidOperation(
            "accepted inline file does not match its marker".into(),
        ));
    }
    file.bytes().map_err(|_| {
        StorageError::InvalidOperation("accepted inline file does not match its marker".into())
    })?;
    Ok(InlineSection {
        cleared: false,
        marker: Some(&file.marker),
        data: Some(file.data.as_str()),
    })
}

pub(super) fn inline_clear() -> InlineSection<'static> {
    InlineSection {
        cleared: true,
        marker: None,
        data: None,
    }
}

fn mutation_id_of(body: &impl Serialize) -> StorageResult<String> {
    let value = serde_json::to_value(body)
        .map_err(|_| StorageError::SerializationError("accepted body is not canonical".into()))?;
    value
        .get("mutation_id")
        .and_then(Value::as_str)
        .filter(|id| mutation_id_ok(id))
        .map(str::to_string)
        .ok_or_else(|| {
            StorageError::InvalidOperation("accepted mutation id is not a durable name".into())
        })
}

pub(super) fn record_body<'a>(mutation: &'a Mutation, parts: &'a RecordParts<'a>) -> Body<'a> {
    Body {
        version: 1,
        mutation_id: mutation.uuid.as_str(),
        kind: parts.kind,
        schema: mutation.schema_name.as_str(),
        action: parts.action,
        key: Some(&mutation.key_value),
        fields: Some(&parts.fields),
        alternates: Some(&mutation.alternates),
        file: parts.file.as_ref().map(file_ref),
        inline_file: parts.inline_file.as_ref().map(inline_ref),
        definition_id: parts.definition_id,
        definition: None,
        contribution: parts.contribution.as_ref().map(contribution_ref),
    }
}

fn file_ref<'a>(section: &'a FileSection<'a>) -> FileSection<'a> {
    FileSection {
        content_sha256: section.content_sha256,
        sealed_sha256: section.sealed_sha256,
        size: section.size,
        name: section.name,
        media_type: section.media_type,
        cleared: section.cleared,
    }
}

fn inline_ref<'a>(section: &'a InlineSection<'a>) -> InlineSection<'a> {
    InlineSection {
        cleared: section.cleared,
        marker: section.marker,
        data: section.data,
    }
}

fn contribution_ref<'a>(section: &'a ContributionSection<'a>) -> ContributionSection<'a> {
    ContributionSection {
        definition_id: section.definition_id,
        writer: section.writer,
        receipt_sha256: section.receipt_sha256,
        intent_sha256: section.intent_sha256,
    }
}

pub(super) fn schema_body<'a>(
    mutation_id: &'a str,
    name: &'a str,
    action: &'a str,
    definition: Option<&'a Value>,
) -> Body<'a> {
    Body {
        version: 1,
        mutation_id,
        kind: "schema",
        schema: name,
        action,
        key: None,
        fields: None,
        alternates: None,
        file: None,
        inline_file: None,
        definition_id: None,
        definition: definition.cloned(),
        contribution: None,
    }
}

pub(super) fn contribution<'a>(
    definition_id: &'a str,
    writer: &'a str,
    receipt_sha256: &'a str,
    intent_sha256: &'a str,
) -> ContributionSection<'a> {
    ContributionSection {
        definition_id,
        writer,
        receipt_sha256,
        intent_sha256,
    }
}
