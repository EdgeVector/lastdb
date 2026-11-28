//! Canonical plaintext for one accepted logical write.

pub(super) use super::payload::{AcceptedBody as Body, FileSection, InlineSection};
use crate::schema::types::{Mutation, MutationType};
use crate::storage::error::{StorageError, StorageResult};
use lastdb_model::hex::hex_lower;
use lastdb_model::record_value::{FileMarker, InlineFile};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::borrow::Cow;
use std::collections::BTreeMap;

const BODY_MAX: usize = 1024 * 1024;

pub(super) fn mutation_id_ok(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

pub(super) fn schema_id(name: &str, tag: &str, authored_at_ms: u64) -> String {
    let mut hasher = Sha256::new();
    hasher.update(name.as_bytes());
    hasher.update([0]);
    hasher.update(tag.as_bytes());
    hasher.update([0]);
    hasher.update(authored_at_ms.to_be_bytes());
    format!("s{}", &hex_lower(hasher.finalize())[..40])
}

/// The mutation id of an entry that is not a record or schema write: a hash of
/// its kind and its order value. Every entry of the stream has a higher order
/// value than the one before it, so the id is new.
pub(super) fn entry_id(kind: &str, order: u64) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"entry");
    hasher.update([0]);
    hasher.update(kind.as_bytes());
    hasher.update([0]);
    hasher.update(order.to_be_bytes());
    format!("e{}", &hex_lower(hasher.finalize())[..40])
}

pub(super) fn action_name(kind: MutationType) -> &'static str {
    match kind {
        MutationType::Create => "create",
        MutationType::Replace => "replace",
        MutationType::Delete => "delete",
        MutationType::Purge => "purge",
    }
}

/// Body kind for a record write. When both an external file and an inline file
/// are present, the kind is still `"file"` so file transport dominates.
pub(super) fn kind_name(file: bool, inline: bool) -> &'static str {
    if file {
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
}

/// The canonical bytes of a body. The author seals and signs these bytes.
pub(super) fn plain(body: &Body<'_>) -> StorageResult<Vec<u8>> {
    let plain = cloud_backup_protocol::v2::hash::canonical_bytes(body)
        .map_err(|_| StorageError::SerializationError("accepted body is not canonical".into()))?;
    if plain.len() > BODY_MAX {
        return Err(StorageError::MutationLimit {
            detail: "accepted body exceeds 1 MiB".into(),
        });
    }
    if !mutation_id_ok(body.mutation_id.as_ref()) {
        return Err(StorageError::InvalidOperation(
            "accepted mutation id is not a durable name".into(),
        ));
    }
    Ok(plain)
}

pub(super) fn file_section<'a>(
    marker: Option<&'a FileMarker>,
    sealed: Option<&'a str>,
    cleared: bool,
) -> FileSection<'a> {
    FileSection {
        content_sha256: marker.map(|marker| Cow::Borrowed(marker.sha256.as_str())),
        sealed_sha256: sealed.map(Cow::Borrowed),
        size: marker.map(|marker| marker.size_bytes),
        name: marker
            .and_then(|marker| marker.name.as_deref())
            .map(Cow::Borrowed),
        media_type: marker
            .and_then(|marker| marker.media_type.as_deref())
            .map(Cow::Borrowed),
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
        marker: Some(Cow::Borrowed(&file.marker)),
        data: Some(Cow::Borrowed(file.data.as_str())),
    })
}

pub(super) fn inline_clear() -> InlineSection<'static> {
    InlineSection {
        cleared: true,
        marker: None,
        data: None,
    }
}

pub(super) fn record_body<'a>(mutation: &'a Mutation, parts: &'a RecordParts<'a>) -> Body<'a> {
    Body {
        version: 1,
        mutation_id: Cow::Borrowed(mutation.uuid.as_str()),
        kind: Cow::Borrowed(parts.kind),
        schema: Cow::Borrowed(mutation.schema_name.as_str()),
        action: Cow::Borrowed(parts.action),
        key: Some(Cow::Borrowed(&mutation.key_value)),
        fields: Some(Cow::Borrowed(&parts.fields)),
        alternates: Some(Cow::Borrowed(&mutation.alternates)),
        file: parts.file.clone(),
        inline_file: parts.inline_file.clone(),
        definition_id: parts.definition_id.map(Cow::Borrowed),
        definition: None,
        contribution: None,
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
        mutation_id: Cow::Borrowed(mutation_id),
        kind: Cow::Borrowed("schema"),
        schema: Cow::Borrowed(name),
        action: Cow::Borrowed(action),
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
