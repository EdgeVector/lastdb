//! Open one entry and build the store mutation. No object fetch and no write.
//!
//! The part payload is an envelope, not canonical JSON. File fragment bytes
//! are a second envelope under the same content key and the same entry
//! binding. `RecordPayload` uses the canonical record fields. The optional
//! `file` pointer is omitted when absent, so a file-free record still
//! re-encodes to those bytes.

use std::collections::{BTreeMap, HashMap};

use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use base64::Engine;
use cloud_backup_protocol::v2::blobs::BlobPath;
use cloud_backup_protocol::{bytes_sha256, canonical_bytes, CloudScope, Digest};
use fold_db::schema::types::{CasExpectation, KeyValue, Mutation, MutationType};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::log_envelope::{self, DatabaseContentKey, IncorporatedRef, LogBinding};

use super::chain::Link;
use super::types::{IncorporatedFrom, SnapshotEntry, SnapshotFileRequest};
use super::RestoreError;

pub(super) struct Prepared {
    pub mutation: Mutation,
    pub hash: String,
    pub file: Option<PreparedFile>,
}

pub(super) struct PreparedFile {
    pub name: Option<String>,
    pub media_type: Option<String>,
    pub size_bytes: u64,
    pub sha256: String,
    pub source: FileSource,
}

pub(super) enum FileSource {
    /// Fragment plaintext. The envelope was opened during the check.
    Inline(Vec<u8>),
    /// Object bytes are still sealed. Apply opens them before `seal_record_file`.
    Object(SnapshotFileRequest),
}

pub(super) fn check_chain(
    key: &DatabaseContentKey,
    database_id: &str,
    links: &[Link],
) -> Result<(), RestoreError> {
    let mut seen = HashMap::new();
    for link in links {
        for part in &link.parts {
            for entry in &part.entries {
                note_entry(&mut seen, &decode_entry(key, database_id, entry)?)?;
            }
        }
    }
    Ok(())
}

pub(super) fn decode_entry(
    key: &DatabaseContentKey,
    database_id: &str,
    entry: &SnapshotEntry,
) -> Result<Prepared, RestoreError> {
    let sealed = URL_SAFE_NO_PAD
        .decode(entry.payload_b64.as_bytes())
        .map_err(|_| RestoreError::Entry("snapshot entry"))?;
    let opened = open_sealed(
        key,
        database_id,
        entry,
        &sealed,
        RestoreError::Entry("snapshot entry"),
    )?;
    let payload: RecordPayload =
        serde_json::from_slice(&opened).map_err(|_| RestoreError::Entry("snapshot entry"))?;
    let again = canonical_bytes(&payload).map_err(|_| RestoreError::Entry("snapshot entry"))?;
    if again != opened {
        return Err(RestoreError::Entry("snapshot entry"));
    }
    accept_payload(key, database_id, entry, &payload, &opened)
}

/// Open one fragment or record envelope. Plaintext and a bad binding are refused.
pub(super) fn open_sealed(
    key: &DatabaseContentKey,
    database_id: &str,
    entry: &SnapshotEntry,
    sealed: &[u8],
    failure: RestoreError,
) -> Result<Vec<u8>, RestoreError> {
    match log_envelope::open(key, &binding(database_id, entry), sealed) {
        Ok(plain) => Ok(plain),
        Err(log_envelope::LogEnvelopeError::Key) => Err(RestoreError::Scope("database store")),
        Err(_) => Err(failure),
    }
}

fn binding<'a>(database_id: &'a str, entry: &'a SnapshotEntry) -> LogBinding<'a> {
    LogBinding {
        database_id,
        writer_key: &entry.writer_key,
        writer_device: &entry.writer_device,
        writer_seq: entry.writer_seq,
        authored_at_ms: entry.authored_at_ms,
        namespace: &entry.namespace,
        incorporated_from: entry.incorporated_from.as_ref().map(incorporated),
    }
}

fn incorporated(from: &IncorporatedFrom) -> IncorporatedRef<'_> {
    IncorporatedRef {
        writer_key: &from.writer_key,
        writer_device: &from.writer_device,
        writer_seq: from.writer_seq,
    }
}

fn note_entry(seen: &mut HashMap<String, String>, prepared: &Prepared) -> Result<(), RestoreError> {
    if let Some(prior) = seen.insert(prepared.mutation.uuid.clone(), prepared.hash.clone()) {
        if prior != prepared.hash {
            return Err(RestoreError::Entry("mutation id"));
        }
    }
    Ok(())
}

fn accept_payload(
    key: &DatabaseContentKey,
    database_id: &str,
    entry: &SnapshotEntry,
    payload: &RecordPayload,
    opened: &[u8],
) -> Result<Prepared, RestoreError> {
    check_namespace(&payload.schema, &entry.namespace)?;
    if !mutation_label(&payload.mutation_id) {
        return Err(RestoreError::Entry("mutation id"));
    }
    let mutation_type = action(payload)?;
    if mutation_type == MutationType::Delete
        && (payload.file.is_some() || !payload.record.is_empty())
    {
        return Err(RestoreError::Entry("snapshot entry"));
    }
    let file = payload
        .file
        .as_ref()
        .map(|file| prepare_file(key, database_id, entry, file))
        .transpose()?;
    Ok(Prepared {
        mutation: to_mutation(payload, mutation_type),
        hash: bytes_sha256(opened).as_str().to_owned(),
        file,
    })
}

fn check_namespace(schema: &str, namespace: &str) -> Result<(), RestoreError> {
    let prefix = schema.split_once('/').map_or(schema, |(prefix, _)| prefix);
    if prefix == namespace {
        Ok(())
    } else {
        Err(RestoreError::Entry("snapshot entry"))
    }
}

fn action(payload: &RecordPayload) -> Result<MutationType, RestoreError> {
    match (payload.action.as_str(), &payload.expected) {
        ("create", CasExpectation::Absent) => Ok(MutationType::Create),
        ("replace", CasExpectation::Revision { revision }) if !revision.is_empty() => {
            Ok(MutationType::Replace)
        }
        ("delete", CasExpectation::Revision { revision }) if !revision.is_empty() => {
            Ok(MutationType::Delete)
        }
        _ => Err(RestoreError::Entry("snapshot entry")),
    }
}

fn prepare_file(
    key: &DatabaseContentKey,
    database_id: &str,
    entry: &SnapshotEntry,
    file: &FileAttachment,
) -> Result<PreparedFile, RestoreError> {
    Digest::try_from(file.sha256.clone()).map_err(|_| RestoreError::Entry("snapshot file"))?;
    let source = match (&file.object, &file.inline_b64) {
        (Some(object), None) => FileSource::Object(object_request(database_id, object)?),
        (None, Some(inline)) => {
            FileSource::Inline(inline_plain(key, database_id, entry, file, inline)?)
        }
        _ => return Err(RestoreError::Entry("snapshot file")),
    };
    Ok(PreparedFile {
        name: file.name.clone(),
        media_type: file.media_type.clone(),
        size_bytes: file.size_bytes,
        sha256: file.sha256.clone(),
        source,
    })
}

fn object_request(
    database_id: &str,
    object: &FileObject,
) -> Result<SnapshotFileRequest, RestoreError> {
    object
        .scope
        .validate()
        .map_err(|_| RestoreError::Entry("snapshot file"))?;
    object
        .path
        .validate()
        .map_err(|_| RestoreError::Entry("snapshot file"))?;
    if object.path.database.as_str() != database_id {
        return Err(RestoreError::Entry("snapshot file"));
    }
    Ok(SnapshotFileRequest {
        database_id: database_id.to_owned(),
        scope: object.scope.clone(),
        path: object.path.clone(),
    })
}

fn inline_plain(
    key: &DatabaseContentKey,
    database_id: &str,
    entry: &SnapshotEntry,
    file: &FileAttachment,
    inline: &str,
) -> Result<Vec<u8>, RestoreError> {
    let sealed = STANDARD
        .decode(inline.as_bytes())
        .map_err(|_| RestoreError::Entry("snapshot file"))?;
    let plain = open_sealed(
        key,
        database_id,
        entry,
        &sealed,
        RestoreError::Entry("snapshot file"),
    )?;
    if plain_matches(&plain, file.size_bytes, &file.sha256) {
        Ok(plain)
    } else {
        Err(RestoreError::Entry("snapshot file"))
    }
}

pub(super) fn plain_matches(plain: &[u8], size_bytes: u64, sha256: &str) -> bool {
    u64::try_from(plain.len()).ok() == Some(size_bytes) && bytes_sha256(plain).as_str() == sha256
}

fn to_mutation(payload: &RecordPayload, mutation_type: MutationType) -> Mutation {
    let mut record_fields = HashMap::with_capacity(payload.record.len());
    for (key, value) in &payload.record {
        record_fields.insert(key.clone(), value.clone());
    }
    Mutation {
        uuid: payload.mutation_id.clone(),
        schema_name: payload.schema.clone(),
        record_fields,
        key_value: payload.key.clone(),
        alternates: payload.alternates.clone(),
        file: None,
        pub_key: payload.pub_key.clone(),
        mutation_type,
        synchronous: None,
        source_file_name: None,
        metadata: None,
        imported_written_at: None,
        logical_counter: 0,
        author_clock_writer_id: String::new(),
        author_clock_signature: String::new(),
        author_clock_signature_version: 0,
        expected: payload.expected.clone(),
        erase_caller: None,
        erase_written_before_nanos: None,
        file_write: None,
        inline_write: None,
        definition_id: None,
        contribution: None,
    }
}

/// Marker file names share E's label: 1 to 64 bytes of letters, digits, `-`, `_`.
pub(super) fn mutation_label(value: &str) -> bool {
    (1..=64).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

/// Canonical record fields, plus one optional fragment pointer.
///
/// A missing `file` is omitted on re-encode. The fragment bytes themselves
/// are not these fields. They stay in `inline_b64` or the object, sealed.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordPayload {
    schema: String,
    action: String,
    key: KeyValue,
    record: BTreeMap<String, Value>,
    alternates: BTreeMap<String, Option<KeyValue>>,
    expected: CasExpectation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    file: Option<FileAttachment>,
    mutation_id: String,
    pub_key: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileAttachment {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    media_type: Option<String>,
    size_bytes: u64,
    sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    object: Option<FileObject>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    inline_b64: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileObject {
    scope: CloudScope,
    path: BlobPath,
}
