//! Stage sealed notes for one accepted user write. Failure leaves the batch uncommitted.

use super::body::{self, RecordParts};
use super::disk::Note;
use crate::schema::types::{FileWrite, LocalFileReference, Mutation, Schema};
use crate::storage::disk_pack::{self, transaction, PackRuntime};
use crate::storage::error::{StorageError, StorageResult};
use lastdb_model::hex::hex_lower;
use lastdb_model::record_value::{FileMarker, InlineWrite};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

struct External<'a> {
    present: bool,
    cleared: bool,
    marker: Option<&'a FileMarker>,
    sealed: Option<String>,
    bytes: Option<Vec<u8>>,
}

pub(crate) async fn stage_records(
    runtime: &PackRuntime,
    content_key: &[u8; 32],
    admit: impl Fn(&LocalFileReference) -> Option<&Path>,
    mutations: &[Mutation],
) -> StorageResult<()> {
    if runtime.fresh_initialization_pending() {
        return Ok(());
    }
    let mut seen = BTreeSet::new();
    let mut notes = Vec::with_capacity(mutations.len());
    for mutation in mutations {
        if !body::mutation_id_ok(&mutation.uuid) || !seen.insert(mutation.uuid.clone()) {
            return Err(invalid("accepted mutation id is not a durable name"));
        }
        notes.push(seal_mutation(content_key, mutation, &admit).await?);
    }
    transaction::stage_accepted(runtime, notes)
}

pub(crate) fn stage_schema_put(
    runtime: &PackRuntime,
    content_key: &[u8; 32],
    name: &str,
    schema: &Schema,
) -> StorageResult<()> {
    if runtime.fresh_initialization_pending() {
        return Ok(());
    }
    let wire = schema.to_wire();
    let tag = wire
        .get_identity_hash()
        .map(String::as_str)
        .unwrap_or("none");
    let mutation_id = body::schema_id(name, tag);
    let definition = serde_json::to_value(&wire).map_err(|_| {
        StorageError::SerializationError("accepted schema body is not canonical".into())
    })?;
    let body = body::schema_body(&mutation_id, name, "put", Some(&definition));
    transaction::stage_accepted(runtime, vec![body::seal(content_key, &body)?])
}

pub(crate) fn stage_schema_drop(
    runtime: &PackRuntime,
    content_key: &[u8; 32],
    name: &str,
) -> StorageResult<()> {
    if runtime.fresh_initialization_pending() {
        return Ok(());
    }
    let mutation_id = body::schema_id(name, "drop");
    let body = body::schema_body(&mutation_id, name, "drop", None);
    transaction::stage_accepted(runtime, vec![body::seal(content_key, &body)?])
}

async fn seal_mutation(
    content_key: &[u8; 32],
    mutation: &Mutation,
    admit: &impl Fn(&LocalFileReference) -> Option<&Path>,
) -> StorageResult<Note> {
    let external = external_file(content_key, mutation, admit).await?;
    let has_file = external.bytes.is_some();
    let sealed = external.sealed.clone();
    let marker = external.marker;
    let cleared = external.cleared;
    let present = external.present;
    let file_bytes = external.bytes;
    let inline_file = inline_section(mutation, has_file)?;
    let (receipt_sha, intent_sha) = contribution_hashes(mutation);
    let contribution = contribution_section(mutation, &receipt_sha, &intent_sha);
    let parts = RecordParts {
        kind: body::kind_name(mutation, has_file, inline_file.is_some()),
        action: body::action_name(mutation.mutation_type),
        fields: mutation
            .record_fields
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>(),
        file: present.then(|| body::file_section(marker, sealed.as_deref(), cleared)),
        inline_file,
        definition_id: mutation.definition_id.as_deref(),
        contribution,
    };
    let canonical = body::record_body(mutation, &parts);
    let mut note = body::seal(content_key, &canonical)?;
    note.file = file_bytes;
    Ok(note)
}

async fn external_file<'a>(
    content_key: &[u8; 32],
    mutation: &'a Mutation,
    admit: &impl Fn(&LocalFileReference) -> Option<&Path>,
) -> StorageResult<External<'a>> {
    let Some(write) = mutation.file_write.as_ref() else {
        return Ok(External::absent());
    };
    match write {
        FileWrite::Clear if mutation.file.is_some() => {
            Err(invalid("accepted file clear still carries a marker"))
        }
        FileWrite::Clear => Ok(External::clear()),
        FileWrite::Put(_) => owned_row(content_key, mutation, write, None),
        FileWrite::LocalPath(_) => {
            let plaintext = read_admitted(mutation, write, admit).await?;
            owned_row(content_key, mutation, write, Some(plaintext))
        }
    }
}

fn owned_row<'a>(
    content_key: &[u8; 32],
    mutation: &'a Mutation,
    write: &'a FileWrite,
    plaintext: Option<Vec<u8>>,
) -> StorageResult<External<'a>> {
    let marker = file_marker(mutation, write)?;
    let row = match (write.sealed_row(), plaintext) {
        (Some(row), None) => {
            crate::storage::format::records::require_row(content_key, marker, row)
                .map_err(|_| invalid("accepted file row does not match its marker"))?;
            row.to_vec()
        }
        (None, Some(plaintext)) => {
            let (row, _) = crate::storage::format::records::seal_file_row(content_key, &plaintext)
                .map_err(|_| StorageError::EncryptionError("accepted file seal failed".into()))?;
            row
        }
        _ => return Err(invalid("accepted file row is missing")),
    };
    let sealed = body::sha256_hex(&row);
    Ok(External {
        present: true,
        cleared: false,
        marker: Some(marker),
        sealed: Some(sealed),
        bytes: Some(row),
    })
}

async fn read_admitted(
    mutation: &Mutation,
    write: &FileWrite,
    admit: &impl Fn(&LocalFileReference) -> Option<&Path>,
) -> StorageResult<Vec<u8>> {
    let reference = write
        .local_reference()
        .ok_or_else(|| invalid("accepted local file is missing"))?;
    let path = admit(reference)
        .ok_or_else(|| invalid("accepted local file was not issued for this database"))?;
    let marker = file_marker(mutation, write)?;
    let owned = path.to_path_buf();
    let size = marker.size_bytes;
    let hash = marker.sha256.clone();
    disk_pack::run_blocking(move || read_local(&owned, size, &hash)).await
}

fn file_marker<'a>(mutation: &'a Mutation, write: &'a FileWrite) -> StorageResult<&'a FileMarker> {
    let from_write = match write {
        FileWrite::Put(file) => file.marker(),
        FileWrite::LocalPath(file) => file.marker(),
        FileWrite::Clear => return Err(invalid("accepted file clear has no marker")),
    };
    if mutation
        .file
        .as_ref()
        .is_some_and(|marker| marker != from_write)
    {
        return Err(invalid("accepted file marker does not match the mutation"));
    }
    Ok(from_write)
}

fn inline_section(
    mutation: &Mutation,
    external: bool,
) -> StorageResult<Option<body::InlineSection<'_>>> {
    match mutation.inline_write.as_ref() {
        None => Ok(None),
        Some(InlineWrite::Clear) => Ok(Some(body::inline_clear())),
        Some(InlineWrite::Put(_)) if external => Err(invalid(
            "accepted record carries both an inline file and an external file",
        )),
        Some(InlineWrite::Put(file)) => Ok(Some(body::inline_put(file)?)),
    }
}

fn contribution_hashes(mutation: &Mutation) -> (String, String) {
    match mutation.contribution.as_ref() {
        Some(admission) => (
            hex_lower(admission.receipt_id),
            hex_lower(admission.intent_digest),
        ),
        None => (String::new(), String::new()),
    }
}

fn contribution_section<'a>(
    mutation: &'a Mutation,
    receipt: &'a str,
    intent: &'a str,
) -> Option<body::ContributionSection<'a>> {
    let admission = mutation.contribution.as_ref()?;
    Some(body::contribution(
        admission.definition_id.as_str(),
        admission.writer.as_str(),
        receipt,
        intent,
    ))
}

fn read_local(path: &Path, size: u64, hash: &str) -> StorageResult<Vec<u8>> {
    let cap = lastdb_model::storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES;
    if size > cap {
        return Err(StorageError::WriteResourceLimit {
            detail: "accepted local file exceeds the in-memory file cap".into(),
        });
    }
    let info =
        std::fs::metadata(path).map_err(|_| invalid("accepted local file is not available"))?;
    if !info.is_file() || info.len() != size {
        return Err(invalid("accepted local file does not match its marker"));
    }
    let bytes = std::fs::read(path).map_err(|_| invalid("accepted local file is not available"))?;
    if bytes.len() as u64 != size || hex_lower(Sha256::digest(&bytes)) != hash {
        return Err(invalid("accepted local file does not match its marker"));
    }
    Ok(bytes)
}

fn invalid(detail: &str) -> StorageError {
    StorageError::InvalidOperation(detail.into())
}

impl External<'_> {
    fn absent() -> Self {
        Self {
            present: false,
            cleared: false,
            marker: None,
            sealed: None,
            bytes: None,
        }
    }

    fn clear() -> Self {
        Self {
            present: true,
            cleared: true,
            marker: None,
            sealed: None,
            bytes: None,
        }
    }
}
