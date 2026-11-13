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
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

tokio::task_local! {
    pub(super) static REPLAY_STORE: Arc<PackRuntime>;
}

pub(crate) fn capture_enabled(runtime: &PackRuntime) -> bool {
    runtime.captures_accepted()
        && !runtime.fresh_initialization_pending()
        && !REPLAY_STORE
            .try_with(|store| std::ptr::eq(store.as_ref(), runtime))
            .unwrap_or(false)
}

struct External<'a> {
    present: bool,
    cleared: bool,
    marker: Option<&'a FileMarker>,
    sealed: Option<String>,
    bytes: Option<Vec<u8>>,
    reference: Option<String>,
}

pub(crate) async fn stage_records(
    runtime: &PackRuntime,
    content_key: &[u8; 32],
    admit: impl Fn(&LocalFileReference) -> Option<PathBuf>,
    mutations: &[Mutation],
) -> StorageResult<()> {
    if !capture_enabled(runtime) {
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
) -> StorageResult<Option<String>> {
    if !capture_enabled(runtime) {
        return Ok(None);
    }
    let wire = schema.to_wire();
    let tag = wire.get_identity_hash().map_or("none", String::as_str);
    let mutation_id = body::schema_id(name, tag);
    let definition = serde_json::to_value(&wire).map_err(|_| {
        StorageError::SerializationError("accepted schema body is not canonical".into())
    })?;
    let body = body::schema_body(&mutation_id, name, "put", Some(&definition));
    let mut note = body::seal(content_key, &body)?;
    note.authored_at_ms = Some(crate::clock::unix_millis());
    transaction::stage_accepted(runtime, vec![note])?;
    Ok(Some(mutation_id))
}

pub(crate) fn stage_schema_drop(
    runtime: &PackRuntime,
    content_key: &[u8; 32],
    name: &str,
) -> StorageResult<Option<String>> {
    if !capture_enabled(runtime) {
        return Ok(None);
    }
    let mutation_id = super::schema_drop_id(name);
    let body = body::schema_body(&mutation_id, name, "drop", None);
    let mut note = body::seal(content_key, &body)?;
    note.authored_at_ms = Some(crate::clock::unix_millis());
    transaction::stage_accepted(runtime, vec![note])?;
    Ok(Some(mutation_id))
}

async fn seal_mutation(
    content_key: &[u8; 32],
    mutation: &Mutation,
    admit: &impl Fn(&LocalFileReference) -> Option<PathBuf>,
) -> StorageResult<Note> {
    let external = external_file(content_key, mutation, admit).await?;
    let has_file = external.bytes.is_some() || external.marker.is_some();
    let sealed = external.sealed.clone();
    let marker = external.marker;
    let cleared = external.cleared;
    let present = external.present;
    let file_bytes = external.bytes;
    let reference = external.reference;
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
    // Keep the author clock outside the canonical body so retries retain their identity.
    note.authored_at_ms = mutation.imported_written_at.map(|time| time / 1_000_000);
    note.file = file_bytes;
    note.reference = reference;
    Ok(note)
}

async fn external_file<'a>(
    content_key: &[u8; 32],
    mutation: &'a Mutation,
    admit: &impl Fn(&LocalFileReference) -> Option<PathBuf>,
) -> StorageResult<External<'a>> {
    let Some(write) = mutation.file_write.as_ref() else {
        return Ok(External::absent());
    };
    match write {
        FileWrite::Clear if mutation.file.is_some() => {
            Err(invalid("accepted file clear still carries a marker"))
        }
        FileWrite::Clear => Ok(External::clear()),
        FileWrite::Put(_) => owned_row(content_key, mutation, write),
        FileWrite::LocalPath(_) => {
            let marker = file_marker(mutation, write)?;
            let reference = confirm_local(marker, write, admit).await?;
            Ok(External::custody(marker, reference))
        }
    }
}

fn owned_row<'a>(
    content_key: &[u8; 32],
    mutation: &'a Mutation,
    write: &'a FileWrite,
) -> StorageResult<External<'a>> {
    let marker = file_marker(mutation, write)?;
    let Some(row) = write.sealed_row() else {
        return Err(invalid("accepted file row is missing"));
    };
    crate::storage::format::records::require_row(content_key, marker, row)
        .map_err(|_| invalid("accepted file row does not match its marker"))?;
    let row = row.to_vec();
    let sealed = body::sha256_hex(&row);
    Ok(External {
        present: true,
        cleared: false,
        marker: Some(marker),
        sealed: Some(sealed),
        bytes: Some(row),
        reference: None,
    })
}

async fn confirm_local(
    marker: &FileMarker,
    write: &FileWrite,
    admit: &impl Fn(&LocalFileReference) -> Option<PathBuf>,
) -> StorageResult<String> {
    let reference = write
        .local_reference()
        .ok_or_else(|| invalid("accepted local file is missing"))?;
    let path = admit(reference)
        .ok_or_else(|| invalid("accepted local file was not issued for this database"))?;
    let reference = path
        .to_str()
        .filter(|text| {
            !text.is_empty()
                && text.len() <= 4096
                && !text
                    .bytes()
                    .any(|byte| byte == 0 || byte == b'\n' || byte == b'\r')
        })
        .ok_or_else(|| invalid("accepted local file path is not a durable reference"))?
        .to_owned();
    let size = marker.size_bytes;
    let hash = marker.sha256.clone();
    disk_pack::run_blocking(move || confirm_file(&path, size, &hash)).await?;
    Ok(reference)
}

/// Hash the admitted file in a fixed buffer. The bytes stay in local-file custody.
fn confirm_file(path: &Path, size: u64, hash: &str) -> StorageResult<()> {
    let info =
        std::fs::metadata(path).map_err(|_| invalid("accepted local file is not available"))?;
    if !info.is_file() || info.len() != size {
        return Err(invalid("accepted local file does not match its marker"));
    }
    let mut file =
        std::fs::File::open(path).map_err(|_| invalid("accepted local file is not available"))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    let mut seen = 0u64;
    loop {
        let count = file
            .read(&mut buffer)
            .map_err(|_| invalid("accepted local file is not available"))?;
        if count == 0 {
            break;
        }
        seen = seen
            .checked_add(count as u64)
            .ok_or_else(|| invalid("accepted local file does not match its marker"))?;
        hasher.update(&buffer[..count]);
    }
    if seen != size || hex_lower(hasher.finalize()) != hash {
        return Err(invalid("accepted local file does not match its marker"));
    }
    Ok(())
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

fn invalid(detail: &str) -> StorageError {
    StorageError::InvalidOperation(detail.into())
}

impl<'a> External<'a> {
    fn absent() -> Self {
        Self {
            present: false,
            cleared: false,
            marker: None,
            sealed: None,
            bytes: None,
            reference: None,
        }
    }

    fn clear() -> Self {
        Self {
            present: true,
            cleared: true,
            marker: None,
            sealed: None,
            bytes: None,
            reference: None,
        }
    }

    fn custody(marker: &'a FileMarker, reference: String) -> Self {
        Self {
            present: true,
            cleared: false,
            marker: Some(marker),
            sealed: None,
            bytes: None,
            reference: Some(reference),
        }
    }
}
