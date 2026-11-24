//! Sign one accepted user write as the next entry of the device stream, and stage it for the
//! spool. Failure leaves the batch uncommitted.

use super::author::{CaptureAuthor, LocalEntry};
use super::body::{self, RecordParts};
use super::disk::Note;
use crate::schema::types::{FileWrite, LocalFileReference, Mutation, Schema};
use crate::storage::disk_pack::{self, transaction, PackRuntime};
use crate::storage::error::{StorageError, StorageResult};
use lastdb_model::hex::{hex_lower, sha256_hex};
use lastdb_model::record_value::{FileMarker, InlineWrite};
use lastdb_model::storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES;
use std::collections::{BTreeMap, BTreeSet};
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

/// The external file of a write: what its body says, and the bytes to seal.
struct External<'a> {
    present: bool,
    cleared: bool,
    marker: Option<&'a FileMarker>,
    /// Hash of the sealed row in this store, for a file the write brought as a row.
    sealed: Option<String>,
    plain: Option<Vec<u8>>,
}

fn author_of(runtime: &PackRuntime) -> StorageResult<&Arc<dyn CaptureAuthor>> {
    runtime
        .capture_author()
        .ok_or_else(|| invalid("captured writes have no author"))
}

/// Sign each mutation as the next entry after `previous` and stage the entries
/// for the spool. Returns the last entry, or `None` when the store captures nothing.
pub(crate) async fn stage_records(
    runtime: &PackRuntime,
    content_key: &[u8; 32],
    admit: impl Fn(&LocalFileReference) -> Option<PathBuf>,
    mutations: &[Mutation],
    previous: Option<LocalEntry>,
) -> StorageResult<Option<LocalEntry>> {
    if !capture_enabled(runtime) {
        return Ok(None);
    }
    // One commit is one pending record in the spool, and a record that names more
    // notes than the control file bound fails the commit queue, which stops the
    // daemon. A commit that large is refused here with an error the caller can read.
    if mutations.len() > super::NOTES_MAX {
        return Err(StorageError::MutationLimit {
            detail: format!(
                "a write that is captured for upload holds at most {} mutations",
                super::NOTES_MAX
            ),
        });
    }
    let author = author_of(runtime)?;
    let mut seen = BTreeSet::new();
    let mut notes = Vec::with_capacity(mutations.len());
    let mut head = previous;
    for mutation in mutations {
        if !body::mutation_id_ok(&mutation.uuid) || !seen.insert(mutation.uuid.clone()) {
            return Err(invalid("accepted mutation id is not a durable name"));
        }
        let (note, next) = sign_mutation(
            author.as_ref(),
            content_key,
            mutation,
            &admit,
            head.as_ref(),
        )
        .await?;
        notes.push(note);
        head = Some(next);
    }
    transaction::stage_accepted(runtime, notes)?;
    Ok(head)
}

/// Sign and stage a schema put as the entry after `previous`. Returns the entry.
/// `None` when the store captures nothing, and when the spool already holds an
/// entry with the id of this put, which it keeps as the only one.
pub(crate) fn stage_schema_put(
    runtime: &PackRuntime,
    name: &str,
    schema: &Schema,
    authored_at_ms: u64,
    previous: Option<&LocalEntry>,
) -> StorageResult<Option<LocalEntry>> {
    if !capture_enabled(runtime) {
        return Ok(None);
    }
    let wire = schema.to_wire();
    let tag = wire.identity_hash.as_deref().unwrap_or("none");
    let mutation_id = body::schema_id(name, tag);
    let definition = serde_json::to_value(&wire).map_err(|_| {
        StorageError::SerializationError("accepted schema body is not canonical".into())
    })?;
    let body = body::schema_body(&mutation_id, name, "put", Some(&definition));
    stage_schema_note(runtime, &mutation_id, &body, authored_at_ms, previous)
}

/// Sign and stage a schema drop. The result is as for [`stage_schema_put`].
pub(crate) fn stage_schema_drop(
    runtime: &PackRuntime,
    name: &str,
    authored_at_ms: u64,
    previous: Option<&LocalEntry>,
) -> StorageResult<Option<LocalEntry>> {
    if !capture_enabled(runtime) {
        return Ok(None);
    }
    let mutation_id = body::schema_id(name, "drop");
    let body = body::schema_body(&mutation_id, name, "drop", None);
    stage_schema_note(runtime, &mutation_id, &body, authored_at_ms, previous)
}

fn stage_schema_note(
    runtime: &PackRuntime,
    mutation_id: &str,
    body: &body::Body<'_>,
    authored_at_ms: u64,
    previous: Option<&LocalEntry>,
) -> StorageResult<Option<LocalEntry>> {
    // The spool takes an id once. A change it already holds gets no second entry,
    // so it must not take a place in the stream.
    if runtime
        .accepted_mutation_seen(mutation_id)
        .map_err(super::storage)?
    {
        return Ok(None);
    }
    let author = author_of(runtime)?;
    let (note, next) = signed_note(
        author.as_ref(),
        previous,
        authored_at_ms,
        (mutation_id, body),
        None,
    )?;
    transaction::stage_accepted(runtime, vec![note])?;
    Ok(Some(next))
}

/// Seal the file and the body of one write, and sign the entry after `previous`.
fn signed_note(
    author: &dyn CaptureAuthor,
    previous: Option<&LocalEntry>,
    order: u64,
    (mutation_id, body): (&str, &body::Body<'_>),
    file: Option<&[u8]>,
) -> StorageResult<(Note, LocalEntry)> {
    let refused = |detail: String| {
        StorageError::InvalidOperation(format!("the write cannot be signed: {detail}"))
    };
    let (plain, plaintext_sha256) = body::plain(body)?;
    let seq = previous
        .map_or(Some(1), |held| held.seq.checked_add(1))
        .ok_or_else(|| invalid("the device stream sequence is exhausted"))?;
    let sealed = file
        .map(|bytes| author.seal_file(seq, bytes))
        .transpose()
        .map_err(refused)?;
    let file_sha256 = sealed.as_deref().map(sha256_hex);
    let signed = author
        .sign(previous, order, &plain, file_sha256.as_deref())
        .map_err(refused)?;
    let note = Note {
        mutation_id: mutation_id.to_owned(),
        plaintext_sha256,
        envelope: signed.entry,
        authored_at_ms: Some(order),
        file: sealed,
    };
    let next = LocalEntry {
        seq,
        hash: signed.hash,
        order,
    };
    Ok((note, next))
}

async fn sign_mutation(
    author: &dyn CaptureAuthor,
    content_key: &[u8; 32],
    mutation: &Mutation,
    admit: &impl Fn(&LocalFileReference) -> Option<PathBuf>,
    previous: Option<&LocalEntry>,
) -> StorageResult<(Note, LocalEntry)> {
    let external = external_file(content_key, mutation, admit).await?;
    let has_file = external.plain.is_some() || external.marker.is_some();
    let sealed = external.sealed.clone();
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
        file: external
            .present
            .then(|| body::file_section(external.marker, sealed.as_deref(), external.cleared)),
        inline_file,
        definition_id: mutation.definition_id.as_deref(),
        contribution,
    };
    let canonical = body::record_body(mutation, &parts);
    // The author time is the order value of the entry. It is outside the canonical
    // body, so a retry keeps its identity.
    let order = mutation
        .imported_written_at
        .map(|time| time / 1_000_000)
        .ok_or_else(|| invalid("a captured write has no author time"))?;
    signed_note(
        author,
        previous,
        order,
        (mutation.uuid.as_str(), &canonical),
        external.plain.as_deref(),
    )
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
            let plain = read_local(marker, write, admit).await?;
            Ok(External::put(marker, None, plain))
        }
    }
}

/// The bytes of a file that the write brought as a sealed row of this store.
fn owned_row<'a>(
    content_key: &[u8; 32],
    mutation: &'a Mutation,
    write: &'a FileWrite,
) -> StorageResult<External<'a>> {
    let marker = file_marker(mutation, write)?;
    let Some(row) = write.sealed_row() else {
        return Err(invalid("accepted file row is missing"));
    };
    let plain = crate::storage::format::records::decrypt_file_row(content_key, marker, row)
        .map_err(|_| invalid("accepted file row does not match its marker"))?;
    Ok(External::put(marker, Some(sha256_hex(row)), plain))
}

/// The bytes of a file that the write links by its path. They are read once,
/// here, so the entry can name the sealed file.
async fn read_local(
    marker: &FileMarker,
    write: &FileWrite,
    admit: &impl Fn(&LocalFileReference) -> Option<PathBuf>,
) -> StorageResult<Vec<u8>> {
    let reference = write
        .local_reference()
        .ok_or_else(|| invalid("accepted local file is missing"))?;
    let path = admit(reference)
        .ok_or_else(|| invalid("accepted local file was not issued for this database"))?;
    let size = marker.size_bytes;
    let hash = marker.sha256.clone();
    disk_pack::run_blocking(move || read_file(&path, size, &hash)).await
}

fn read_file(path: &Path, size: u64, hash: &str) -> StorageResult<Vec<u8>> {
    let unavailable = || invalid("accepted local file is not available");
    let differs = || invalid("accepted local file does not match its marker");
    let held = std::fs::metadata(path).map_err(|_| unavailable())?;
    if !held.is_file() || held.len() != size || size > LOCAL_FILE_INPUT_MEMORY_BYTES {
        return Err(differs());
    }
    let bytes = std::fs::read(path).map_err(|_| unavailable())?;
    if bytes.len() as u64 == size && sha256_hex(&bytes) == hash {
        Ok(bytes)
    } else {
        Err(differs())
    }
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
        admission.schema_scope,
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
            plain: None,
        }
    }

    fn clear() -> Self {
        Self {
            present: true,
            cleared: true,
            marker: None,
            sealed: None,
            plain: None,
        }
    }

    fn put(marker: &'a FileMarker, sealed: Option<String>, plain: Vec<u8>) -> Self {
        Self {
            present: true,
            cleared: false,
            marker: Some(marker),
            sealed,
            plain: Some(plain),
        }
    }
}
