//! Sign one accepted user write as the next entry of the device stream, and stage it for the
//! spool, the cloud log. Failure leaves the batch uncommitted.
//!
//! The cloud log exists only to send this device's signed stream to the cloud. The local node
//! writes its entries and never reads them back for a check: only entries of other devices are
//! read, when they arrive (see `PackRuntime::replay_accepted`, under which nothing is captured).
//!
//! The mutation id of a captured write is new by construction, so the spool stores no id index:
//! - a record write gets a fresh UUIDv4 from the node (`stamp_and_validate` in the app write
//!   path, and the erase, incorporate and replay callers that build their own mutation);
//! - a schema put or drop derives its id from the schema name, its identity, and its author
//!   millisecond, and the author clock gives every entry of the stream a millisecond above the
//!   one before it.
//!
//! The format check, the repeat check inside one batch, and the comparison with the queued,
//! uncommitted writes (`precheck_accepted_queue`) stay. They read memory only.

mod files;

use super::author::{CaptureAuthor, LocalEntry};
use super::body::{self, RecordParts};
use super::disk::Note;
use crate::schema::types::{LocalFileReference, Mutation, Schema};
use crate::storage::disk_pack::{transaction, PackRuntime};
use crate::storage::error::{StorageError, StorageResult};
use lastdb_model::hex::sha256_hex;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
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
    for mutation in mutations {
        if !body::mutation_id_ok(&mutation.uuid) {
            return Err(invalid("accepted mutation id is not a durable name"));
        }
        // A repeat inside this batch only. The cloud log is not read back for its ids.
        if !seen.insert(mutation.uuid.as_str()) {
            return Err(invalid("mutation id was already accepted into the spool"));
        }
    }
    let mut notes = Vec::with_capacity(mutations.len());
    let mut head = previous;
    for mutation in mutations {
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

/// Sign and stage a schema put as the entry after `previous`. Returns the entry,
/// or `None` when the store captures nothing.
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
    let mutation_id = body::schema_id(name, tag, authored_at_ms);
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
    let mutation_id = body::schema_id(name, "drop", authored_at_ms);
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
    // The id names the author millisecond, which is above that of every earlier entry of the
    // stream, so it is new; the cloud log is not read back for it. `prepare` still refuses
    // a repeat inside one commit and `precheck_accepted_queue` one among the queued writes.
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
    let plain = body::plain(body)?;
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
        seq,
        mutation_id: mutation_id.to_owned(),
        envelope: signed.entry,
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
    let external = files::external_file(content_key, mutation, admit).await?;
    let has_file = external.plain.is_some() || external.marker.is_some();
    let sealed = external.sealed.clone();
    let inline_file = files::inline_section(mutation)?;
    let parts = RecordParts {
        // File transport dominates when both attachment slots are written.
        kind: body::kind_name(has_file, inline_file.is_some()),
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
    };
    let canonical = body::record_body(mutation, &parts);
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

fn invalid(detail: &str) -> StorageError {
    StorageError::InvalidOperation(detail.into())
}
