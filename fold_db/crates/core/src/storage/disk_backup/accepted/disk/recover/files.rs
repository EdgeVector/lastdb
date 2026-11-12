//! Entry files, staging, and id records for one accepted stream.

use super::super::super::io;
use super::super::super::Error;
use super::super::{Inner, Meta, Note, PendingNote};
use std::path::Path;

const BODY_MAX: u64 = 2 * 1024 * 1024;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Committed {
    version: u16,
    frontier_before: u64,
}

pub(super) fn write_committed(root: &Path, frontier_before: u64) -> Result<(), Error> {
    io::write_json(
        &root.join("committed.json"),
        &Committed {
            version: 1,
            frontier_before,
        },
    )
}

pub(super) fn split_committed(root: &Path, frontier_before: u64) -> Result<bool, Error> {
    Ok(
        io::read_json::<Committed>(&root.join("committed.json"))?.is_some_and(|committed| {
            committed.version == 1 && committed.frontier_before == frontier_before
        }),
    )
}

pub(super) fn read_meta(dir: &Path) -> Result<Meta, Error> {
    io::require_dir(dir)?;
    let meta = io::read_json::<Meta>(&dir.join("meta.json"))?.ok_or(Error::Gap)?;
    if meta.version != 1 {
        return Err(Error::Gap);
    }
    Ok(meta)
}

pub(super) fn read_id(root: &Path, id: &str) -> Result<Option<String>, Error> {
    let path = root.join("ids").join(id);
    let Some(bytes) = io::read_limited(&path, 64)? else {
        return Ok(None);
    };
    let text = std::str::from_utf8(&bytes).map_err(|_| Error::Gap)?;
    if super::super::sha_ok(text) {
        Ok(Some(text.to_string()))
    } else {
        Err(Error::Gap)
    }
}

pub(super) fn verified_bytes(
    path: &Path,
    len: u64,
    expected: &str,
    max: u64,
) -> Result<Vec<u8>, Error> {
    if !super::super::sha_ok(expected) || len > max {
        return Err(Error::Gap);
    }
    let bytes = io::read_exact(path, max)?;
    if bytes.len() as u64 != len || super::super::body::sha256_hex(&bytes) != expected {
        return Err(Error::Gap);
    }
    Ok(bytes)
}

pub(super) fn body_max() -> u64 {
    BODY_MAX
}

pub(super) fn file_max() -> u64 {
    lastdb_model::storage_policy::native_write::LOCAL_FILE_INPUT_MEMORY_BYTES.saturating_add(64)
}

pub(super) fn seq_name(sequence: u64) -> String {
    format!("{sequence:020}")
}

pub(super) fn list_sequences(dir: &Path) -> Result<Vec<u64>, Error> {
    let mut sequences = Vec::new();
    let entries = std::fs::read_dir(dir).map_err(|_| Error::Unavailable)?;
    for entry in entries {
        let entry = entry.map_err(|_| Error::Unavailable)?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(Error::Gap);
        };
        if name.len() != 20 || !name.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(Error::Gap);
        }
        let sequence = name.parse().map_err(|_| Error::Gap)?;
        if !entry.file_type().map_err(|_| Error::Unavailable)?.is_dir() {
            return Err(Error::Gap);
        }
        sequences.push(sequence);
    }
    sequences.sort_unstable();
    if sequences.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(Error::Gap);
    }
    Ok(sequences)
}

pub(super) fn clear_staging(root: &Path) -> Result<(), Error> {
    let staging = root.join("staging");
    if !staging.exists() {
        return Ok(());
    }
    io::require_dir(&staging)?;
    let entries = std::fs::read_dir(&staging).map_err(|_| Error::Unavailable)?;
    for entry in entries {
        let entry = entry.map_err(|_| Error::Unavailable)?;
        let kind = entry.file_type().map_err(|_| Error::Unavailable)?;
        if kind.is_dir() {
            io::remove_dir(&entry.path())?;
        } else if kind.is_file() {
            io::remove_file(&entry.path())?;
        } else {
            return Err(Error::Gap);
        }
    }
    Ok(())
}

pub(super) fn retire_files(root: &Path) -> Result<(), Error> {
    clear_staging(root)?;
    io::remove_file(&root.join("pending.json"))?;
    io::remove_file(&root.join("committed.json"))
}

pub(super) fn gap(root: &Path, inner: &mut Inner) -> Result<(), Error> {
    inner.block(root, Error::Gap);
    Err(Error::Gap)
}

pub(super) fn pending_note(sequence: u64, note: &Note) -> PendingNote {
    PendingNote {
        sequence,
        mutation_id: note.mutation_id.clone(),
        plaintext_sha256: note.plaintext_sha256.clone(),
        envelope_sha256: super::super::body::sha256_hex(&note.envelope),
        file_sha256: note.file.as_deref().map(super::super::body::sha256_hex),
        envelope_bytes: note.envelope.len() as u64,
        file_bytes: note.file.as_ref().map(Vec::len).unwrap_or(0) as u64,
    }
}

pub(super) fn notes_ok(notes: &[PendingNote]) -> bool {
    let Some(first) = notes.first() else {
        return false;
    };
    first.sequence > 0
        && notes
            .windows(2)
            .all(|pair| pair[1].sequence == pair[0].sequence + 1)
        && notes.iter().all(note_ok)
}

pub(super) fn write_staged(
    root: &Path,
    notes: &[Note],
    planned: &[PendingNote],
) -> Result<(), Error> {
    for planned_note in planned {
        let note = notes
            .iter()
            .find(|note| note.mutation_id == planned_note.mutation_id)
            .ok_or(Error::Unavailable)?;
        let dir = root.join("staging").join(seq_name(planned_note.sequence));
        io::ensure_dir(&dir)?;
        io::write_json(&dir.join("meta.json"), &meta_of(planned_note))?;
        io::write_bytes(&dir.join("body.bin"), &note.envelope)?;
        if let Some(file) = note.file.as_deref() {
            io::write_bytes(&dir.join("file.bin"), file)?;
        }
    }
    Ok(())
}

pub(super) fn install(
    root: &Path,
    inner: &mut Inner,
    pending: &super::super::Pending,
) -> Result<(), Error> {
    super::ensure_tree(root)?;
    let mut added = 0u64;
    for note in &pending.notes {
        place_entry(root, note)?;
        write_id(root, note)?;
        added = added
            .saturating_add(note.envelope_bytes)
            .saturating_add(note.file_bytes);
    }
    let opened = inner
        .state
        .as_ref()
        .map(|state| state.opened_frontier)
        .unwrap_or(pending.frontier_before);
    let snapshot = inner
        .state
        .as_ref()
        .map(|state| state.snapshot_required)
        .unwrap_or(inner.history);
    let acked = inner.state.as_ref().map(|state| state.acked).unwrap_or(0);
    let retained = inner
        .state
        .as_ref()
        .map(|state| state.retained_bytes)
        .unwrap_or(0)
        .saturating_add(added);
    let head = pending.notes.last().map(|note| note.sequence).unwrap_or(0);
    inner.store(
        root,
        super::super::State {
            version: 1,
            opened_frontier: opened,
            head,
            acked,
            retained_bytes: retained,
            snapshot_required: snapshot,
            gap: false,
            armed: false,
        },
    )
}

/// A pending batch whose sequences are already durable must match those entries.
pub(super) fn require_installed(
    root: &Path,
    head: u64,
    acked: u64,
    notes: &[PendingNote],
) -> Result<(), Error> {
    for note in notes {
        if note.sequence == 0 || note.sequence > head {
            return Err(Error::Gap);
        }
        if note.sequence <= acked {
            match read_id(root, &note.mutation_id)? {
                Some(existing) if existing == note.plaintext_sha256 => {}
                _ => return Err(Error::Gap),
            }
        } else {
            verify_entry(&root.join("entries").join(seq_name(note.sequence)), note)?;
        }
    }
    Ok(())
}

fn note_ok(note: &PendingNote) -> bool {
    super::super::id_ok(&note.mutation_id)
        && super::super::sha_ok(&note.plaintext_sha256)
        && super::super::sha_ok(&note.envelope_sha256)
        && note.file_sha256.as_deref().is_none_or(super::super::sha_ok)
}

fn meta_of(note: &PendingNote) -> Meta {
    Meta {
        version: 1,
        sequence: note.sequence,
        mutation_id: note.mutation_id.clone(),
        plaintext_sha256: note.plaintext_sha256.clone(),
        envelope_sha256: note.envelope_sha256.clone(),
        file_sha256: note.file_sha256.clone(),
        envelope_bytes: note.envelope_bytes,
        file_bytes: note.file_bytes,
    }
}

fn place_entry(root: &Path, note: &PendingNote) -> Result<(), Error> {
    let name = seq_name(note.sequence);
    let dest = root.join("entries").join(&name);
    if !dest.exists() {
        let src = root.join("staging").join(&name);
        if !src.exists() {
            return Err(Error::Gap);
        }
        std::fs::rename(&src, &dest).map_err(|_| Error::Unavailable)?;
        io::sync_dir(&root.join("entries"))?;
    }
    verify_entry(&dest, note)
}

fn verify_entry(dir: &Path, note: &PendingNote) -> Result<(), Error> {
    let meta = read_meta(dir)?;
    if meta.sequence != note.sequence
        || meta.mutation_id != note.mutation_id
        || meta.plaintext_sha256 != note.plaintext_sha256
        || meta.envelope_sha256 != note.envelope_sha256
        || meta.file_sha256 != note.file_sha256
        || meta.envelope_bytes != note.envelope_bytes
        || meta.file_bytes != note.file_bytes
    {
        return Err(Error::Gap);
    }
    verified_bytes(
        &dir.join("body.bin"),
        note.envelope_bytes,
        &note.envelope_sha256,
        BODY_MAX,
    )?;
    match note.file_sha256.as_deref() {
        Some(expected) => {
            verified_bytes(&dir.join("file.bin"), note.file_bytes, expected, file_max())?;
        }
        None if note.file_bytes == 0 && !dir.join("file.bin").exists() => {}
        None => return Err(Error::Gap),
    }
    Ok(())
}

fn write_id(root: &Path, note: &PendingNote) -> Result<(), Error> {
    let path = root.join("ids").join(&note.mutation_id);
    match io::read_limited(&path, 64)? {
        Some(bytes) => {
            let text = std::str::from_utf8(&bytes).map_err(|_| Error::Gap)?;
            if text == note.plaintext_sha256 {
                Ok(())
            } else {
                Err(Error::Gap)
            }
        }
        None => io::write_bytes(&path, note.plaintext_sha256.as_bytes()),
    }
}
