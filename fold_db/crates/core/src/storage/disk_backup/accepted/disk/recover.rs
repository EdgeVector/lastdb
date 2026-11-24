//! Crash recovery for one accepted stream. A hole becomes a gap. It is not skipped.

mod files;

use super::super::io;
use super::super::{Entry, EntryFile, Frontiers};
use super::{Error, Inner, Note, Pending, PendingNote};
use std::os::unix::fs::MetadataExt;
use std::path::Path;

/// How many acknowledged entries pass between two trims of the id records.
pub(super) const TRIM_EVERY: u64 = 2_500;

fn inspect_gap<T>(root: &Path, inner: &mut Inner, result: Result<T, Error>) -> Result<T, Error> {
    if matches!(result, Err(Error::Gap)) {
        let _ = files::gap(root, inner);
    }
    result
}

/// Trim the id records to the newest ones. A failed trim leaves the directory larger, never wrong.
pub(super) fn trim_ids(root: &Path, inner: &Inner) {
    let Some(state) = inner.state.as_ref() else {
        return;
    };
    let _ = files::trim_ids(root, state.head, state.acked);
}

pub(super) fn mutation_recorded(root: &Path, id: &str) -> Result<bool, Error> {
    files::read_id(root, id).map(|found| found.is_some())
}

pub(super) fn ensure_tree(root: &Path) -> Result<(), Error> {
    io::ensure_dir(root)?;
    io::ensure_dir(&root.join("entries"))?;
    io::ensure_dir(&root.join("ids"))?;
    io::ensure_dir(&root.join("staging"))?;
    Ok(())
}

pub(super) fn reconcile(root: &Path, inner: &mut Inner) -> Result<(), Error> {
    let pending = io::read_json::<Pending>(&root.join("pending.json"))?;
    if pending.is_none() {
        files::clear_staging(root)?;
    }
    // A commit that crashed while its entries moved from staging to `entries` leaves
    // some of them placed, above the head of the state. The pending record names
    // them, and `finish_pending` places the rest. Any other entry above the head is
    // a hole.
    let installing: Vec<u64> = pending
        .iter()
        .flat_map(|pending| pending.notes.iter().map(|note| note.sequence))
        .collect();
    let entries = root.join("entries");
    if !entries.exists() {
        if inner.state.as_ref().is_some_and(|state| state.head > 0) {
            return Err(Error::Gap);
        }
        return Ok(());
    }
    io::require_dir(&entries)?;
    let sequences = files::list_sequences(&entries)?;
    let Some(state) = inner.state.clone() else {
        return if sequences
            .iter()
            .all(|sequence| installing.contains(sequence))
        {
            Ok(())
        } else {
            Err(Error::Gap)
        };
    };
    if state.acked > state.head {
        return Err(Error::Gap);
    }
    for sequence in sequences
        .iter()
        .copied()
        .filter(|sequence| *sequence <= state.acked)
    {
        io::remove_dir(&entries.join(files::seq_name(sequence)))?;
    }
    let (live, beyond): (Vec<u64>, Vec<u64>) = sequences
        .into_iter()
        .filter(|sequence| *sequence > state.acked)
        .partition(|sequence| *sequence <= state.head);
    let expected: Vec<u64> = (state.acked + 1..=state.head).collect();
    if live != expected || !beyond.iter().all(|sequence| installing.contains(sequence)) {
        return Err(Error::Gap);
    }
    let mut retained = 0u64;
    for sequence in &expected {
        let meta = files::read_meta(&entries.join(files::seq_name(*sequence)))?;
        retained = retained
            .saturating_add(meta.envelope_bytes)
            .saturating_add(meta.file_bytes);
    }
    if let Some(state) = inner.state.as_mut() {
        state.retained_bytes = retained;
    }
    Ok(())
}

pub(super) fn finish_pending(root: &Path, inner: &mut Inner) -> Result<(), Error> {
    let Some(pending) = io::read_json::<Pending>(&root.join("pending.json"))? else {
        return io::remove_file(&root.join("committed.json"));
    };
    if pending.version != 1 || !files::notes_ok(&pending.notes) {
        return files::gap(root, inner);
    }
    if inner.blocked || inner.state.as_ref().is_some_and(|state| state.gap) {
        return Err(Error::Gap);
    }
    let head = inner.state.as_ref().map_or(0, |state| state.head);
    let acked = inner.state.as_ref().map_or(0, |state| state.acked);
    let last = pending.notes.last().map_or(0, |note| note.sequence);
    if head >= last {
        return inspect_gap(
            root,
            inner,
            files::require_installed(root, head, acked, &pending.notes),
        )
        .and_then(|()| files::retire_files(root));
    }
    let before_main = pending
        .main_frontier_before
        .unwrap_or(pending.frontier_before);
    if inner.main_frontier < before_main {
        return files::gap(root, inner);
    }
    if inner.main_frontier == before_main {
        if pending.notes.iter().any(|note| {
            root.join("entries")
                .join(files::seq_name(note.sequence))
                .exists()
        }) {
            return files::gap(root, inner);
        }
        return files::retire_files(root);
    }
    if pending.notes[0].sequence != head + 1 {
        return files::gap(root, inner);
    }
    let installed = files::install(root, inner, &pending);
    inspect_gap(root, inner, installed).and_then(|()| files::retire_files(root))
}

/// Read a page, stopping before the next entry would pass one in-memory file cap.
pub(super) fn read_page(
    root: &Path,
    inner: &mut Inner,
    after: u64,
    last: u64,
) -> Result<(u64, Vec<Entry>), Error> {
    let mut items = Vec::new();
    let mut held = 0u64;
    for sequence in (after + 1)..=last {
        let weight = inspect_gap(root, inner, files::entry_weight(root, sequence))?;
        if !items.is_empty() && held.saturating_add(weight) > files::page_budget() {
            break;
        }
        let entry = inspect_gap(root, inner, load_entry(root, sequence))?;
        held = held.saturating_add(weight);
        items.push(entry);
    }
    let through = items.last().map_or(after, |entry| entry.sequence);
    Ok((through, items))
}

pub(super) fn load_entry(root: &Path, sequence: u64) -> Result<Entry, Error> {
    let dir = root.join("entries").join(files::seq_name(sequence));
    let note = files::read_meta(&dir)?.note;
    if note.sequence != sequence || !super::id_ok(&note.mutation_id) {
        return Err(Error::Gap);
    }
    let signed = files::verified_bytes(
        &dir.join("body.bin"),
        note.envelope_bytes,
        &note.envelope_sha256,
        files::body_max(),
    )?;
    let file = match note.file_sha256 {
        Some(sha256) => {
            if !super::sha_ok(&sha256) {
                return Err(Error::Gap);
            }
            let path = dir.join("file.bin");
            let held = match std::fs::symlink_metadata(&path) {
                Ok(held) => held,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Err(Error::Gap);
                }
                Err(_) => return Err(Error::Unavailable),
            };
            if !held.is_file()
                || !crate::storage::disk_pack::admit::is_private_meta(&held)
                || held.nlink() != 1
                || held.len() != note.file_bytes
                || note.file_bytes > files::file_max()
            {
                return Err(Error::Gap);
            }
            Some(EntryFile {
                path,
                sha256,
                bytes: note.file_bytes,
            })
        }
        None => match std::fs::symlink_metadata(dir.join("file.bin")) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && note.file_bytes == 0 => {
                None
            }
            Err(_) if note.file_bytes == 0 => return Err(Error::Unavailable),
            _ => return Err(Error::Gap),
        },
    };
    Ok(Entry {
        sequence,
        signed,
        file,
    })
}

pub(super) fn retained_bytes(root: &Path, after: u64, through: u64) -> Result<u64, Error> {
    let entries = root.join("entries");
    let mut retained = 0u64;
    for sequence in (after + 1)..=through {
        let meta = files::read_meta(&entries.join(files::seq_name(sequence)))?;
        if meta.sequence != sequence {
            return Err(Error::Gap);
        }
        retained = retained
            .checked_add(meta.envelope_bytes)
            .and_then(|total| total.checked_add(meta.file_bytes))
            .ok_or(Error::Gap)?;
    }
    Ok(retained)
}

pub(super) fn release_acked(root: &Path, after: u64, through: u64) {
    let entries = root.join("entries");
    // A crash or transient removal failure is recovered from the persisted
    // acknowledgement when reopening the log.
    for sequence in (after + 1)..=through {
        let _ = io::remove_dir(&entries.join(files::seq_name(sequence)));
    }
}

pub(super) fn plan(root: &Path, inner: &Inner, notes: &[Note]) -> Result<Vec<PendingNote>, Error> {
    let mut next = inner.state.as_ref().map_or(0, |state| state.head);
    let mut added = 0u64;
    let mut planned = Vec::with_capacity(notes.len());
    for note in notes {
        if !super::id_ok(&note.mutation_id) || !super::sha_ok(&note.plaintext_sha256) {
            return Err(Error::Conflict);
        }
        if planned
            .iter()
            .any(|item: &PendingNote| item.mutation_id == note.mutation_id)
        {
            return Err(Error::Conflict);
        }
        if files::read_id(root, &note.mutation_id)?.is_some() {
            return Err(Error::Conflict);
        }
        next = next.checked_add(1).ok_or(Error::Unavailable)?;
        added = added.checked_add(note.byte_weight()).ok_or(Error::Spool)?;
        planned.push(files::pending_note(next, note));
    }
    if planned.len() != notes.len() {
        return Err(Error::Conflict);
    }
    let retained = inner.state.as_ref().map_or(0, |state| state.retained_bytes);
    if retained
        .checked_add(added)
        .is_none_or(|total| total > super::spool_cap())
    {
        return Err(Error::Spool);
    }
    Ok(planned)
}

pub(super) fn write_pending(
    root: &Path,
    before: Frontiers,
    notes: &[Note],
    planned: &[PendingNote],
) -> Result<(), Error> {
    ensure_tree(root)?;
    files::clear_staging(root)?;
    if let Err(error) = files::write_staged(root, notes, planned) {
        let _ = files::clear_staging(root);
        return Err(error);
    }
    let pending = Pending {
        version: 1,
        frontier_before: before.total,
        main_frontier_before: Some(before.main),
        notes: planned.to_vec(),
    };
    if let Err(error) = io::write_json(&root.join("pending.json"), &pending) {
        let _ = files::clear_staging(root);
        let _ = io::remove_file(&root.join("pending.json"));
        return Err(error);
    }
    Ok(())
}

pub(super) fn discard(root: &Path) -> Result<(), Error> {
    files::retire_files(root)
}

pub(super) fn persist_gap(root: &Path, inner: &mut Inner) {
    if let Some(state) = inner.state.as_mut() {
        state.gap = true;
        let _ = io::write_json(&root.join("state.json"), state);
        return;
    }
    match io::read_json::<super::State>(&root.join("state.json")) {
        Ok(None) => {
            let mut state = inner.fresh(inner.frontier, inner.history);
            state.gap = true;
            if ensure_tree(root).is_ok() && io::write_json(&root.join("state.json"), &state).is_ok()
            {
                inner.state = Some(state);
                inner.history = false;
            }
        }
        Ok(Some(mut state)) => {
            state.gap = true;
            if io::write_json(&root.join("state.json"), &state).is_ok() {
                inner.state = Some(state);
            }
        }
        Err(_) => {}
    }
}
