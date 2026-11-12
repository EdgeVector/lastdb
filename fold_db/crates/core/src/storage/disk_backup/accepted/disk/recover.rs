//! Crash recovery for one accepted stream. A hole becomes a gap. It is not skipped.

mod files;

use super::super::io;
use super::super::Entry;
use super::{Error, Inner, Note, Pending, PendingNote};
use std::path::Path;

pub(super) fn ensure_tree(root: &Path) -> Result<(), Error> {
    io::ensure_dir(root)?;
    io::ensure_dir(&root.join("entries"))?;
    io::ensure_dir(&root.join("ids"))?;
    io::ensure_dir(&root.join("staging"))?;
    Ok(())
}

pub(super) fn reconcile(root: &Path, inner: &mut Inner) -> Result<(), Error> {
    if io::read_json::<Pending>(&root.join("pending.json"))?.is_none() {
        files::clear_staging(root)?;
    }
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
        return if sequences.is_empty() {
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
    let live: Vec<u64> = sequences
        .into_iter()
        .filter(|sequence| *sequence > state.acked)
        .collect();
    let expected: Vec<u64> = (state.acked + 1..=state.head).collect();
    if live != expected {
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
        return clear_armed(root, inner);
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
        return match files::require_installed(root, head, acked, &pending.notes) {
            Ok(()) => files::retire_files(root),
            Err(Error::Gap) => files::gap(root, inner),
            Err(error) => Err(error),
        };
    }
    if inner.frontier < pending.frontier_before {
        return files::gap(root, inner);
    }
    if inner.frontier == pending.frontier_before {
        files::retire_files(root)?;
        return clear_armed(root, inner);
    }
    let armed = inner.state.as_ref().is_some_and(|state| state.armed);
    if armed && !files::split_committed(root, pending.frontier_before)? {
        return files::gap(root, inner);
    }
    if pending.notes[0].sequence != head + 1 {
        return files::gap(root, inner);
    }
    match files::install(root, inner, &pending) {
        Ok(()) => files::retire_files(root),
        Err(Error::Gap) => files::gap(root, inner),
        Err(error) => Err(error),
    }
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
        let Ok(weight) = files::entry_weight(root, sequence) else {
            let _ = files::gap(root, inner);
            return Err(Error::Gap);
        };
        if !items.is_empty() && held.saturating_add(weight) > files::page_budget() {
            break;
        }
        let Ok(entry) = load_entry(root, sequence) else {
            let _ = files::gap(root, inner);
            return Err(Error::Gap);
        };
        held = held.saturating_add(weight);
        items.push(entry);
    }
    let through = items.last().map_or(after, |entry| entry.sequence);
    Ok((through, items))
}

pub(super) fn load_entry(root: &Path, sequence: u64) -> Result<Entry, Error> {
    let dir = root.join("entries").join(files::seq_name(sequence));
    let meta = files::read_meta(&dir)?;
    if meta.sequence != sequence || !super::id_ok(&meta.mutation_id) {
        return Err(Error::Gap);
    }
    let envelope = files::verified_bytes(
        &dir.join("body.bin"),
        meta.envelope_bytes,
        &meta.envelope_sha256,
        files::body_max(),
    )?;
    let file_ciphertext = match meta.file_sha256 {
        Some(expected) => Some(files::verified_bytes(
            &dir.join("file.bin"),
            meta.file_bytes,
            &expected,
            files::file_max(),
        )?),
        None if meta.file_bytes == 0 && !dir.join("file.bin").exists() => None,
        None => return Err(Error::Gap),
    };
    Ok(Entry {
        sequence,
        mutation_id: meta.mutation_id,
        envelope,
        file_ciphertext,
    })
}

pub(super) fn unacked_bytes(root: &Path, through: u64, head: u64) -> Result<u64, Error> {
    let mut retained = 0u64;
    for sequence in (through + 1)..=head {
        let meta = files::read_meta(&root.join("entries").join(files::seq_name(sequence)))?;
        retained = retained
            .saturating_add(meta.envelope_bytes)
            .saturating_add(meta.file_bytes);
    }
    Ok(retained)
}

pub(super) fn release_acked(root: &Path, through: u64) -> Result<(), Error> {
    let entries = root.join("entries");
    if !entries.exists() {
        return Ok(());
    }
    for sequence in files::list_sequences(&entries)? {
        if sequence <= through {
            io::remove_dir(&entries.join(files::seq_name(sequence)))?;
        }
    }
    Ok(())
}

pub(super) fn plan(root: &Path, inner: &Inner, notes: &[Note]) -> Result<Vec<PendingNote>, Error> {
    let mut next = inner.state.as_ref().map_or(0, |state| state.head);
    let mut added = 0u64;
    let mut planned = Vec::new();
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
        match files::read_id(root, &note.mutation_id)? {
            Some(existing) if existing == note.plaintext_sha256 => continue,
            Some(_) => return Err(Error::Conflict),
            None => {}
        }
        next = next.checked_add(1).ok_or(Error::Unavailable)?;
        let file_len = note.file.as_ref().map_or(0, Vec::len) as u64;
        added = added
            .checked_add(note.envelope.len() as u64)
            .and_then(|sum| sum.checked_add(file_len))
            .ok_or(Error::Spool)?;
        planned.push(files::pending_note(next, note));
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
    frontier_before: u64,
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
        frontier_before,
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

pub(super) fn mark_committed(root: &Path, frontier_before: u64) -> Result<(), Error> {
    ensure_tree(root)?;
    files::write_committed(root, frontier_before)
}

pub(super) fn clear_armed(root: &Path, inner: &mut Inner) -> Result<(), Error> {
    io::remove_file(&root.join("committed.json"))?;
    let Some(state) = inner.state.clone() else {
        return Ok(());
    };
    if !state.armed {
        return Ok(());
    }
    let mut state = state;
    state.armed = false;
    inner.store(root, state)
}

pub(super) fn arm(root: &Path, inner: &mut Inner) -> Result<(), Error> {
    inner.refuse_gap()?;
    let mut state = inner
        .state
        .clone()
        .unwrap_or_else(|| inner.fresh(inner.frontier, inner.history));
    state.armed = true;
    inner.store(root, state)
}

pub(super) fn persist_gap(root: &Path, inner: &mut Inner) {
    if let Some(state) = inner.state.as_mut() {
        state.gap = true;
        state.armed = false;
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
            state.armed = false;
            if io::write_json(&root.join("state.json"), &state).is_ok() {
                inner.state = Some(state);
            }
        }
        Err(_) => {}
    }
}
