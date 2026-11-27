//! Commit-log frame replay, tombstone-pressure recovery, retired-log pruning, and corruption checks.

use super::{file_path, LogFiles, LOG, MAX_FILES, SUFFIX};
use crate::disk_run::codec::{corrupt, decode_block, BlockRecords};
use crate::disk_run::home::manifest::Manifest;
use crate::disk_run::home::memory::Memory;
use crate::disk_run::home::seal::hidden_bytes;
use crate::disk_run::Found;
use crate::durability::{sync_directory, sync_dirty_file};
use crate::frame::{decode_frame, encoded_len, MAX_DECOMPRESSED_FRAME_BYTES};
use crate::{Error, Result};
use std::fs::{self, OpenOptions};
use std::path::Path;

/// Rebuild the memory layer from the log of the home at `root`, whose CURRENT holds every
/// commit up to `manifest.frontier`. `None` for the files when the home has no log.
pub(in crate::disk_run::home) fn replay(
    root: &Path,
    key: &[u8; 32],
    manifest: &Manifest,
    prune: bool,
) -> Result<(Memory, Option<LogFiles>)> {
    let sealed = manifest.frontier;
    let dir = root.join(LOG);
    let entries = match fs::read_dir(&dir) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((Memory::default(), None)),
        res => res?,
    };
    let mut seqs = Vec::new();
    for entry in entries {
        if seqs.len() >= MAX_FILES {
            return Err(Error::WriteCapacity("commit log has too many files".into()));
        }
        let name = entry?.file_name();
        let text = name.to_string_lossy();
        seqs.extend(
            text.strip_suffix(SUFFIX)
                .and_then(|s| s.parse::<u64>().ok()),
        );
    }
    seqs.sort_unstable();
    let (mut frames, mut scanned, mut max_last) = (Vec::new(), Vec::with_capacity(seqs.len()), 0);
    for &seq in &seqs {
        let bytes = fs::read(file_path(&dir, seq))?;
        let (last, valid_len) = read_frames(key, &bytes, sealed, &mut frames)?;
        max_last = max_last.max(last);
        scanned.push((seq, last, valid_len, bytes.len()));
    }
    let mut torn_files = 0usize;
    for &(_, last, valid_len, total_len) in &scanned {
        if valid_len < total_len {
            torn_files += 1;
            // Only the one active tip file may have a torn tail: either after `max_last`, or on
            // the first append to a newly switched file (`last == 0`; any lost unsealed commit
            // below `max_last` fails the consecutive-frontier check below).
            if torn_files > 1 || (last > 0 && last < max_last) {
                return Err(corrupt("commit log: an unreadable logged commit"));
            }
        }
    }
    frames.sort_unstable_by_key(|(ctr, _, _)| *ctr);
    let mut memory = Memory::default();
    for (n, (ctr, records, cost)) in (1u64..).zip(frames) {
        if Some(ctr) != sealed.checked_add(n) {
            return Err(corrupt("commit log: a logged commit is missing"));
        }
        let (mut tombstones, mut key_bytes) = (0u64, 0u64);
        for (rk, found) in &records {
            if matches!(found, Found::Tombstone) {
                tombstones += 1;
                key_bytes = key_bytes.saturating_add(rk.len() as u64);
            }
        }
        let hidden = key_bytes.saturating_add(hidden_bytes(manifest, tombstones));
        memory.push(records, cost, hidden);
    }
    let files = if prune {
        prune_replayed(&dir, sealed, scanned)?
    } else {
        scanned
            .into_iter()
            .map(|(seq, last, _, _)| (seq, last))
            .collect()
    };
    let next_seq = seqs.last().map_or(1, |s| s + 1);
    Ok((memory, Some(LogFiles { next_seq, files })))
}

fn prune_replayed(
    dir: &Path,
    sealed: u64,
    scanned: Vec<(u64, u64, usize, usize)>,
) -> Result<Vec<(u64, u64)>> {
    let (mut files, mut unlinked) = (Vec::new(), false);
    for (seq, last, valid_len, total_len) in scanned {
        let path = file_path(dir, seq);
        if last <= sealed {
            match fs::remove_file(path) {
                Ok(()) => unlinked = true,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        } else {
            if valid_len < total_len {
                let file = OpenOptions::new().write(true).open(&path)?;
                file.set_len(valid_len as u64)?;
                sync_dirty_file(&file)?;
            }
            files.push((seq, last));
        }
    }
    if unlinked {
        sync_directory(dir)?;
    }
    Ok(files)
}

fn has_later_frame(key: &[u8; 32], rest: &[u8]) -> bool {
    (0..rest.len()).any(|off| {
        encoded_len(&rest[off..])
            .filter(|&len| len <= rest.len() - off)
            .is_some_and(|len| {
                decode_frame(
                    key,
                    &rest[off..off + len],
                    MAX_DECOMPRESSED_FRAME_BYTES,
                    (None, None),
                )
                .is_ok()
            })
    })
}

/// Decode the frames of one log file and keep those above `sealed`. Returns `(last_ctr, valid_len)`.
fn read_frames(
    key: &[u8; 32],
    bytes: &[u8],
    sealed: u64,
    out: &mut Vec<(u64, BlockRecords, u64)>,
) -> Result<(u64, usize)> {
    let (mut at, mut last) = (0usize, 0u64);
    while at < bytes.len() {
        let Some(len) = encoded_len(&bytes[at..]).filter(|&n| n <= bytes.len() - at) else {
            if has_later_frame(key, &bytes[at + 1..]) {
                return Err(corrupt("commit log: an unreadable logged commit"));
            }
            break;
        };
        match decode_frame(
            key,
            &bytes[at..at + len],
            MAX_DECOMPRESSED_FRAME_BYTES,
            (None, None),
        ) {
            Ok((_, ctr, payload)) => {
                if ctr > sealed {
                    out.push((ctr, decode_block(&payload)?, len as u64));
                }
                last = last.max(ctr);
                at += len;
            }
            // A valid frame after an unreadable one in the same file would be a lost commit.
            Err(_) if has_later_frame(key, &bytes[at + 1..]) => {
                return Err(corrupt("commit log: an unreadable logged commit"));
            }
            Err(_) => break,
        }
    }
    Ok((last, at))
}
