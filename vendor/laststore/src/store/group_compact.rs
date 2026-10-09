//! Plain-packaging group rewrite, and the walk that rewrites only the groups
//! that hold dead bytes.
//!
//! A rewrite swaps a group's segment files. Once a plane holds the only copy of
//! its rows (the `cas_blobs` file-blob plane), the order of the disk steps is
//! the crash story, so it is written down here:
//!
//! 1. Build the new segment image in memory. Nothing on disk changes.
//! 2. Remove the group key sidecar and any leftover `*.seg.tmp`.
//! 3. Write and sync `<new>.seg.tmp`, then rename it to `<new>.seg`.
//! 4. Sync the directory. From here the new segment is durable.
//! 5. Switch the in-memory group to the new segment.
//! 6. Remove the old segments, lowest sequence first, and stop at the first
//!    error (a missing file is not an error).
//! 7. Sync the directory again.
//!
//! The new segment has the highest sequence, so replay applies it last. A
//! crash after step 4 leaves the old segments, the new one, or the new one plus
//! a suffix of the old ones. Every one of those replays to the same live set.
//! A crash before step 4 leaves the old segments, and at most a stray `.tmp`.

use super::{encode_segment_payload, sync_dir, LastStore, Loc, Shard};
use crate::segfmt::encode_put;
use crate::{durability, keysidecar, Result};
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::ErrorKind;
use std::path::Path;
use uuid::Uuid;

/// Record one disk step in the test trace.
fn step(_event: impl FnOnce() -> String) {}

impl LastStore {
    /// Rewrite the hash groups of `collection` that hold dead bytes, and no
    /// others. Returns the number of groups rewritten.
    ///
    /// [`Self::compact_collection`] rewrites every group on disk, dirty or not.
    /// For a plane whose rows are the only copy (`cas_blobs`) that puts clean
    /// groups inside the rewrite's crash window and costs a full read and write
    /// of the plane for nothing. This walk loads each group once (a read) and
    /// takes its exact in-memory residue: a group with `dead_bytes == 0` is left
    /// byte-identical. A `never_compact` collection is a no-op.
    pub fn compact_collection_dead_groups(&self, collection: &str) -> Result<u64> {
        if self.opts.collection_policy(collection).never_compact {
            return Ok(0);
        }
        let mut rewritten = 0u64;
        for (shard, group) in self.handles_on_disk(collection)? {
            let handle = self.scan_handle_by_key(&(collection.to_string(), shard, group))?;
            let mut sh = handle.handle().lock().expect("poison");
            if sh.residue.dead_bytes == 0 {
                continue;
            }
            Self::compact_shard(&mut sh)?;
            rewritten += 1;
        }
        Ok(rewritten)
    }
}

/// Plain packaging: rewrite the group's live records into one new segment.
///
/// See the module comment for the disk order. On an error before step 5 the
/// in-memory group is unchanged and every old segment is still on disk.
pub(super) fn compact_plain_shard(sh: &mut Shard) -> Result<()> {
    if sh.segments.is_empty() {
        return Ok(());
    }
    LastStore::sync_open(sh)?;
    sh.open_file = None;
    let old = sh.segments.clone();
    let new_seq = old.last().copied().unwrap_or(0) + 1;
    // Compaction reads every live record, and a trimmed group would serve
    // each one from its own `File::open`. Pull the sealed segments back in
    // for the duration: they are about to be rewritten and deleted anyway,
    // and `new_buf` below already holds the whole group in memory, so this
    // does not change compaction's memory class. `seg_bytes` is cleared
    // again at the end, and on every error path.
    for &seq in &old {
        if Some(seq) == sh.segments.last().copied() || sh.seg_bytes.contains_key(&seq) {
            continue;
        }
        let path = sh.dir.join(format!("{seq:010}.seg"));
        match fs::read(&path) {
            Ok(bytes) => {
                sh.seg_bytes.insert(seq, bytes);
            }
            // A segment that cannot be read here is not fatal yet: the
            // per-record path reports the miss with the id that needed it.
            Err(_) => continue,
        }
    }
    let staged = stage_new_segment(sh, new_seq);
    let (new_index, new_buf, chunk_uuid, next_frame_counter) = match staged {
        Ok(staged) => staged,
        Err(error) => {
            sh.seg_bytes.clear();
            return Err(error);
        }
    };
    sh.segments = if new_index.is_empty() {
        vec![]
    } else {
        vec![new_seq]
    };
    sh.replace_index(new_index);
    sh.open_len = new_buf.len() as u64;
    sh.file_len = new_buf.len() as u64;
    // The whole rewritten segment is on disk and fsynced above, so hand it
    // to the trim pass as reclaimable rather than pinning it: `open_buf`
    // still starts at 0, and every byte in it is below `file_len`.
    sh.open_buf = new_buf;
    sh.open_buf_base = 0;
    sh.open_chunk_uuid = chunk_uuid;
    sh.next_frame_counter = next_frame_counter;
    sh.seg_bytes.clear();
    sh.dirty_ops = 0;
    sh.dirty_bytes = 0;
    // The in-memory group is now the new segment, so a failure below must not
    // leave the old one looking current. Report the first error, after the sync.
    let removed = remove_old_segments(&sh.dir, &old);
    let synced = sync_dir(&sh.dir);
    step(|| "sync_dir:old".to_string());
    removed.and(synced)
}

/// New index, new segment image, its chunk id, and its next frame counter.
type Staged = (BTreeMap<String, Loc>, Vec<u8>, Option<Uuid>, u64);

/// Steps 1 to 4 of the module comment. On `Ok`, the new segment (when the
/// group has live records) is renamed into place and the directory is synced.
/// On `Err`, no new segment is left behind and no old segment was touched.
fn stage_new_segment(sh: &mut Shard, new_seq: u64) -> Result<Staged> {
    let mut new_index = BTreeMap::new();
    let mut new_buf = Vec::new();
    let entries = sh.live_locations()?;
    for (id, loc) in entries {
        let body = LastStore::read_at(sh, loc)?;
        let line = encode_put(&id, &body)?;
        let offset = new_buf.len() as u64;
        new_buf.extend_from_slice(&line);
        new_index.insert(
            id,
            Loc::Legacy {
                seg: new_seq,
                offset,
                len: line.len() as u64,
            },
        );
    }
    // The key sidecar names the segments it was written for. After a rewrite
    // that empties the group, the next first segment can have the same
    // sequence and length as the one the sidecar describes, and a stale
    // sidecar would then validate against it. Drop it before any segment
    // changes. It is a cache; a load without it parses the group.
    remove_if_present(&keysidecar::sidecar_path(&sh.dir))?;
    step(|| "sidecar".to_string());
    remove_stale_tmp(&sh.dir);
    if new_buf.is_empty() {
        return Ok((new_index, new_buf, None, 0));
    }
    let path = sh.dir.join(format!("{new_seq:010}.seg"));
    let tmp = sh.dir.join(format!("{new_seq:010}.seg.tmp"));
    let (disk_bytes, chunk_uuid, next_frame_counter) = encode_segment_payload(sh, 0, &new_buf)?;
    if let Err(error) = write_and_rename(&tmp, &path, &disk_bytes) {
        let _ = fs::remove_file(&tmp);
        return Err(error);
    }
    // The old segments are only removed once the new name is durable. If the
    // directory cannot be synced, take the new segment back out: the old ones
    // are all still in place, and the group keeps appending to the old last.
    if let Err(error) = sync_dir_after_rename(&sh.dir) {
        let _ = fs::remove_file(&path);
        return Err(error);
    }
    Ok((new_index, new_buf, chunk_uuid, next_frame_counter))
}

fn write_and_rename(tmp: &Path, path: &Path, bytes: &[u8]) -> Result<()> {
    fs::write(tmp, bytes)?;
    let file = OpenOptions::new().write(true).open(tmp)?;
    durability::sync_dirty_file(&file)?;
    drop(file);
    fs::rename(tmp, path)?;
    step(|| "rename".to_string());
    Ok(())
}

/// Step 4: the new segment's name is durable before any old segment goes.
fn sync_dir_after_rename(dir: &Path) -> Result<()> {
    sync_dir(dir)?;
    step(|| "sync_dir:new".to_string());
    Ok(())
}

/// Remove the old segments lowest sequence first, stopping at the first error.
/// A prefix of the old segments is a state replay handles; an arbitrary subset
/// is not (a delete marker could go while the put it covers stays).
fn remove_old_segments(dir: &Path, old: &[u64]) -> Result<()> {
    for seq in old {
        let injected = false;
        let removed = if injected {
            Err(std::io::Error::other("injected unlink failure").into())
        } else {
            remove_if_present(&dir.join(format!("{seq:010}.seg")))
        };
        removed?;
        step(|| format!("unlink:{seq}"));
    }
    Ok(())
}

fn remove_if_present(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

/// A crash between the write and the rename leaves `<seq>.seg.tmp`. Load reads
/// only `*.seg`, so it is dead weight that also counts toward the group's
/// on-disk estimate. Best effort: a failure here does not stop the rewrite.
fn remove_stale_tmp(dir: &Path) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let is_tmp = path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".seg.tmp"));
        if is_tmp {
            let _ = fs::remove_file(&path);
        }
    }
}
