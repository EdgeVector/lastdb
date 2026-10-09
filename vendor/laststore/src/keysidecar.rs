//! On-disk id sidecar for one hash group.
//!
//! A keys-only pass (`list_prefix_keys_paged`, `list_range_keys_paged`) visits
//! every group in a collection and needs nothing but ids. Without a sidecar the
//! only way to recover a non-resident group's ids is a full `load_shard`:
//! read the whole segment, parse every record, rebuild the index — on the
//! primary that is ~5.2 MB per group across 1024 groups for `atoms`, to produce
//! an id set a few percent of that size.
//!
//! The in-memory key-index cache removes the *repeat* cost of that within one
//! process. This module is its on-disk tier, so the *first* walk after a restart
//! or an eviction is cheap too.
//!
//! # This is a cache, never an authority
//!
//! Every read validates the sidecar against the group's segment files and falls
//! back to a full load on any mismatch. A stale, truncated, corrupt, or
//! hand-edited sidecar can cost time; it cannot produce a wrong id set. The two
//! validated outcomes are:
//!
//! - identical segment stamps — the ids are used as written;
//! - same segments with a longer final one — only the appended suffix is parsed
//!   and replayed over the ids (plain packaging has disk offset == plaintext
//!   offset, and a recorded length is always a record boundary).
//!
//! Anything else — a segment sequence changed by compaction, a file shorter
//! than recorded after a recovery truncate, a bad checksum — falls back.
//!
//! # Why plaintext ids are acceptable here
//!
//! Only [`crate::PackagingMode::Plain`] groups get a sidecar. Plain packaging
//! already documents group files as structurally readable and stores ids
//! verbatim in the segment, leaving body secrecy to upper layers. The sidecar
//! therefore discloses nothing the segment beside it does not. Frame-AEAD
//! groups get no sidecar rather than a plaintext id list next to the cabinet.

use crate::segfmt;
use crate::store::{fnv1a64, GroupResidue};
use crate::Result;
use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};

/// File name of a group's id sidecar, beside its `*.seg` files.
const SIDECAR_FILE: &str = "keys-v1.idx";
const SIDECAR_TMP: &str = "keys-v1.idx.tmp";
/// Current framing: stamps, then the group's record-byte residue
/// (`live_bytes`, `dead_bytes`), then the front-coded ids.
const MAGIC: &[u8; 6] = b"LSKI3\0";
/// Previous framing: stamps, then ids, with no residue. Still readable — the
/// ids are as trustworthy as ever, only the residue is unknown — so an upgrade
/// does not throw away every sidecar on the home.
const MAGIC_V2: &[u8; 6] = b"LSKI2\0";

/// One segment file's identity at snapshot time.
///
/// Length is the discriminator that makes append detectable: segments are
/// append-only between compactions, so a longer final segment means new records
/// and nothing else.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SegmentStamp {
    pub seq: u64,
    pub len: u64,
}

pub(crate) fn sidecar_path(dir: &Path) -> PathBuf {
    dir.join(SIDECAR_FILE)
}

/// Current `(seq, len)` of every `*.seg` in `dir`, ascending by seq.
pub(crate) fn segment_stamps(dir: &Path) -> Result<Vec<SegmentStamp>> {
    let mut out = Vec::new();
    if !dir.exists() {
        return Ok(out);
    }
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().and_then(|x| x.to_str()) != Some("seg") {
            continue;
        }
        let Some(seq) = path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.parse::<u64>().ok())
        else {
            continue;
        };
        out.push(SegmentStamp {
            seq,
            len: fs::metadata(&path)?.len(),
        });
    }
    out.sort_unstable_by_key(|s| s.seq);
    Ok(out)
}

/// Append `v` as a LEB128-style varint (7 bits per byte, high bit continues).
fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Read a varint at `off`, advancing it. `None` if it runs past `limit` or is
/// longer than a `u64` can hold.
fn take_varint(bytes: &[u8], off: &mut usize, limit: usize) -> Option<u64> {
    let mut result: u64 = 0;
    for shift in (0..64).step_by(7) {
        if *off >= limit {
            return None;
        }
        let byte = bytes[*off];
        *off += 1;
        result |= u64::from(byte & 0x7f).checked_shl(shift)?;
        if byte & 0x80 == 0 {
            return Some(result);
        }
    }
    None
}

/// Bytes `a` and `b` share from the front.
fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

fn encode(stamps: &[SegmentStamp], residue: GroupResidue, ids: &BTreeSet<String>) -> Vec<u8> {
    let mut out = Vec::with_capacity(MAGIC.len() + 24 + stamps.len() * 16 + ids.len() * 16 + 8);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&(stamps.len() as u32).to_le_bytes());
    for stamp in stamps {
        out.extend_from_slice(&stamp.seq.to_le_bytes());
        out.extend_from_slice(&stamp.len.to_le_bytes());
    }
    // Residue rides in the same write as the ids because both are snapshots
    // of the same critical section: the stamps that validate the ids are the
    // stamps that validate the residue.
    out.extend_from_slice(&residue.live_bytes.to_le_bytes());
    out.extend_from_slice(&residue.dead_bytes.to_le_bytes());
    out.extend_from_slice(&(ids.len() as u32).to_le_bytes());
    // Front coding. A `BTreeSet` iterates in byte-lexicographic order, so each
    // id shares a prefix with the one before it and needs to store only the
    // divergent tail. Ids in one hash group are dominated by a repeated
    // molecule/schema prefix, so on real collections this is a 40-80% cut of
    // the id block — the sidecar's whole payload.
    //
    // The shared run is measured in bytes and may split a multi-byte codepoint;
    // that is fine because `decode` rebuilds the full byte string and validates
    // UTF-8 once, on the joined id, never on a fragment.
    let mut prev: &[u8] = b"";
    for id in ids {
        let cur = id.as_bytes();
        let shared = common_prefix_len(prev, cur);
        put_varint(&mut out, shared as u64);
        put_varint(&mut out, (cur.len() - shared) as u64);
        out.extend_from_slice(&cur[shared..]);
        prev = cur;
    }
    let checksum = fnv1a64(&out);
    out.extend_from_slice(&checksum.to_le_bytes());
    out
}

/// The fixed-width part of a sidecar, before the id block.
struct SidecarHeader {
    stamps: Vec<SegmentStamp>,
    /// `None` for a v2 sidecar, which recorded no residue.
    residue: Option<GroupResidue>,
    id_count: usize,
    /// Offset the id block starts at.
    ids_offset: usize,
    /// Everything before the trailing checksum.
    body_len: usize,
}

/// Validate framing and checksum and parse the fixed-width header.
///
/// Split out from `decode_ids` so a caller that only needs the stamps or the
/// residue does not pay to rebuild the id set. Accepts the current framing
/// and the previous one (`LSKI2`, no residue field).
fn parse_header(bytes: &[u8]) -> Option<SidecarHeader> {
    if bytes.len() < MAGIC.len() + 8 {
        return None;
    }
    let has_residue = if bytes.starts_with(MAGIC) {
        true
    } else if bytes.starts_with(MAGIC_V2) {
        false
    } else {
        return None;
    };
    let body_len = bytes.len() - 8;
    let stored = u64::from_le_bytes(bytes[body_len..].try_into().ok()?);
    if stored != fnv1a64(&bytes[..body_len]) {
        return None;
    }

    let mut off = MAGIC.len();
    let mut take = |n: usize| -> Option<&[u8]> {
        let end = off.checked_add(n)?;
        if end > body_len {
            return None;
        }
        let slice = &bytes[off..end];
        off = end;
        Some(slice)
    };

    let stamp_count = u32::from_le_bytes(take(4)?.try_into().ok()?) as usize;
    let mut stamps = Vec::with_capacity(stamp_count.min(1024));
    for _ in 0..stamp_count {
        let seq = u64::from_le_bytes(take(8)?.try_into().ok()?);
        let len = u64::from_le_bytes(take(8)?.try_into().ok()?);
        stamps.push(SegmentStamp { seq, len });
    }

    let residue = if has_residue {
        let live_bytes = u64::from_le_bytes(take(8)?.try_into().ok()?);
        let dead_bytes = u64::from_le_bytes(take(8)?.try_into().ok()?);
        Some(GroupResidue {
            live_bytes,
            dead_bytes,
        })
    } else {
        None
    };

    let id_count = u32::from_le_bytes(take(4)?.try_into().ok()?) as usize;
    Some(SidecarHeader {
        stamps,
        residue,
        id_count,
        ids_offset: off,
        body_len,
    })
}

/// The stamps a sidecar in `dir` was written for, without rebuilding its ids.
///
/// `None` when there is no sidecar, or it is malformed — both mean "this run
/// has no proof the file on disk is current", so a caller must assume it is
/// not. Validates the checksum for the same reason [`read_valid`] does: a
/// corrupt sidecar must never be mistaken for an up-to-date one, or eviction
/// would skip the write that repairs it.
///
/// Also `None` for a v2 sidecar. Its ids are valid, but it carries no
/// residue, and the writers use this answer to decide whether the file
/// already says everything the current framing says. Treating a v2 file as
/// current would leave every group that was written before the upgrade
/// without residue for as long as its segments stayed unchanged — the
/// coldest groups, exactly the ones only a sidecar can answer for.
pub(crate) fn recorded_stamps(dir: &Path) -> Option<Vec<SegmentStamp>> {
    let bytes = fs::read(sidecar_path(dir)).ok()?;
    let header = parse_header(&bytes)?;
    header.residue?;
    Some(header.stamps)
}

/// What a sidecar can say about a cold group's record-byte residue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SidecarResidue {
    /// Residue as of the sidecar's stamps.
    pub residue: GroupResidue,
    /// Bytes appended to the final segment after the stamps were taken. Their
    /// live/dead split is unknown without a parse, so a caller counts them as
    /// unmeasured rather than guessing.
    pub appended_bytes: u64,
}

/// Residue for the group in `dir`, without loading the shard and without
/// rebuilding its ids.
///
/// `None` when there is no sidecar this run can trust: no file, a v2 file
/// (which recorded no residue), a corrupt file, or segments that no longer
/// match the stamps other than by a clean append to the final one. Same
/// validation as [`read_valid`], because the residue is only as current as
/// the stamps it was written with.
pub(crate) fn read_residue(dir: &Path) -> Option<SidecarResidue> {
    let bytes = fs::read(sidecar_path(dir)).ok()?;
    let header = parse_header(&bytes)?;
    let residue = header.residue?;
    let current = segment_stamps(dir).ok()?;
    let recorded = header.stamps;
    if current.len() != recorded.len() {
        return None;
    }
    let Some((current_last, current_head)) = current.split_last() else {
        return Some(SidecarResidue {
            residue,
            appended_bytes: 0,
        });
    };
    let (recorded_last, recorded_head) = recorded.split_last()?;
    if current_head != recorded_head || current_last.seq != recorded_last.seq {
        return None;
    }
    if current_last.len < recorded_last.len {
        return None;
    }
    Some(SidecarResidue {
        residue,
        appended_bytes: current_last.len - recorded_last.len,
    })
}

/// A selection within a group. This bounds decoded keys, not the encoded
/// legacy file or the work needed to validate all its front-coded frames.
#[derive(Clone, Copy)]
pub(crate) struct KeyWindow<'a> {
    pub start: &'a str,
    pub end: Option<&'a str>,
    pub after_id: Option<&'a str>,
    pub limit: usize,
}

impl KeyWindow<'_> {
    fn contains(self, id: &str) -> bool {
        id >= self.start
            && self.end.map_or(true, |end| id < end)
            && self.after_id.map_or(true, |after| id > after)
    }
}

fn decode_ids(
    bytes: &[u8],
    header: &SidecarHeader,
    window: Option<KeyWindow<'_>>,
) -> Option<BTreeSet<String>> {
    let mut off = header.ids_offset;
    let body_len = header.body_len;
    let limit = window.map_or(usize::MAX, |window| window.limit);
    let mut ids = BTreeSet::<String>::new();
    // Reuse one byte buffer, including when the shared prefix splits a UTF-8
    // codepoint. Validate every reconstructed key, even beyond the window.
    let mut prev: Vec<u8> = Vec::new();
    for _ in 0..header.id_count {
        let shared = usize::try_from(take_varint(bytes, &mut off, body_len)?).ok()?;
        let suffix_len = usize::try_from(take_varint(bytes, &mut off, body_len)?).ok()?;
        if shared > prev.len() {
            return None;
        }
        let end = off.checked_add(suffix_len)?;
        if end > body_len {
            return None;
        }
        prev.truncate(shared);
        prev.extend_from_slice(&bytes[off..end]);
        off = end;
        let id = std::str::from_utf8(&prev).ok()?;
        if limit == 0 || window.is_some_and(|window| !window.contains(id)) {
            continue;
        }
        // Old readers sort and deduplicate even a checksum-valid out-of-order
        // stream. Preserve that behavior without allocating keys we discard.
        if ids.len() == limit && ids.last().is_some_and(|last| id >= last.as_str()) {
            continue;
        }
        ids.insert(id.to_owned());
        if ids.len() > limit {
            ids.pop_last();
        }
    }
    if off != body_len {
        return None;
    }
    Some(ids)
}

/// Write `ids` and `residue` for the group in `dir`, valid as of `stamps`.
///
/// Atomic: a torn write leaves the previous sidecar intact rather than a
/// half-file. `stamps` must have been taken while holding the shard lock, in
/// the same critical section as `ids` and `residue` — if the group is appended
/// to afterwards the reader detects the longer segment and replays the
/// difference.
pub(crate) fn write(
    dir: &Path,
    stamps: &[SegmentStamp],
    residue: GroupResidue,
    ids: &BTreeSet<String>,
) -> Result<()> {
    let encoded = encode(stamps, residue, ids);
    let tmp = dir.join(SIDECAR_TMP);
    fs::write(&tmp, &encoded)?;
    let file = OpenOptions::new().write(true).open(&tmp)?;
    crate::durability::sync_dirty_file(&file)?;
    drop(file);
    fs::rename(&tmp, sidecar_path(dir))?;
    Ok(())
}

/// Ids for the group in `dir`, or `None` if there is no sidecar this run can
/// trust and the caller must load the shard.
pub(crate) fn read_valid(dir: &Path) -> Option<BTreeSet<String>> {
    read_valid_with_window(dir, None)
}

pub(crate) fn read_valid_window(dir: &Path, window: KeyWindow<'_>) -> Option<BTreeSet<String>> {
    read_valid_with_window(dir, Some(window))
}

fn read_valid_with_window(dir: &Path, window: Option<KeyWindow<'_>>) -> Option<BTreeSet<String>> {
    let bytes = fs::read(sidecar_path(dir)).ok()?;
    let header = parse_header(&bytes)?;
    if let Some(window) = window {
        if segment_stamps(dir).ok()? == header.stamps {
            let ids = decode_ids(&bytes, &header, Some(window))?;
            // Preserve the original post-decode stamp check. A concurrent
            // append must take the complete suffix-replay path below.
            if segment_stamps(dir).ok()? == header.stamps {
                return Some(ids);
            }
        }
    }
    // Do not truncate before suffix replay: a delete from a full page must
    // expose the next surviving key. This uncommon compatibility path retains
    // the complete set until every appended put/delete is applied.
    let mut ids = decode_ids(&bytes, &header, None)?;
    let recorded = header.stamps;
    let current = segment_stamps(dir).ok()?;

    if current.len() != recorded.len() {
        return None;
    }
    let Some((current_last, current_head)) = current.split_last() else {
        // No segments on either side: an empty group, correctly described by an
        // empty id set.
        return Some(ids);
    };
    let (recorded_last, recorded_head) = recorded.split_last()?;

    // Every sealed segment must be byte-identical. Only the final one may have
    // grown, and only by appended records.
    if current_head != recorded_head || current_last.seq != recorded_last.seq {
        return None;
    }
    if current_last.len < recorded_last.len {
        return None;
    }
    if current_last.len > recorded_last.len {
        replay_suffix(dir, *recorded_last, current_last.len, &mut ids)?;
    }
    if let Some(window) = window {
        ids = ids
            .into_iter()
            .filter(|id| window.contains(id))
            .take(window.limit)
            .collect();
    }
    Some(ids)
}

/// Ids for the group in `dir`, tolerating segments appended *after* the
/// sidecar was written — the shape a dead group is in after a crash.
///
/// [`read_valid`] answers `None` unless every recorded segment is byte-identical
/// and at most the final one grew, because its caller has a cheap authority to
/// fall back to (a `load_shard`). This reader exists for the one caller that
/// must never fall back: `LastStore::drop_hash_group_dir`, which has to prove
/// a 39 GB group holds a single id without loading it (primary restart loop,
/// 2026-09-21, `metadata/0/g/025`). A sidecar is written on eviction, so a
/// group whose owner was killed mid-append has a sidecar that trails its
/// segments by a few files. Those files are replayed in full here, one at a
/// time, so memory stays one segment wide.
///
/// Still `None` when the recorded stamps are not a prefix of what is on disk:
/// a sealed segment that changed length, a recorded sequence that vanished, or
/// any record that does not parse. That is a group the sidecar cannot vouch
/// for, and the caller refuses.
pub(crate) fn read_ids_tolerating_appends(dir: &Path) -> Option<BTreeSet<String>> {
    let bytes = fs::read(sidecar_path(dir)).ok()?;
    let header = parse_header(&bytes)?;
    let mut ids = decode_ids(&bytes, &header, None)?;
    let recorded = header.stamps;
    let current = segment_stamps(dir).ok()?;
    if current.len() < recorded.len() {
        return None;
    }
    let (covered, appended) = current.split_at(recorded.len());
    // Every recorded segment must still be there with at least its recorded
    // length, and all but the last must be byte-identical.
    if let Some((recorded_last, recorded_head)) = recorded.split_last() {
        let (covered_last, covered_head) = covered.split_last()?;
        if covered_head != recorded_head || covered_last.seq != recorded_last.seq {
            return None;
        }
        if covered_last.len < recorded_last.len {
            return None;
        }
        if covered_last.len > recorded_last.len {
            let is_final = appended.is_empty();
            replay_range(
                &dir.join(format!("{:010}.seg", recorded_last.seq)),
                recorded_last.len,
                covered_last.len,
                &mut ids,
                is_final,
            )?;
        }
    }
    for (index, stamp) in appended.iter().enumerate() {
        let is_final = index + 1 == appended.len();
        replay_range(
            &dir.join(format!("{:010}.seg", stamp.seq)),
            0,
            stamp.len,
            &mut ids,
            is_final,
        )?;
    }
    Some(ids)
}

/// Apply the records in `path[from..to)` onto `ids`.
///
/// `allow_torn_tail` admits one trailing partial record, and only that: the
/// shape an append-only segment is left in when its writer is killed mid
/// write (the authoritative `load_shard` truncates it on the next open). A
/// torn record was never acknowledged, so it cannot name a live id. Any other
/// parse surprise is `None`.
fn replay_range(
    path: &Path,
    from: u64,
    to: u64,
    ids: &mut BTreeSet<String>,
    allow_torn_tail: bool,
) -> Option<()> {
    use std::io::{Read, Seek, SeekFrom};

    let mut file = fs::File::open(path).ok()?;
    file.seek(SeekFrom::Start(from)).ok()?;
    let mut bytes = vec![0u8; usize::try_from(to.checked_sub(from)?).ok()?];
    file.read_exact(&mut bytes).ok()?;
    let mut off = 0usize;
    while off < bytes.len() {
        match segfmt::parse_at(&bytes, off).ok()? {
            Some(rec) => {
                if rec.is_put {
                    ids.insert(rec.id);
                } else {
                    ids.remove(&rec.id);
                }
                off += rec.raw_len;
            }
            None if allow_torn_tail => return Some(()),
            None => return None,
        }
    }
    Some(())
}

/// Apply records appended to `stamp`'s segment beyond `stamp.len` onto `ids`.
///
/// `None` on any surprise: a short read, a record that does not parse, or a
/// trailing partial record. All of those mean the file is not the clean append
/// the stamps implied, so the caller must fall back rather than guess.
fn replay_suffix(
    dir: &Path,
    stamp: SegmentStamp,
    current_len: u64,
    ids: &mut BTreeSet<String>,
) -> Option<()> {
    use std::io::{Read, Seek, SeekFrom};

    let path = dir.join(format!("{:010}.seg", stamp.seq));
    let mut file = fs::File::open(path).ok()?;
    file.seek(SeekFrom::Start(stamp.len)).ok()?;
    let mut suffix = vec![0u8; usize::try_from(current_len - stamp.len).ok()?];
    file.read_exact(&mut suffix).ok()?;

    let mut off = 0usize;
    while off < suffix.len() {
        // A recorded length is always a record boundary (it is the flushed
        // `file_len`), so the suffix starts cleanly and every record in it must
        // parse whole.
        let rec = segfmt::parse_at(&suffix, off).ok()??;
        if rec.is_put {
            ids.insert(rec.id);
        } else {
            ids.remove(&rec.id);
        }
        off += rec.raw_len;
    }
    if off != suffix.len() {
        return None;
    }
    Some(())
}
