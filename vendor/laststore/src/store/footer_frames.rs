use super::*;

pub(super) fn encode_footer_record(sh: &Shard, chunk_uuid: Uuid) -> Result<Vec<u8>> {
    let mut frame_entries = sh
        .frame_locs
        .iter()
        .filter_map(|(&(uuid, frame_idx), loc)| {
            (uuid == chunk_uuid).then_some((frame_idx, loc.disk_offset, loc.disk_len))
        })
        .collect::<Vec<_>>();
    frame_entries.sort_by_key(|(frame_idx, _, _)| *frame_idx);

    let mut index_entries = sh
        .index
        .iter()
        .filter_map(|(id, loc)| match (*loc)? {
            Loc::Chunk {
                chunk_uuid: uuid,
                frame_idx,
                offset_in_frame,
                len,
            } if uuid == chunk_uuid => Some((id.clone(), frame_idx, offset_in_frame, len)),
            _ => None,
        })
        .collect::<Vec<_>>();
    index_entries.sort_by(|a, b| a.0.cmp(&b.0));

    let mut chunk_mutations = BTreeMap::new();
    let mut off = 0usize;
    while off < sh.open_buf.len() {
        let rec = segfmt::parse_at(&sh.open_buf, off)?
            .ok_or_else(|| Error::Corrupt("bad footer source records".into()))?;
        off += rec.raw_len;
        chunk_mutations.insert(rec.id, rec.is_put);
    }
    let tombstones = chunk_mutations
        .into_iter()
        .filter_map(|(id, is_put)| (!is_put).then_some(id))
        .collect::<Vec<_>>();

    let mut out = Vec::new();
    out.extend_from_slice(FOOTER_RECORD_MAGIC);
    out.extend_from_slice(&(frame_entries.len() as u64).to_le_bytes());
    for (frame_idx, disk_offset, disk_len) in frame_entries {
        out.extend_from_slice(&frame_idx.to_le_bytes());
        out.extend_from_slice(&disk_offset.to_le_bytes());
        out.extend_from_slice(&disk_len.to_le_bytes());
    }
    out.extend_from_slice(&(index_entries.len() as u64).to_le_bytes());
    for (id, frame_idx, offset_in_frame, len) in index_entries {
        let id_bytes = id.as_bytes();
        let id_len = u32::try_from(id_bytes.len())
            .map_err(|_| Error::Corrupt("footer id too long".into()))?;
        out.extend_from_slice(&id_len.to_le_bytes());
        out.extend_from_slice(id_bytes);
        out.extend_from_slice(&frame_idx.to_le_bytes());
        out.extend_from_slice(&offset_in_frame.to_le_bytes());
        out.extend_from_slice(&len.to_le_bytes());
    }
    out.extend_from_slice(&(tombstones.len() as u64).to_le_bytes());
    for id in tombstones {
        let id_bytes = id.as_bytes();
        let id_len = u32::try_from(id_bytes.len())
            .map_err(|_| Error::Corrupt("footer id too long".into()))?;
        out.extend_from_slice(&id_len.to_le_bytes());
        out.extend_from_slice(id_bytes);
    }
    Ok(out)
}

pub(super) fn write_footer_trailer(
    f: &mut File,
    footer_offset: u64,
    footer_len: u64,
) -> Result<()> {
    let mut trailer = [0u8; FOOTER_TRAILER_LEN];
    trailer[..FOOTER_TRAILER_MAGIC.len()].copy_from_slice(FOOTER_TRAILER_MAGIC);
    trailer[8..16].copy_from_slice(&footer_offset.to_le_bytes());
    trailer[16..24].copy_from_slice(&footer_len.to_le_bytes());
    f.write_all(&trailer)?;
    Ok(())
}

pub(super) fn is_footer_trailer_at(bytes: &[u8], off: usize) -> bool {
    bytes.len().saturating_sub(off) == FOOTER_TRAILER_LEN
        && bytes[off..].starts_with(FOOTER_TRAILER_MAGIC)
}

pub(super) fn record_frame(
    sh: &mut Shard,
    key: FrameKey,
    path: PathBuf,
    disk_offset: u64,
    disk_len: u64,
    payload: &[u8],
) {
    sh.insert_frame_loc(
        key,
        FrameDiskLoc {
            path,
            disk_offset,
            disk_len,
        },
    );
    if !sh.uses_sorted_index() {
        insert_frame_cache(sh, key, payload.to_vec());
    }
}

pub(super) fn insert_frame_cache(sh: &mut Shard, key: FrameKey, payload: Vec<u8>) {
    if !sh.frame_cache.contains_key(&key) {
        sh.frame_cache_order.push_back(key);
    }
    sh.frame_cache.insert(key, payload);
    while sh.frame_cache_order.len() > FRAME_CACHE_LIMIT {
        if let Some(old) = sh.frame_cache_order.pop_front() {
            sh.frame_cache.remove(&old);
        }
    }
}

/// Read one record's byte range out of a plain segment file.
///
/// Serves both a sealed segment and the flushed prefix of the open one — under
/// plain packaging both are the same thing to a `pread`, and `open_buf_base`
/// never advances past `file_len`, so a range below the base is on disk.
///
/// Only valid for `data_key.is_none()`: under frame AEAD the file bytes are
/// encoded frames and a record's plaintext offset is not a file offset.
pub(super) fn read_segment_range(dir: &Path, seg: u64, offset: u64, len: u64) -> Result<Vec<u8>> {
    let path = dir.join(format!("{seg:010}.seg"));
    let mut f = File::open(&path)
        .map_err(|e| Error::Corrupt(format!("missing seg {seg} at {}: {e}", path.display())))?;
    f.seek(SeekFrom::Start(offset))?;
    let mut buf = vec![0u8; len as usize];
    f.read_exact(&mut buf).map_err(|e| {
        Error::Corrupt(format!(
            "short read of {len} bytes at {offset} in seg {seg}: {e}"
        ))
    })?;
    Ok(buf)
}

/// Drop the parts of a resident group that are pure read cache.
///
/// These maps are byte-for-byte reproducible from files this store has already
/// written: `seg_bytes` holds sealed segments verbatim, `values` holds bodies
/// that `index` can address, and `frame_cache` holds decoded immutable frames.
/// None is needed for correctness once [`LastStore::read_at`] can read the
/// record back off disk.
///
/// So is the open tail, up to `file_len`. What is load-bearing about `open_buf`
/// is only the part past it — appends no file has yet. A tail that has been
/// spilled in full is reproducible like any sealed segment, and giving it up is
/// what takes a resident group from `max_segment_bytes` down to about its
/// index.
///
/// Returns whether anything was released.
pub(super) fn trim_shard_read_caches(sh: &mut Shard) -> bool {
    let mut trimmed = false;
    // Under frame AEAD the segment bytes are the decrypted plaintext and there
    // is no offset-addressable file to read them back from.
    if sh.data_key.is_none() && !sh.seg_bytes.is_empty() {
        // Reassign rather than `clear`, so the map's own table is returned to
        // the allocator too and the process gives the memory back.
        sh.seg_bytes = HashMap::new();
        trimmed = true;
    }
    if !sh.values.is_empty() {
        sh.values = HashMap::new();
        trimmed = true;
    }
    if !sh.frame_cache.is_empty() || !sh.frame_cache_order.is_empty() {
        sh.frame_cache = HashMap::new();
        sh.frame_cache_order = VecDeque::new();
        trimmed = true;
    }
    if open_tail_is_reproducible(sh) {
        // Every byte in the buffer is below `file_len`, so `read_at` can pread
        // any of them. Advance the base to the end of what is on disk and drop
        // the allocation; the next append starts a fresh, small buffer there.
        sh.open_buf_base = sh.file_len;
        sh.open_buf = Vec::new();
        trimmed = true;
    } else if open_buf_slack(&sh.open_buf) > 0 {
        // Some of the tail is unflushed and load-bearing, but the slack the
        // doubling growth left past it is not, and the warm set is charged for
        // capacity.
        sh.open_buf.shrink_to_fit();
        trimmed = true;
    }
    trimmed
}

/// Whether the whole open tail is already on disk and can be read back.
///
/// Under frame AEAD it never is: `open_buf` is plaintext, the file holds
/// encoded frames, and a record's offset in the buffer is not a file offset.
pub(super) fn open_tail_is_reproducible(sh: &Shard) -> bool {
    sh.data_key.is_none()
        && !sh.open_buf.is_empty()
        && sh.file_len == sh.open_buf_base + sh.open_buf.len() as u64
}

/// Reclaimable slack past the open tail's contents.
///
/// Ignored below a threshold so that `shrink_to_fit` — which is free to leave
/// the allocation a little larger than `len` — cannot leave a permanent scrap
/// of "trimmable" behind and keep waking the trim pass to reclaim nothing.
pub(super) fn open_buf_slack(open_buf: &Vec<u8>) -> u64 {
    const MIN_RECLAIM: u64 = 64 * 1024;
    let slack = (open_buf.capacity() as u64).saturating_sub(open_buf.len() as u64);
    if slack >= MIN_RECLAIM {
        slack
    } else {
        0
    }
}

/// Grow `open_buf` in doubling steps, capped at the segment size.
///
/// A flat 4 MiB reservation on first append made every group that had ever
/// taken a write cost the warm budget 4 MiB, nearly all of it capacity that
/// would never be filled. Doubling keeps appends amortised without
/// over-reserving a group that holds a few kilobytes.
pub(super) fn grow_open_buf_for_append(open_buf: &mut Vec<u8>, extra: usize, max_seg: u64) {
    let needed = open_buf.len() + extra;
    if open_buf.capacity() < needed {
        let target = needed.saturating_mul(2).min(max_seg as usize).max(needed);
        open_buf.reserve_exact(target - open_buf.len());
    }
}
