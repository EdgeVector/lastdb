use super::*;

pub(super) fn read_encrypted_frame_uncached(
    sh: &Shard,
    key: FrameKey,
    loc: &FrameDiskLoc,
) -> Result<Vec<u8>> {
    let data_key = *sh
        .data_key
        .as_ref()
        .ok_or_else(|| Error::Config("encrypted frame read without data key".into()))?;
    let mut f = File::open(&loc.path)?;
    f.seek(SeekFrom::Start(loc.disk_offset))?;
    let mut encoded = vec![0u8; loc.disk_len as usize];
    f.read_exact(&mut encoded)?;
    let decoded = if sh.uses_sorted_index() && !sh.sorted_legacy_encrypted {
        frame::decode_frame_bounded(&data_key, &encoded, SORTED_ENCRYPTED_TAIL_BYTES as usize)?
    } else {
        frame::decode_frame(&data_key, &encoded)?
    };
    if decoded.header.chunk_uuid != key.0
        || decoded.header.counter != key.1
        || decoded.header.shard != sh.shard
        || is_seal_record(&decoded.payload)
        || is_footer_record(&decoded.payload)
    {
        return Err(Error::AeadAuthFail);
    }
    Ok(decoded.payload)
}

pub(super) fn read_encrypted_frame(
    sh: &mut Shard,
    key: FrameKey,
    loc: &FrameDiskLoc,
) -> Result<Vec<u8>> {
    let payload = read_encrypted_frame_uncached(sh, key, loc)?;
    insert_frame_cache(sh, key, payload.clone());
    Ok(payload)
}

/// Encode one buffered compaction frame, append it to the rewrite file, and
/// reset the buffer for the next one.
///
/// Split out of [`compact_encrypted_shard`] so the rewrite loop can hold the
/// shard immutably here while it still reads from it through `&mut Shard`
/// between flushes.
#[allow(clippy::too_many_arguments)]
pub(super) fn flush_compact_frame(
    sh: &Shard,
    data_key: &[u8; 32],
    chunk_uuid: Uuid,
    tmp: &Path,
    chunk_path: &Path,
    file: &mut Option<File>,
    disk_offset: &mut u64,
    frame_idx: &mut u64,
    next_csn: &mut u64,
    buf: &mut Vec<u8>,
    frame_locs: &mut HashMap<FrameKey, FrameDiskLoc>,
) -> Result<()> {
    if buf.is_empty() {
        return Ok(());
    }
    let header = FrameHeader {
        chunk_uuid,
        shard: sh.shard,
        start_csn: *next_csn,
        counter: *frame_idx,
    };
    let encoded = encode_frame_for_shard(sh, data_key, header, buf)?;
    if file.is_none() {
        fs::create_dir_all(encrypted_chunks_dir(sh))?;
        *file = Some(
            OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(tmp)?,
        );
    }
    let f = file.as_mut().expect("rewrite file opened above");
    f.write_all(&encoded)?;
    // The loc records the *final* path: the caller renames `tmp` over it before
    // anything reads through these locs.
    frame_locs.insert(
        (chunk_uuid, *frame_idx),
        FrameDiskLoc {
            path: chunk_path.to_path_buf(),
            disk_offset: *disk_offset,
            disk_len: encoded.len() as u64,
        },
    );
    *disk_offset = disk_offset.saturating_add(encoded.len() as u64);
    *next_csn = next_csn.saturating_add(count_complete_records(buf, false)?);
    *frame_idx = frame_idx.saturating_add(1);
    buf.clear();
    Ok(())
}

/// Drop one frame's decoded payload from the shard's frame cache.
///
/// The cache is an LRU sized in *frames*, so during a rewrite it would
/// otherwise retain up to [`FRAME_CACHE_LIMIT`] whole source frames — bytes the
/// rewrite has already copied and will never read again.
pub(super) fn evict_frame_cache(sh: &mut Shard, key: FrameKey) {
    if sh.frame_cache.remove(&key).is_some() {
        sh.frame_cache_order.retain(|cached| *cached != key);
    }
}

// lint:fn-size-ok verbatim move from store.rs; splitting this function is separate work
pub(super) fn compact_encrypted_shard(sh: &mut Shard) -> Result<()> {
    if sh.index.is_empty() && sh.open_chunk_uuid.is_none() && sh.frame_locs.is_empty() {
        return Ok(());
    }
    LastStore::sync_open(sh)?;
    sh.open_file = None;

    let old_paths = encrypted_shard_paths(sh)?;
    let data_key = *sh
        .data_key
        .as_ref()
        .ok_or_else(|| Error::Config("encrypted compact without data key".into()))?;

    // Copy the live set in *source frame* order rather than id order. A read
    // decodes a whole frame, so visiting a frame's records together means each
    // source frame is decoded once and can be dropped as soon as its last
    // record is copied. Walking `index` in id order instead either re-decodes
    // frames or holds many of them at once.
    let mut by_source: BTreeMap<FrameKey, Vec<(String, Loc)>> = BTreeMap::new();
    let mut unframed: Vec<(String, Loc)> = Vec::new();
    for (id, loc) in sh.live_locations()? {
        match loc {
            Loc::Chunk {
                chunk_uuid,
                frame_idx,
                ..
            } => by_source
                .entry((chunk_uuid, frame_idx))
                .or_default()
                .push((id.clone(), loc)),
            // Not addressable by frame. An encrypted shard should not hold
            // these, but a mixed-packaging home must still compact.
            Loc::Legacy { .. } | Loc::Sorted { .. } => unframed.push((id.clone(), loc)),
        }
    }

    let chunk_uuid = Uuid::new_v4();
    let chunk_path = encrypted_chunk_path(sh, chunk_uuid);
    let tmp = chunk_path.with_extension("seg.tmp");
    let mut new_index: BTreeMap<String, Loc> = BTreeMap::new();
    let mut new_frame_locs: HashMap<FrameKey, FrameDiskLoc> = HashMap::new();
    let mut file: Option<File> = None;
    let mut buf: Vec<u8> = Vec::new();
    let mut disk_offset = 0u64;
    let mut frame_idx = 0u64;
    let mut next_csn = 0u64;

    let groups = std::mem::take(&mut by_source)
        .into_iter()
        .map(|(key, members)| (Some(key), members))
        .chain(std::iter::once((None, std::mem::take(&mut unframed))));
    for (source, members) in groups {
        for (id, loc) in members {
            let body = LastStore::read_at(sh, loc)?;
            let line = encode_put(&id, &body)?;
            drop(body);
            // Seal the buffered frame *before* the record that would overflow
            // it, so a single record larger than the target still gets a frame
            // of its own rather than being split.
            if !buf.is_empty() && buf.len() + line.len() > COMPACT_FRAME_TARGET_BYTES {
                flush_compact_frame(
                    sh,
                    &data_key,
                    chunk_uuid,
                    &tmp,
                    &chunk_path,
                    &mut file,
                    &mut disk_offset,
                    &mut frame_idx,
                    &mut next_csn,
                    &mut buf,
                    &mut new_frame_locs,
                )?;
            }
            let offset_in_frame = buf.len() as u64;
            buf.extend_from_slice(&line);
            new_index.insert(
                id,
                Loc::Chunk {
                    chunk_uuid,
                    frame_idx,
                    offset_in_frame,
                    len: line.len() as u64,
                },
            );
        }
        if let Some(key) = source {
            evict_frame_cache(sh, key);
        }
    }
    flush_compact_frame(
        sh,
        &data_key,
        chunk_uuid,
        &tmp,
        &chunk_path,
        &mut file,
        &mut disk_offset,
        &mut frame_idx,
        &mut next_csn,
        &mut buf,
        &mut new_frame_locs,
    )?;

    // Every source read is done, so the old addressing can go.
    sh.clear_index();
    sh.frame_cache.clear();
    sh.frame_cache_order.clear();
    sh.clear_frame_locs();
    sh.open_buf = Vec::new();
    sh.open_buf_base = 0;
    sh.open_len = 0;
    sh.file_len = 0;
    sh.open_file = None;
    sh.open_chunk_uuid = None;
    sh.next_frame_counter = 0;

    if let Some(mut f) = file {
        sh.replace_index(new_index);
        sh.replace_frame_locs(new_frame_locs);

        let end_csn = next_csn.saturating_sub(1);
        let seal_payload = encode_seal_record(end_csn);
        let seal_header = FrameHeader {
            chunk_uuid,
            shard: sh.shard,
            start_csn: end_csn.saturating_add(1),
            counter: frame_idx,
        };
        let seal_encoded = encode_frame_for_shard(sh, &data_key, seal_header, &seal_payload)?;
        f.write_all(&seal_encoded)?;
        // `encode_footer_record` derives the chunk's tombstones from
        // `sh.open_buf`, which is empty here. That is the right answer rather
        // than an omission: a rewrite emits `encode_put` for every live id and
        // no deletes, so a compacted chunk has no tombstones to record.
        let footer_payload = encode_footer_record(sh, chunk_uuid)?;
        let footer_header = FrameHeader {
            chunk_uuid,
            shard: sh.shard,
            start_csn: end_csn.saturating_add(1),
            counter: frame_idx.saturating_add(1),
        };
        let footer_encoded = encode_frame_for_shard(sh, &data_key, footer_header, &footer_payload)?;
        let footer_offset = f.metadata()?.len();
        f.write_all(&footer_encoded)?;
        write_footer_trailer(&mut f, footer_offset, footer_encoded.len() as u64)?;
        durability::sync_dirty_file(&f)?;
        drop(f);
        fs::rename(&tmp, &chunk_path)?;
        sync_dir(&encrypted_chunks_dir(sh))?;
        // The rewritten frames are deliberately *not* seeded into the frame
        // cache. Seeding it cost another full copy of the shard's live set, and
        // with bounded frames the first read back is one small decode.
    }

    for path in old_paths {
        if Some(path.as_path())
            != sh
                .open_chunk_uuid
                .map(|u| encrypted_tail_path(sh, u))
                .as_deref()
        {
            let _ = fs::remove_file(path);
        }
    }
    sync_dir(&encrypted_chunks_dir(sh))?;
    sync_dir(&encrypted_tail_dir(sh))?;
    sync_dir(&sh.dir)?;
    sh.dirty_ops = 0;
    sh.dirty_bytes = 0;
    Ok(())
}

pub(super) fn parse_loc_record(bytes: &[u8], offset: u64, len: u64) -> Result<segfmt::Rec> {
    let s = offset as usize;
    let e = s + len as usize;
    if e > bytes.len() {
        return Err(Error::Corrupt("loc oob".into()));
    }
    segfmt::parse_at(&bytes[s..e], 0)?.ok_or_else(|| Error::Corrupt("empty rec".into()))
}

pub(super) fn sorted_unsealed_bytes(sh: &Shard) -> u64 {
    let sealed = sh.sorted_segments.iter().fold(0u64, |bytes, segment| {
        bytes.saturating_add(segment.record_bytes())
    });
    sh.residue
        .live_bytes
        .saturating_add(sh.residue.dead_bytes)
        .saturating_sub(sealed)
}

pub(super) fn sorted_encrypted_tail_pending(directory: &Path) -> Result<bool> {
    let root = directory.join("sorted-tail");
    if !root.exists() {
        return Ok(false);
    }
    let mut generation = 0;
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        if path.extension().and_then(|name| name.to_str()) == Some("seg") {
            if let Some(sequence) = path
                .file_stem()
                .and_then(|name| name.to_str())
                .and_then(|name| name.parse::<u64>().ok())
            {
                generation = generation.max(sequence);
            }
        }
    }
    Ok(!encrypted_files(&root.join(format!("{generation:010}")))?.is_empty())
}

pub(super) fn reset_sorted_encrypted_tail(sh: &mut Shard) {
    sh.frame_cache = HashMap::new();
    sh.frame_cache_order = VecDeque::new();
    sh.frame_locs = HashMap::new();
    sh.frame_loc_path_bytes = 0;
    sh.open_chunk_uuid = None;
    sh.next_frame_counter = 0;
    sh.open_frame_start_csn = None;
    sh.sorted_legacy_encrypted = false;
    sh.sorted_rollback_buffer = false;
    sh.sorted_tail_directory_dirty = false;
}

pub(super) fn remove_retired_encrypted_sources(
    directory: &Path,
    previous_high: u64,
    legacy: bool,
) -> Result<()> {
    if legacy {
        for source in [
            encrypted_chunks_dir_for(directory),
            encrypted_tail_dir_for(directory),
        ] {
            for (_, path) in encrypted_files(&source)? {
                fs::remove_file(path)?;
            }
            if source.exists() {
                sync_dir(&source)?;
            }
        }
    }
    let root = directory.join("sorted-tail");
    if !root.exists() {
        return Ok(());
    }
    // Two fixed directory levels within this group; never walk other groups.
    for entry in fs::read_dir(&root)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir()
            || !entry
                .file_name()
                .to_str()
                .and_then(|name| name.parse::<u64>().ok())
                .is_some_and(|sequence| sequence <= previous_high)
        {
            continue;
        }
        let source = entry.path();
        for (_, path) in encrypted_files(&source)? {
            fs::remove_file(path)?;
        }
        sync_dir(&source)?;
        // Unknown contents do not belong to this cleanup pass.
        if fs::read_dir(&source)?.next().is_none() {
            fs::remove_dir(&source)?;
        }
    }
    sync_dir(&root)
}
