use super::*;

pub(super) fn encrypted_tail_dir(sh: &Shard) -> PathBuf {
    if sh.uses_sorted_index() && !sh.sorted_legacy_encrypted {
        sh.dir
            .join("sorted-tail")
            .join(format!("{:010}", sh.plain_high_seq))
    } else {
        encrypted_tail_dir_for(&sh.dir)
    }
}

pub(super) fn encrypted_chunks_dir(sh: &Shard) -> PathBuf {
    encrypted_chunks_dir_for(&sh.dir)
}

pub(super) fn encrypted_tail_dir_for(shard_dir: &Path) -> PathBuf {
    shard_dir.join("tail")
}

pub(super) fn encrypted_chunks_dir_for(shard_dir: &Path) -> PathBuf {
    shard_dir.join("chunks")
}

pub(super) fn encrypted_quarantine_dir(sh: &Shard) -> PathBuf {
    sh.dir.join("quarantine")
}

pub(super) fn encrypted_tail_path(sh: &Shard, chunk_uuid: Uuid) -> PathBuf {
    encrypted_tail_dir(sh).join(format!("{chunk_uuid}.seg"))
}

pub(super) fn encrypted_chunk_path(sh: &Shard, chunk_uuid: Uuid) -> PathBuf {
    encrypted_chunks_dir(sh).join(format!("{chunk_uuid}.seg"))
}

pub(super) fn quarantine_encrypted_chunk(
    sh: &Shard,
    chunk_uuid: Uuid,
    path: &Path,
) -> Result<PathBuf> {
    fs::create_dir_all(encrypted_quarantine_dir(sh))?;
    let dst = encrypted_quarantine_dir(sh).join(format!("{chunk_uuid}.seg"));
    if dst.exists() {
        let _ = fs::remove_file(&dst);
    }
    fs::rename(path, &dst)?;
    sync_dir(&encrypted_quarantine_dir(sh))?;
    sync_dir(&encrypted_chunks_dir(sh))?;
    Ok(dst)
}

pub(super) type LocatedChunkFile = (String, u16, Option<u32>, PathBuf);

pub(super) fn find_chunk_file(
    root: &Path,
    chunk_uuid: Uuid,
    role_dir: &str,
) -> Result<Option<LocatedChunkFile>> {
    let data_dir = root.join("data");
    if !data_dir.exists() {
        return Ok(None);
    }
    for collection_entry in fs::read_dir(data_dir)? {
        let collection_entry = collection_entry?;
        if !collection_entry.file_type()?.is_dir() {
            continue;
        }
        let collection = collection_entry.file_name().to_string_lossy().into_owned();
        for shard_entry in fs::read_dir(collection_entry.path())? {
            let shard_entry = shard_entry?;
            if !shard_entry.file_type()?.is_dir() {
                continue;
            }
            let shard_name = shard_entry.file_name().to_string_lossy().into_owned();
            let Ok(shard) = u16::from_str_radix(&shard_name, 16) else {
                continue;
            };
            let path = shard_entry
                .path()
                .join(role_dir)
                .join(format!("{chunk_uuid}.seg"));
            if path.exists() {
                return Ok(Some((collection, shard, None, path)));
            }

            let groups_dir = shard_entry.path().join("g");
            if !groups_dir.exists() {
                continue;
            }
            for group_entry in fs::read_dir(groups_dir)? {
                let group_entry = group_entry?;
                if !group_entry.file_type()?.is_dir() {
                    continue;
                }
                let group_name = group_entry.file_name().to_string_lossy().into_owned();
                let Ok(group) = u32::from_str_radix(&group_name, 16) else {
                    continue;
                };
                let path = group_entry
                    .path()
                    .join(role_dir)
                    .join(format!("{chunk_uuid}.seg"));
                if path.exists() {
                    return Ok(Some((collection, shard, Some(group), path)));
                }
            }
        }
    }
    Ok(None)
}

pub(super) fn shard_group_from_dir(sh: &Shard) -> Option<u32> {
    let group_name = sh.dir.file_name()?.to_string_lossy();
    if sh
        .dir
        .parent()
        .and_then(|p| p.file_name())
        .and_then(|s| s.to_str())
        != Some("g")
    {
        return None;
    }
    u32::from_str_radix(&group_name, 16).ok()
}

pub(super) fn encrypted_files(dir: &Path) -> Result<Vec<(Uuid, PathBuf)>> {
    let mut out = Vec::new();
    if !dir.exists() {
        return Ok(out);
    }
    for e in fs::read_dir(dir)? {
        let path = e?.path();
        if path.extension().and_then(|x| x.to_str()) != Some("seg") {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        let Ok(uuid) = Uuid::parse_str(stem) else {
            continue;
        };
        out.push((uuid, path));
    }
    Ok(out)
}

pub(super) fn plain_segment_log_uuid(
    collection: &str,
    shard: u16,
    group: Option<u32>,
    seq: u64,
) -> Uuid {
    // Stable per (collection, shard, group, seq) so re-uploads hit the same
    // content key.
    //
    // `group` is part of the identity, not decoration: in hash-group layout every
    // one of the 1024 groups owns its own `{seq:010}.seg`, so a derivation over
    // (collection, shard, seq) alone collapses all of them onto ONE uuid. That
    // made a chunk ref ambiguous — it could resolve to any group's bytes — and
    // silently deduped distinct segments in the backup manifest. The domain
    // string is `v2` because including `group` changes every derived uuid; `v1`
    // ids only ever existed on homes whose backup could not complete (the
    // verify side could not resolve them at all), so nothing durable is keyed
    // by them.
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(b"laststore-plain-seg-v2\0");
    h.update(collection.as_bytes());
    h.update(b"\0");
    h.update(shard.to_le_bytes());
    match group {
        Some(group) => {
            h.update([1u8]);
            h.update(group.to_le_bytes());
        }
        None => h.update([0u8]),
    }
    h.update(seq.to_le_bytes());
    let dig = h.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&dig[..16]);
    // Set RFC 4122 variant/version bits so parsers accept it as a UUID.
    bytes[6] = (bytes[6] & 0x0f) | 0x50; // version 5-ish
    bytes[8] = (bytes[8] & 0x3f) | 0x80; // RFC 4122 variant
    Uuid::from_bytes(bytes)
}

/// Identity of one plain SegmentLog handle: (collection, shard, hash group).
pub(super) type PlainSegHandleKey = (String, u16, Option<u32>);

/// Reverse map from derived chunk uuid to seq, for one handle.
pub(super) type PlainSegSeqByUuid = HashMap<Uuid, u64>;

thread_local! {
    pub(super) static PLAIN_SEG_SEQ_CACHE: std::cell::RefCell<HashMap<PlainSegHandleKey, PlainSegSeqByUuid>> =
        std::cell::RefCell::new(HashMap::new());
}

/// Invert [`plain_segment_log_uuid`] for restore install.
///
/// Cache only the current handle on each worker. Restore visits each group's
/// plain chunks together; retaining every previous group's 50,001-entry map
/// made process memory grow with the number of restored groups.
pub(super) fn plain_segment_log_seq_for_uuid(
    collection: &str,
    shard: u16,
    group: Option<u32>,
    chunk_uuid: Uuid,
) -> Option<u64> {
    const MAX_SEQ: u64 = 50_000;
    PLAIN_SEG_SEQ_CACHE.with(|cell| {
        let mut cache = cell.borrow_mut();
        let key = (collection.to_string(), shard, group);
        if !cache.contains_key(&key) {
            cache.clear();
        }
        let map = cache
            .entry(key)
            .or_insert_with(|| HashMap::with_capacity(8));
        if let Some(seq) = map.get(&chunk_uuid) {
            return Some(*seq);
        }
        // Grow the inverse map only through the requested sequence. Most
        // restored handles start at sequence zero; eagerly hashing every
        // possible sequence made that first installation need 50,001 hashes.
        for seq in map.len() as u64..=MAX_SEQ {
            let uuid = plain_segment_log_uuid(collection, shard, group, seq);
            map.insert(uuid, seq);
            if uuid == chunk_uuid {
                return Some(seq);
            }
        }
        None
    })
}

/// Numbered sealed segs directly inside one handle dir (`{seq:010}.seg`).
///
/// `handle_dir` is the directory that actually holds the segs: the shard dir in
/// flat layout, or `<shard>/g/<group>` in hash-group layout. `group` must match
/// that directory — it is part of the derived uuid.
pub(super) fn plain_segment_log_files(
    handle_dir: &Path,
    collection: &str,
    shard: u16,
    group: Option<u32>,
) -> Result<Vec<(Uuid, PathBuf)>> {
    let mut out = Vec::new();
    if !handle_dir.exists() {
        return Ok(out);
    }
    reject_incomplete_sorted_restore(handle_dir)?;
    let mut seqs = Vec::new();
    for e in fs::read_dir(handle_dir)? {
        let path = e?.path();
        if path.extension().and_then(|x| x.to_str()) != Some("seg") {
            continue;
        }
        // Skip uuid-named segs (those belong under chunks/ if frame-AEAD).
        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        if Uuid::parse_str(stem).is_ok() {
            continue;
        }
        if let Ok(seq) = stem.parse::<u64>() {
            seqs.push((seq, path));
        }
    }
    seqs.sort_by_key(|(seq, _)| *seq);
    for (seq, path) in seqs {
        let chunk_uuid = plain_segment_log_uuid(collection, shard, group, seq);
        out.extend(sorted_file::physical_parts(&path)?);
        out.push((chunk_uuid, path));
    }
    Ok(out)
}

pub(super) fn find_plain_segment_log_chunk(
    root: &Path,
    chunk_uuid: Uuid,
) -> Result<Option<SealedChunkMeta>> {
    let data_dir = root.join("data");
    if !data_dir.exists() {
        return Ok(None);
    }
    for collection_entry in fs::read_dir(data_dir)? {
        let collection_entry = collection_entry?;
        if !collection_entry.file_type()?.is_dir() {
            continue;
        }
        let collection = collection_entry.file_name().to_string_lossy().into_owned();
        for shard_entry in fs::read_dir(collection_entry.path())? {
            let shard_entry = shard_entry?;
            if !shard_entry.file_type()?.is_dir() {
                continue;
            }
            let shard_name = shard_entry.file_name().to_string_lossy().into_owned();
            let Ok(shard) = u16::from_str_radix(&shard_name, 16) else {
                continue;
            };
            // Flat layout: segs sit directly in the shard dir.
            for (uuid, path) in
                plain_segment_log_files(&shard_entry.path(), &collection, shard, None)?
            {
                if uuid == chunk_uuid {
                    return Ok(Some(SealedChunkMeta {
                        collection,
                        shard,
                        group_id: None,
                        chunk_uuid,
                        path,
                        end_csn: 0,
                    }));
                }
            }

            // Hash-group layout: segs sit in `<shard>/g/<group>/`. `enumerate_chunks`
            // walks exactly these directories, so verify has to as well — scanning
            // only the shard dir resolved nothing on a hash-group home and turned
            // every enumerated chunk into `missing chunk`, which aborted the whole
            // backup walk and stopped all cloud backup.
            let groups_dir = shard_entry.path().join("g");
            if !groups_dir.exists() {
                continue;
            }
            for group_entry in fs::read_dir(groups_dir)? {
                let group_entry = group_entry?;
                if !group_entry.file_type()?.is_dir() {
                    continue;
                }
                let group_name = group_entry.file_name().to_string_lossy().into_owned();
                let Ok(group) = u32::from_str_radix(&group_name, 16) else {
                    continue;
                };
                for (uuid, path) in
                    plain_segment_log_files(&group_entry.path(), &collection, shard, Some(group))?
                {
                    if uuid == chunk_uuid {
                        return Ok(Some(SealedChunkMeta {
                            collection,
                            shard,
                            group_id: Some(group),
                            chunk_uuid,
                            path,
                            end_csn: 0,
                        }));
                    }
                }
            }
        }
    }
    Ok(None)
}

// `sort_encrypted_files_by_mtime` was deleted here on purpose. Replay order is
// CSN, not wall-clock: sorting by `(mtime, path)` tied whenever two chunks were
// sealed inside one filesystem timestamp tick and then fell back to random uuid
// order, which could invert a put and a later delete of the same id and
// resurrect a deleted key. Do not reintroduce a timestamp-ordered replay.

pub(super) fn encrypted_shard_paths(sh: &Shard) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for (_, path) in encrypted_files(&encrypted_chunks_dir(sh))? {
        out.push(path);
    }
    for (_, path) in encrypted_files(&encrypted_tail_dir(sh))? {
        out.push(path);
    }
    Ok(out)
}
