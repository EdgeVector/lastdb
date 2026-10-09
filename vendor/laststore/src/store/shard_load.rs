use super::*;

/// Is this error the process (or the system) being out of file descriptors?
///
/// Matched on the raw errno rather than [`std::io::ErrorKind`], because `EMFILE`
/// and `ENFILE` have no stable `ErrorKind` variant — they arrive as
/// `Uncategorized`, which is both unmatchable on stable and shared with errors
/// that releasing a descriptor would not fix.
pub(super) fn is_descriptor_exhaustion(e: &Error) -> bool {
    let Error::Io(io) = e else {
        return false;
    };
    matches!(
        io.raw_os_error(),
        // EMFILE: this process is at its RLIMIT_NOFILE.
        // ENFILE: the whole system's file table is full.
        Some(n) if n == rustix::io::Errno::MFILE.raw_os_error()
            || n == rustix::io::Errno::NFILE.raw_os_error()
    )
}

/// Sum on-disk file sizes under a group directory. Used to charge an
/// in-flight cold load before the parse allocates decrypt/frame buffers.
///
/// This is a directory listing of one group home, not a collection scan.
pub(super) fn estimate_group_on_disk_bytes(dir: &Path) -> u64 {
    fn walk(path: &Path) -> u64 {
        let Ok(meta) = fs::symlink_metadata(path) else {
            return 0;
        };
        if meta.file_type().is_symlink() {
            return 0;
        }
        if meta.is_file() {
            return meta.len();
        }
        if !meta.is_dir() {
            return 0;
        }
        let Ok(entries) = fs::read_dir(path) else {
            return 0;
        };
        entries
            .filter_map(std::result::Result::ok)
            .map(|entry| walk(&entry.path()))
            .fold(0u64, u64::saturating_add)
    }
    walk(dir)
}

/// Inspect only the newest seal and file sizes of newer append tails. The
/// temporary footer is dropped here, without installing a resident group.
/// Older files can be obsolete crash remnants and need no reads.
pub(super) fn read_plain_sorted_residue(
    dir: &Path,
    shard: u16,
) -> Result<Option<(GroupResidue, u64)>> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().and_then(|extension| extension.to_str()) == Some("seg") {
            if let Some(sequence) = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .and_then(|stem| stem.parse::<u64>().ok())
            {
                paths.push((sequence, path));
            }
        }
    }
    paths.sort_unstable_by_key(|(sequence, _)| *sequence);
    let mut appended_bytes = 0u64;
    for (_, path) in paths.into_iter().rev() {
        if sorted::Segment::recognizes(&path)? {
            let segment = sorted::Segment::open(&path, None)?;
            if segment.header.shard != shard {
                return Err(Error::Corrupt(
                    "sorted residue belongs to another shard".into(),
                ));
            }
            return Ok(Some((
                GroupResidue {
                    live_bytes: segment.header.group_live_bytes,
                    dead_bytes: segment.header.group_dead_bytes,
                },
                appended_bytes,
            )));
        }
        appended_bytes = appended_bytes.saturating_add(fs::metadata(path)?.len());
    }
    Ok(None)
}

pub(super) fn reject_incomplete_sorted_restore(directory: &Path) -> Result<()> {
    // One existence probe suffices, including a crash before the intent or
    // manifest write. Only verified restore publication removes this marker.
    if directory.join(".sorted-restore-pending").exists() {
        return Err(Error::Corrupt(
            "sorted restore is incomplete; its manifest is not published".into(),
        ));
    }
    Ok(())
}

// lint:fn-size-ok verbatim move from store.rs; splitting this function is separate work
pub(super) fn load_shard(
    dir: PathBuf,
    collection: String,
    shard: u16,
    data_key: Option<[u8; 32]>,
    policy: CollectionPolicy,
    sorted_mode: bool,
) -> Result<Shard> {
    let mut sh = Shard {
        dir,
        collection,
        shard,
        data_key,
        policy,
        sorted_mode,
        ..Default::default()
    };
    reject_incomplete_sorted_restore(&sh.dir)?;
    if !sh.dir.exists() {
        return Ok(sh);
    }
    let mut seqs = Vec::new();
    for e in fs::read_dir(&sh.dir)? {
        let p = e?.path();
        if p.extension().and_then(|x| x.to_str()) != Some("seg") {
            continue;
        }
        if let Some(stem) = p.file_stem().and_then(|s| s.to_str()) {
            if let Ok(n) = stem.parse::<u64>() {
                seqs.push(n);
            }
        }
    }
    seqs.sort_unstable();
    sh.plain_high_seq = seqs.last().copied().unwrap_or(0);
    let mut sealed = Vec::new();
    let mut legacy = Vec::new();
    // A complete snapshot is authoritative. Inspect newest files first and
    // stop there; obsolete files may be damaged or only partly unlinked.
    for sequence in seqs.into_iter().rev() {
        let path = sh.dir.join(format!("{sequence:010}.seg"));
        if sorted::Segment::recognizes(&path)? {
            let segment = sorted::Segment::open(&path, sh.data_key.as_ref())?;
            if segment.header.shard != sh.shard {
                return Err(Error::Corrupt(
                    "sorted segment belongs to another shard".into(),
                ));
            }
            let complete = segment.header.complete;
            sealed.push((sequence, segment));
            if complete {
                break;
            }
        } else {
            legacy.push(sequence);
        }
    }
    sealed.reverse();
    legacy.reverse();
    if !sealed.is_empty() {
        if !sealed.first().expect("sealed").1.header.complete {
            return Err(Error::Corrupt(
                "sorted group has no complete snapshot".into(),
            ));
        }
        let newest = sealed.last().expect("sealed").0;
        legacy.retain(|sequence| *sequence > newest);
        sh.sorted_segments = sealed.into_iter().map(|(_, segment)| segment).collect();
        sh.sorted_mode = true;
        let last = &sh.sorted_segments.last().expect("sealed").header;
        sh.max_csn = last.end_csn;
        sh.residue = GroupResidue {
            live_bytes: last.group_live_bytes,
            dead_bytes: last.group_dead_bytes,
        };
        let total = sh
            .sorted_segments
            .iter()
            .try_fold(0u64, |sum, segment| sum.checked_add(segment.record_bytes()))
            .ok_or_else(|| Error::Corrupt("sorted record byte count overflow".into()))?;
        if sh.residue.live_bytes.checked_add(sh.residue.dead_bytes) != Some(total) {
            return Err(Error::Corrupt(
                "sorted residue differs from sealed record bytes".into(),
            ));
        }
    }
    if sh.data_key.is_some() {
        if !sh.sorted_segments.is_empty() {
            if !legacy.is_empty() {
                return Err(Error::Corrupt(
                    "encrypted sorted group has a raw numbered tail".into(),
                ));
            }
            return load_sorted_encrypted_tail(sh);
        }
        let has_sorted_tail = sh.dir.join("sorted-tail").exists();
        let has_legacy = !encrypted_files(&encrypted_chunks_dir(&sh))?.is_empty()
            || !encrypted_files(&encrypted_tail_dir_for(&sh.dir))?.is_empty();
        if has_legacy {
            if has_sorted_tail {
                return Err(Error::Corrupt(
                    "encrypted group mixes legacy and sorted tails without a snapshot".into(),
                ));
            }
            // Legacy replay needs the legacy path and caches. The first sorted
            // append converts it before a new generation-scoped tail can exist.
            sh.sorted_mode = false;
            let mut loaded = load_encrypted_shard(sh)?;
            loaded.sorted_mode = sorted_mode;
            loaded.sorted_legacy_encrypted = sorted_mode;
            return Ok(loaded);
        }
        sh.sorted_mode |= has_sorted_tail;
        return if sh.uses_sorted_index() {
            load_sorted_encrypted_tail(sh)
        } else {
            load_encrypted_shard(sh)
        };
    }
    let seqs = legacy;
    for (i, &seq) in seqs.iter().enumerate() {
        let last = i + 1 == seqs.len();
        let path = sh.dir.join(format!("{seq:010}.seg"));
        let disk_data = fs::read(&path)?;
        let (data, clean_disk_len, chunk_uuid, next_frame_counter) =
            decode_segment_payload(&sh, seq, &disk_data, last)?;
        let mut off = 0usize;
        let mut clean = 0usize;
        while off < data.len() {
            match segfmt::parse_at(&data, off)? {
                None => {
                    if last {
                        break;
                    }
                    return Err(Error::Corrupt(format!("bad sealed seg {seq}")));
                }
                Some(rec) => {
                    let len = rec.raw_len;
                    if rec.is_put {
                        sh.insert_index(
                            rec.id,
                            Loc::Legacy {
                                seg: seq,
                                offset: off as u64,
                                len: len as u64,
                            },
                        )?;
                    } else {
                        sh.remove_index(&rec.id)?;
                        sh.note_delete_marker(len as u64);
                    }
                    off += len;
                    clean = off;
                }
            }
        }
        if last {
            if clean < data.len() || clean_disk_len < disk_data.len() as u64 {
                let f = OpenOptions::new().write(true).open(&path)?;
                let truncate_to = if sh.data_key.is_some() {
                    clean_disk_len
                } else {
                    clean as u64
                };
                f.set_len(truncate_to)?;
                durability::sync_dirty_file(&f)?;
            }
            sh.open_len = clean as u64;
            sh.file_len = clean as u64;
            // A cold load used to materialise the whole open segment — up to
            // `max_segment_bytes` per group, charged to the warm budget the
            // moment the group became resident, for bytes the file has and
            // `read_at` can pread. Start the buffer empty at the end of what
            // is on disk instead; appends extend it from there.
            sh.open_buf = Vec::new();
            sh.open_buf_base = clean as u64;
            sh.open_chunk_uuid = chunk_uuid;
            sh.next_frame_counter = next_frame_counter;
            sh.open_frame_start_csn = None;
        } else {
            sh.seg_bytes.insert(seq, data);
        }
    }
    sh.segments = seqs;
    Ok(sh)
}
