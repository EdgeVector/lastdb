// lint:file-size-ok verbatim move from store.rs; splitting this file further is separate work
use super::*;

pub(super) fn load_sorted_encrypted_tail(mut sh: Shard) -> Result<Shard> {
    // Only the current generation's bounded append tail remains relevant.
    // A complete snapshot makes older encrypted bodies unnecessary to read.
    let mut tails = Vec::new();
    for (uuid, path) in encrypted_files(&encrypted_tail_dir(&sh))? {
        if fs::metadata(&path)?.len() > sorted_file::PART_BYTES {
            return Err(Error::Corrupt(
                "sorted encrypted tail exceeds its physical file cap".into(),
            ));
        }
        let bytes = fs::read(&path)?;
        let decoded = decode_encrypted_file(&sh, uuid, &path, &bytes, true)?;
        if decoded.sealed {
            return Err(Error::Corrupt("sorted append tail contains a seal".into()));
        }
        if decoded.clean_disk_len < bytes.len() as u64 {
            let file = OpenOptions::new().write(true).open(&path)?;
            file.set_len(decoded.clean_disk_len)?;
            durability::sync_dirty_file(&file)?;
        }
        tails.push((decoded.max_csn, uuid, decoded));
    }
    tails.sort_by_key(|(csn, uuid, _)| (*csn, *uuid));
    for (csn, uuid, decoded) in tails {
        apply_encrypted_frames(&mut sh, uuid, &decoded.frames, false)?;
        for (counter, location) in decoded.frame_locs {
            sh.insert_frame_loc((uuid, counter), location);
        }
        sh.max_csn = sh.max_csn.max(csn);
    }
    // Never reuse a recovered AEAD nonce domain, including a truncated tail.
    LastStore::open_fresh_encrypted_tail(&mut sh);
    Ok(sh)
}

// lint:fn-size-ok verbatim move from store.rs; splitting this function is separate work
pub(super) fn load_encrypted_shard(mut sh: Shard) -> Result<Shard> {
    if !sh.dir.exists() {
        return Ok(sh);
    }

    let chunks = encrypted_files(&encrypted_chunks_dir(&sh))?;
    let tails = encrypted_files(&encrypted_tail_dir(&sh))?;

    // REPLAY ORDER IS CSN, NOT MTIME.
    //
    // These were sorted by `(mtime, path)`, and both halves of that key are
    // wrong. `mtime` is a filesystem timestamp whose granularity is coarse
    // enough that chunks sealed in quick succession TIE, and the tie-break —
    // `path` — is `<random-uuid>.seg`. So two chunks sealed inside one tick
    // replayed in effectively RANDOM order.
    //
    // Replay is not order-insensitive. `apply_encrypted_frames` inserts on a
    // put record and removes on a delete record, so if a `put id` and a later
    // `delete id` land in different chunks and those chunks invert, the put is
    // applied last and wins: A DELETED KEY COMES BACK, in the group's live
    // authoritative index, on every subsequent load.
    //
    // The failure is one-directional by construction — inverting can only
    // resurrect, never lose — which is exactly how it presented in CI:
    // `RESURRECTED=[...] LOST=[]` on every single hit.
    //
    // It needed an mtime tie, so it was ~invisible on APFS (nanosecond
    // timestamps) and reproduced only inside the Linux CI container: ~644
    // macOS reps found nothing, 2,800 Linux reps hit it at ~1.4%.
    //
    // `max_csn` is the real causal key — `allocate_csn` hands out a monotonic
    // CSN for every put AND delete, so ordering by it reconstructs the true
    // write order regardless of what the filesystem thinks the clock was. The
    // uuid stays only as a stable tie-break, to keep the order total.
    //
    // Decoding is split from applying so the sort can happen in between;
    // `decode_encrypted_file` takes `&Shard`, so this costs no extra reads.
    let mut sealed = Vec::with_capacity(chunks.len());
    for (chunk_uuid, path) in chunks {
        match decode_verified_sealed_chunk(&sh, chunk_uuid, &path) {
            Ok(decoded) => sealed.push((decoded.max_csn, chunk_uuid, decoded)),
            Err(err) => {
                if matches!(err, Error::Io(_)) {
                    return Err(err);
                }
                quarantine_encrypted_chunk(&sh, chunk_uuid, &path)?;
            }
        }
    }
    sealed.sort_by_key(|(csn, uuid, _)| (*csn, *uuid));
    for (_, chunk_uuid, decoded) in sealed {
        apply_verified_sealed_chunk(&mut sh, chunk_uuid, decoded)?;
    }

    // Same ordering rule for unsealed tails, and for the same reason. Ordering
    // also decides which tail is adopted as THE open tail below (the loop's
    // last iteration wins), so by-CSN makes that the genuinely newest one
    // rather than whichever the filesystem happened to stamp last.
    let mut open_tails = Vec::with_capacity(tails.len());
    for (chunk_uuid, path) in tails {
        let disk_data = fs::read(&path)?;
        let decoded = decode_encrypted_file(&sh, chunk_uuid, &path, &disk_data, true)?;
        sh.max_csn = sh.max_csn.max(decoded.max_csn);
        if decoded.clean_disk_len < disk_data.len() as u64 {
            let f = OpenOptions::new().write(true).open(&path)?;
            f.set_len(decoded.clean_disk_len)?;
            durability::sync_dirty_file(&f)?;
        }
        if decoded.frames.is_empty() {
            let _ = fs::remove_file(path);
            continue;
        }
        open_tails.push((decoded.max_csn, chunk_uuid, decoded));
    }
    open_tails.sort_by_key(|(csn, uuid, _)| (*csn, *uuid));

    for (_, chunk_uuid, decoded) in open_tails {
        apply_encrypted_frames(&mut sh, chunk_uuid, &decoded.frames, false)?;
        let plaintext: Vec<u8> = decoded
            .frames
            .iter()
            .flat_map(|(_, payload)| payload.iter().copied())
            .collect();
        for (frame_idx, payload) in decoded.frames {
            insert_frame_cache(&mut sh, (chunk_uuid, frame_idx), payload);
        }
        for (frame_idx, loc) in decoded.frame_locs {
            sh.insert_frame_loc((chunk_uuid, frame_idx), loc);
        }
        sh.open_chunk_uuid = Some(chunk_uuid);
        sh.open_len = plaintext.len() as u64;
        sh.file_len = plaintext.len() as u64;
        sh.open_buf = plaintext;
        sh.next_frame_counter = decoded.next_frame_counter;
        sh.open_frame_start_csn = None;
        seal_recovered_tail(&mut sh, chunk_uuid)?;
    }
    if sh.data_key.is_some() && sh.open_chunk_uuid.is_none() {
        LastStore::open_fresh_encrypted_tail(&mut sh);
    }
    Ok(sh)
}

pub(super) fn apply_encrypted_frames(
    sh: &mut Shard,
    chunk_uuid: Uuid,
    frames: &[(u64, Vec<u8>)],
    allow_partial_last_frame: bool,
) -> Result<()> {
    for (frame_idx, payload) in frames {
        let mut off = 0usize;
        while off < payload.len() {
            match segfmt::parse_at(payload, off)? {
                None => {
                    if allow_partial_last_frame {
                        break;
                    }
                    return Err(Error::Corrupt(format!("bad encrypted chunk {chunk_uuid}")));
                }
                Some(rec) => {
                    let len = rec.raw_len;
                    if rec.is_put {
                        sh.insert_index(
                            rec.id,
                            Loc::Chunk {
                                chunk_uuid,
                                frame_idx: *frame_idx,
                                offset_in_frame: off as u64,
                                len: len as u64,
                            },
                        )?;
                    } else {
                        sh.remove_index(&rec.id)?;
                        sh.note_delete_marker(len as u64);
                    }
                    off += len;
                }
            }
        }
    }
    Ok(())
}

pub(super) struct DecodedEncryptedFile {
    pub(super) frames: Vec<(u64, Vec<u8>)>,
    pub(super) frame_locs: Vec<(u64, FrameDiskLoc)>,
    pub(super) clean_disk_len: u64,
    pub(super) next_frame_counter: u64,
    pub(super) sealed: bool,
    pub(super) max_csn: u64,
}

// lint:fn-size-ok verbatim move from store.rs; splitting this function is separate work
pub(super) fn decode_encrypted_file(
    sh: &Shard,
    expected_uuid: Uuid,
    path: &Path,
    disk_data: &[u8],
    allow_truncated_tail: bool,
) -> Result<DecodedEncryptedFile> {
    let data_key = sh
        .data_key
        .as_ref()
        .ok_or_else(|| Error::Config("encrypted decode without data key".into()))?;
    let mut frames = Vec::new();
    let mut frame_locs = Vec::new();
    let mut off = 0usize;
    let mut plaintext_len = 0u64;
    let mut next_frame_counter = 0u64;
    let mut max_csn = 0u64;
    let mut sealed = false;
    while off < disk_data.len() {
        if is_footer_trailer_at(disk_data, off) {
            break;
        }
        if disk_data.len() - off < frame::min_encoded_len() {
            if allow_truncated_tail {
                break;
            }
            return Err(Error::Corrupt(format!(
                "bad sealed encrypted chunk {expected_uuid}"
            )));
        }
        let frame_len = match frame::encoded_len(&disk_data[off..off + frame::header_size()]) {
            Ok(len) if off + len <= disk_data.len() => len,
            Ok(_) if allow_truncated_tail => break,
            Ok(_) => {
                return Err(Error::Corrupt(format!(
                    "bad sealed encrypted chunk {expected_uuid}"
                )));
            }
            Err(e) => return Err(e),
        };
        let decoded = if sh.uses_sorted_index() && !sh.sorted_legacy_encrypted {
            frame::decode_frame_bounded(
                data_key,
                &disk_data[off..off + frame_len],
                SORTED_ENCRYPTED_TAIL_BYTES as usize,
            )?
        } else {
            frame::decode_frame(data_key, &disk_data[off..off + frame_len])?
        };
        if decoded.header.shard != sh.shard || decoded.header.chunk_uuid != expected_uuid {
            return Err(Error::AeadAuthFail);
        }
        if decoded.header.counter != next_frame_counter {
            return Err(Error::AeadAuthFail);
        }
        if is_seal_record(&decoded.payload) {
            let end_csn = decode_seal_record(&decoded.payload)?;
            if decoded.header.start_csn != end_csn.saturating_add(1) {
                return Err(Error::AeadAuthFail);
            }
            max_csn = max_csn.max(end_csn);
            sealed = true;
            off += frame_len;
            next_frame_counter = next_frame_counter.saturating_add(1);
            if off == disk_data.len() || is_footer_trailer_at(disk_data, off) {
                break;
            }
            continue;
        }
        if is_footer_record(&decoded.payload) {
            if !sealed {
                return Err(Error::Corrupt(format!(
                    "chunk {expected_uuid} has footer before seal"
                )));
            }
            if decoded.header.start_csn != max_csn.saturating_add(1) {
                return Err(Error::AeadAuthFail);
            }
            off += frame_len;
            next_frame_counter = next_frame_counter.saturating_add(1);
            if off == disk_data.len() || is_footer_trailer_at(disk_data, off) {
                break;
            }
            continue;
        }
        if sealed {
            return Err(Error::Corrupt(format!(
                "sealed chunk {expected_uuid} has data after seal"
            )));
        }
        let records = count_complete_records(&decoded.payload, false)?;
        if records > 0 {
            let frame_end = decoded.header.start_csn.saturating_add(records - 1);
            max_csn = max_csn.max(frame_end);
        }
        plaintext_len = plaintext_len.saturating_add(decoded.payload.len() as u64);
        frame_locs.push((
            decoded.header.counter,
            FrameDiskLoc {
                path: path.to_path_buf(),
                disk_offset: off as u64,
                disk_len: frame_len as u64,
            },
        ));
        frames.push((decoded.header.counter, decoded.payload));
        off += frame_len;
        next_frame_counter = next_frame_counter.saturating_add(1);
    }
    Ok(DecodedEncryptedFile {
        frames,
        frame_locs,
        clean_disk_len: off as u64,
        next_frame_counter,
        sealed,
        max_csn,
    })
}

/// Read and authenticate a sealed chunk WITHOUT applying it.
///
/// Split out from [`load_verified_sealed_chunk`] so `load_shard` can decode
/// every chunk, order them by CSN, and only then apply — see the ordering
/// comment there. Takes `&Shard` precisely so a whole group's chunks can be
/// decoded before any of them mutates it.
pub(super) fn decode_verified_sealed_chunk(
    sh: &Shard,
    chunk_uuid: Uuid,
    path: &Path,
) -> Result<DecodedEncryptedFile> {
    let disk_data = fs::read(path)?;
    let decoded = decode_encrypted_file(sh, chunk_uuid, path, &disk_data, false)?;
    if !decoded.sealed {
        return Err(Error::Corrupt(format!(
            "sealed chunk {chunk_uuid} has no seal record"
        )));
    }
    Ok(decoded)
}

/// Apply an already-decoded sealed chunk. Callers are responsible for applying
/// chunks in CSN order; replay is order-dependent.
pub(super) fn apply_verified_sealed_chunk(
    sh: &mut Shard,
    chunk_uuid: Uuid,
    decoded: DecodedEncryptedFile,
) -> Result<()> {
    sh.max_csn = sh.max_csn.max(decoded.max_csn);
    apply_encrypted_frames(sh, chunk_uuid, &decoded.frames, false)?;
    for (frame_idx, payload) in decoded.frames {
        insert_frame_cache(sh, (chunk_uuid, frame_idx), payload);
    }
    for (frame_idx, loc) in decoded.frame_locs {
        sh.insert_frame_loc((chunk_uuid, frame_idx), loc);
    }
    Ok(())
}

pub(super) fn load_verified_sealed_chunk(
    sh: &mut Shard,
    chunk_uuid: Uuid,
    path: &Path,
) -> Result<()> {
    let decoded = decode_verified_sealed_chunk(sh, chunk_uuid, path)?;
    apply_verified_sealed_chunk(sh, chunk_uuid, decoded)
}

pub(super) fn seal_recovered_tail(sh: &mut Shard, chunk_uuid: Uuid) -> Result<()> {
    if sh.open_len == 0 {
        sh.open_chunk_uuid = None;
        sh.next_frame_counter = 0;
        return Ok(());
    }
    fs::create_dir_all(encrypted_tail_dir(sh))?;
    fs::create_dir_all(encrypted_chunks_dir(sh))?;
    let path = encrypted_tail_path(sh, chunk_uuid);
    let mut f = OpenOptions::new().append(true).open(&path)?;
    let data_key = *sh
        .data_key
        .as_ref()
        .ok_or_else(|| Error::Config("encrypted recovered seal without data key".into()))?;
    let end_csn = sh.max_csn;
    let seal_payload = encode_seal_record(end_csn);
    let seal_header = FrameHeader {
        chunk_uuid,
        shard: sh.shard,
        start_csn: end_csn.saturating_add(1),
        counter: sh.next_frame_counter,
    };
    let seal_encoded = encode_frame_for_shard(sh, &data_key, seal_header, &seal_payload)?;
    f.write_all(&seal_encoded)?;
    sh.next_frame_counter = sh.next_frame_counter.saturating_add(1);

    let footer_payload = encode_footer_record(sh, chunk_uuid)?;
    let footer_header = FrameHeader {
        chunk_uuid,
        shard: sh.shard,
        start_csn: end_csn.saturating_add(1),
        counter: sh.next_frame_counter,
    };
    let footer_encoded = encode_frame_for_shard(sh, &data_key, footer_header, &footer_payload)?;
    let footer_offset = f.metadata()?.len();
    f.write_all(&footer_encoded)?;
    write_footer_trailer(&mut f, footer_offset, footer_encoded.len() as u64)?;
    durability::sync_dirty_file(&f)?;
    drop(f);

    let dst = encrypted_chunk_path(sh, chunk_uuid);
    fs::rename(&path, &dst)?;
    sh.rename_frame_loc_paths(&path, &dst);
    sync_dir(&encrypted_chunks_dir(sh))?;
    sync_dir(&encrypted_tail_dir(sh))?;

    sh.open_buf.clear();
    sh.open_len = 0;
    sh.file_len = 0;
    sh.open_file = None;
    sh.open_chunk_uuid = None;
    sh.next_frame_counter = 0;
    sh.open_frame_start_csn = None;
    sh.dirty_ops = 0;
    sh.dirty_bytes = 0;
    Ok(())
}
