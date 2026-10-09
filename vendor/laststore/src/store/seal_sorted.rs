use super::*;

impl LastStore {
    pub(super) fn read_at_uncached(sh: &Shard, loc: Loc) -> Result<Vec<u8>> {
        let record = match loc {
            Loc::Sorted {
                segment,
                offset,
                body_len,
                ..
            } => {
                let segment = sh.sorted_segments.get(segment).ok_or_else(|| {
                    Error::Corrupt("sorted location names a missing segment".into())
                })?;
                return segment
                    .cursor(sh.data_key.as_ref(), b"")?
                    .read_bytes(offset, body_len);
            }
            Loc::Legacy { seg, offset, len } => {
                if Some(seg) == sh.segments.last().copied() && offset >= sh.open_buf_base {
                    parse_loc_record(&sh.open_buf, offset - sh.open_buf_base, len)?
                } else if let Some(bytes) = sh.seg_bytes.get(&seg) {
                    parse_loc_record(bytes, offset, len)?
                } else if sh.data_key.is_none() {
                    parse_loc_record(&read_segment_range(&sh.dir, seg, offset, len)?, 0, len)?
                } else {
                    return Err(Error::Corrupt(
                        "legacy encrypted segment is unavailable".into(),
                    ));
                }
            }
            Loc::Chunk {
                chunk_uuid,
                frame_idx,
                offset_in_frame,
                len,
            } => {
                let key = (chunk_uuid, frame_idx);
                if Some(chunk_uuid) == sh.open_chunk_uuid && frame_idx == sh.next_frame_counter {
                    parse_loc_record(
                        &sh.open_buf,
                        sh.file_len + offset_in_frame - sh.open_buf_base,
                        len,
                    )?
                } else if let Some(payload) = sh.frame_cache.get(&key) {
                    parse_loc_record(payload, offset_in_frame, len)?
                } else {
                    let location = sh.frame_locs.get(&key).ok_or_else(|| {
                        Error::Corrupt("encrypted tail frame is unavailable".into())
                    })?;
                    let payload = read_encrypted_frame_uncached(sh, key, location)?;
                    parse_loc_record(&payload, offset_in_frame, len)?
                }
            }
        };
        record
            .body
            .ok_or_else(|| Error::Corrupt("selected location contains a delete".into()))
    }

    pub(super) fn read_at(sh: &mut Shard, loc: Loc) -> Result<Vec<u8>> {
        if sh.uses_sorted_index() {
            return Self::read_at_uncached(sh, loc);
        }
        let rec = match loc {
            Loc::Sorted {
                segment,
                offset,
                body_len,
                ..
            } => {
                let segment = sh.sorted_segments.get(segment).ok_or_else(|| {
                    Error::Corrupt("sorted location names a missing segment".into())
                })?;
                return segment
                    .cursor(sh.data_key.as_ref(), b"")?
                    .read_bytes(offset, body_len);
            }
            Loc::Legacy { seg, offset, len } => {
                // The open tail is authoritative from `open_buf_base` up: that
                // range can hold appends no file has yet. Below the base, and
                // behind the open segment entirely, the bytes are already on
                // disk, so a cached copy and the file agree and either will do.
                let from_memory =
                    if Some(seg) == sh.segments.last().copied() && offset >= sh.open_buf_base {
                        Some(parse_loc_record(
                            &sh.open_buf,
                            offset - sh.open_buf_base,
                            len,
                        )?)
                    } else if let Some(bytes) = sh.seg_bytes.get(&seg) {
                        Some(parse_loc_record(bytes, offset, len)?)
                    } else {
                        None
                    };
                match from_memory {
                    Some(rec) => rec,
                    // Not in memory — read just this record's bytes back. Under
                    // plain packaging `Loc::Legacy` offsets are file offsets
                    // (see `decode_segment_payload`, which hands the loader the
                    // disk bytes verbatim), so the range is the record and
                    // nothing else. This is what lets the warm set drop both
                    // `seg_bytes` and the flushed prefix of the open tail
                    // rather than hold a copy of bytes the file already has.
                    None if sh.data_key.is_none() => {
                        let bytes = read_segment_range(&sh.dir, seg, offset, len)?;
                        parse_loc_record(&bytes, 0, len)?
                    }
                    None => return Err(Error::Corrupt(format!("missing seg {seg}"))),
                }
            }
            Loc::Chunk {
                chunk_uuid,
                frame_idx,
                offset_in_frame,
                len,
            } => {
                let key = (chunk_uuid, frame_idx);
                if let Some(frame_payload) = sh.frame_cache.get(&key) {
                    parse_loc_record(frame_payload, offset_in_frame, len)?
                } else if Some(chunk_uuid) == sh.open_chunk_uuid
                    && frame_idx == sh.next_frame_counter
                {
                    let s = sh.file_len + offset_in_frame;
                    parse_loc_record(&sh.open_buf, s, len)?
                } else if let Some(disk_loc) = sh.frame_locs.get(&key).cloned() {
                    let frame_payload = read_encrypted_frame(sh, key, &disk_loc)?;
                    parse_loc_record(&frame_payload, offset_in_frame, len)?
                } else {
                    return Err(Error::Corrupt(format!(
                        "missing frame {frame_idx} in chunk {chunk_uuid}"
                    )));
                }
            }
        };
        rec.body
            .ok_or_else(|| Error::Corrupt(format!("no body {}", rec.id)))
    }

    pub(super) fn seal_sorted_tail(sh: &mut Shard) -> Result<()> {
        if Self::sorted_snapshot_needed(sh) {
            return Self::sync_open(sh);
        }
        if sh.index.is_empty() {
            return Self::sync_open(sh);
        }
        if sh.sorted_segments.is_empty() {
            return Self::compact_sorted_shard(sh);
        }
        if sh.data_key.is_none() {
            Self::sync_open(sh)?;
        }
        let previous_high = sh.plain_high_seq;
        let legacy_encrypted = sh.sorted_legacy_encrypted;
        let sequence = sh
            .plain_high_seq
            .checked_add(1)
            .ok_or_else(|| Error::Corrupt("segment sequence exhausted".into()))?;
        let tail_bytes = sh
            .index
            .iter()
            .try_fold(0u64, |sum, (key, location)| {
                sum.checked_add(
                    location.map_or(3 + key.len() as u64, |location| location.record_len()),
                )
            })
            .ok_or_else(|| Error::Corrupt("tail byte count overflow".into()))?;
        let total = sh
            .sorted_segments
            .iter()
            .try_fold(tail_bytes, |sum, segment| {
                sum.checked_add(segment.record_bytes())
            })
            .ok_or_else(|| Error::Corrupt("sealed byte count overflow".into()))?;
        let dead_bytes = total
            .checked_sub(sh.residue.live_bytes)
            .ok_or_else(|| Error::Corrupt("live residue exceeds seal output".into()))?;
        let header = sorted::Header {
            chunk_uuid: Uuid::new_v4(),
            shard: sh.shard,
            end_csn: sh.max_csn,
            encrypted: sh.data_key.is_some(),
            complete: false,
            group_live_bytes: sh.residue.live_bytes,
            group_dead_bytes: dead_bytes,
        };
        let path = sh.dir.join(format!("{sequence:010}.seg"));
        let segment = sorted::Segment::write_from(
            &path,
            header,
            sh.data_key.as_ref(),
            &mut SortedRewrite::new(sh, true)?,
        )?;
        let old = std::mem::take(&mut sh.segments);
        sh.open_file = None;
        sh.clear_index();
        sh.residue = GroupResidue {
            live_bytes: header.group_live_bytes,
            dead_bytes: header.group_dead_bytes,
        };
        sh.sorted_segments.push(segment);
        sh.plain_high_seq = sequence;
        sh.open_buf = Vec::new();
        sh.open_buf_base = 0;
        sh.open_len = 0;
        sh.file_len = 0;
        sh.values = HashMap::new();
        sh.seg_bytes = HashMap::new();
        sh.sidecar_stamps = None;
        sh.dirty_ops = 0;
        sh.dirty_bytes = 0;
        if sh.data_key.is_some() {
            reset_sorted_encrypted_tail(sh);
            remove_retired_encrypted_sources(&sh.dir, previous_high, legacy_encrypted)?;
        }
        for sequence in old {
            sorted_file::remove(&sh.dir.join(format!("{sequence:010}.seg")))?;
        }
        sync_dir(&sh.dir)?;
        if sh.sorted_segments.len() > 4 {
            Self::compact_sorted_shard(sh)?;
        }
        Ok(())
    }

    pub(super) fn compact_sorted_shard(sh: &mut Shard) -> Result<()> {
        if Self::sorted_snapshot_needed(sh) {
            return Self::sync_open(sh);
        }
        if sh.data_key.is_none() {
            Self::sync_open(sh)?;
        }
        Self::rewrite_sorted_snapshot(sh)
    }

    pub(super) fn sorted_snapshot_needed(sh: &Shard) -> bool {
        sh.uses_sorted_index()
            && (sh.dirty_ops != 0 || sh.dirty_bytes != 0)
            && (sh.sorted_rollback_buffer
                || sh.sorted_segments.len() > 4
                || sh.open_len > sh.max_sorted_tail_bytes.max(64 * 1024))
    }

    pub(super) fn rewrite_sorted_snapshot(sh: &mut Shard) -> Result<()> {
        fs::create_dir_all(&sh.dir)?;
        let legacy_encrypted = sh.sorted_legacy_encrypted;
        let previous_high = sh
            .plain_high_seq
            .max(sh.segments.last().copied().unwrap_or(0));
        let sequence = previous_high
            .checked_add(1)
            .ok_or_else(|| Error::Corrupt("segment sequence exhausted".into()))?;
        let path = sh.dir.join(format!("{sequence:010}.seg"));
        let header = sorted::Header {
            chunk_uuid: Uuid::new_v4(),
            shard: sh.shard,
            end_csn: sh.max_csn,
            encrypted: sh.data_key.is_some(),
            complete: true,
            group_live_bytes: sh.residue.live_bytes,
            group_dead_bytes: 0,
        };
        let segment = sorted::Segment::write_from(
            &path,
            header,
            sh.data_key.as_ref(),
            &mut SortedRewrite::new(sh, false)?,
        )?;
        // The complete snapshot is durable before old files can disappear.
        // Its marker lets recovery ignore old deletes/puts after a crash at
        // any point in the unlink pass, including an empty complete snapshot.
        sh.open_file = None;
        sh.clear_index();
        sh.residue.live_bytes = segment.record_bytes();
        sh.sorted_segments = vec![segment];
        sh.sorted_mode = true;
        sh.plain_high_seq = sequence;
        sh.segments.clear();
        sh.open_buf = Vec::new();
        sh.open_buf_base = 0;
        sh.open_len = 0;
        sh.file_len = 0;
        sh.values.clear();
        sh.seg_bytes.clear();
        sh.sidecar_stamps = None;
        sh.dirty_ops = 0;
        sh.dirty_bytes = 0;
        if sh.data_key.is_some() {
            reset_sorted_encrypted_tail(sh);
            remove_retired_encrypted_sources(&sh.dir, previous_high, legacy_encrypted)?;
        }
        for entry in fs::read_dir(&sh.dir)? {
            let old = entry?.path();
            if old.extension().and_then(|extension| extension.to_str()) == Some("seg")
                && old
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .and_then(|stem| stem.parse::<u64>().ok())
                    .is_some_and(|old| old <= previous_high)
            {
                sorted_file::remove(&old)?;
            }
        }
        match fs::remove_file(keysidecar::sidecar_path(&sh.dir)) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        sorted_file::cleanup_orphans(&sh.dir)?;
        sync_dir(&sh.dir)?;
        Ok(())
    }

    pub(super) fn compact_shard(sh: &mut Shard) -> Result<()> {
        if sh.uses_sorted_index() {
            return Self::compact_sorted_shard(sh);
        }
        if sh.data_key.is_some() {
            return compact_encrypted_shard(sh);
        }
        group_compact::compact_plain_shard(sh)
    }
}
