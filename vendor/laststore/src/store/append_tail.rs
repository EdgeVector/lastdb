use super::*;

impl LastStore {
    pub(super) fn append_large_sorted(
        sh: &mut Shard,
        line: &[u8],
        id: &str,
        op: CaptureOp,
        previous_len: Option<u64>,
    ) -> Result<Loc> {
        if !sh.index.is_empty() {
            Self::seal_sorted_tail(sh)?;
        }
        Self::sync_open(sh)?;
        fs::create_dir_all(&sh.dir)?;
        tag_backup_excluded_collection(sh)?;
        let sequence = sh
            .plain_high_seq
            .checked_add(1)
            .ok_or_else(|| Error::Corrupt("segment sequence exhausted".into()))?;
        let is_put = op == CaptureOp::Put;
        let body = if is_put { &line[7 + id.len()..] } else { &[] };
        let mut residue = sh.residue;
        if let Some(previous) = previous_len {
            residue.live_bytes = residue.live_bytes.saturating_sub(previous);
            residue.dead_bytes = residue.dead_bytes.saturating_add(previous);
        }
        if is_put {
            residue.live_bytes = residue.live_bytes.saturating_add(line.len() as u64);
        } else {
            residue.dead_bytes = residue.dead_bytes.saturating_add(line.len() as u64);
        }
        let header = sorted::Header {
            chunk_uuid: Uuid::new_v4(),
            shard: sh.shard,
            end_csn: sh.max_csn,
            encrypted: sh.data_key.is_some(),
            complete: sh.sorted_segments.is_empty(),
            group_live_bytes: residue.live_bytes,
            group_dead_bytes: residue.dead_bytes,
        };
        let staged = sh
            .dir
            .join(format!("{sequence:010}.{}.sorted-stage", header.chunk_uuid));
        let mut source = sorted::BorrowedRecord {
            head: Some(sorted::RecordHead {
                key: id.to_string(),
                body_len: is_put.then_some(body.len()),
            }),
            body,
        };
        let prepared =
            sorted::Segment::write_from(&staged, header, sh.data_key.as_ref(), &mut source);
        let mut segment = match prepared {
            Ok(segment) => segment,
            Err(error) => {
                if staged.exists() {
                    let _ = sorted_file::remove(&staged);
                }
                return Err(error);
            }
        };
        let old_path = match segment.publish_as(sh.dir.join(format!("{sequence:010}.seg"))) {
            Ok(path) => path,
            Err(error) => {
                let _ = sorted_file::remove(&staged);
                return Err(error);
            }
        };
        // No fallible work follows publication. The caller updates the index
        // and captures its inverse before a directory sync can fail.
        let location = Loc::Sorted {
            segment: sh.sorted_segments.len(),
            offset: (if is_put { 7 } else { 3 }) + id.len() as u64,
            body_len: body.len(),
            len: line.len() as u64,
        };
        sh.sorted_segments.push(segment);
        sh.pending_sorted_publish = Some(old_path);
        sh.plain_high_seq = sequence;
        sh.dirty_ops = sh.dirty_ops.saturating_add(1);
        sh.dirty_bytes = sh.dirty_bytes.saturating_add(line.len() as u64);
        if !is_put {
            sh.note_delete_marker(line.len() as u64);
        }
        Ok(location)
    }

    /// Emergency process rollback uses the inverse bytes already captured by
    /// the transaction. It does no file I/O and never uses a retired location.
    /// The buffer can temporarily exceed the ordinary seal cap by the undo
    /// payload. The transaction failure path attempts a flush before gate release;
    /// a flush error remains a rollback failure, not a durability success.
    pub(super) fn append_plain_rollback_buffer(
        sh: &mut Shard,
        line: &[u8],
        op: CaptureOp,
    ) -> Result<Loc> {
        if sh.segments.is_empty() {
            let sequence = sh
                .plain_high_seq
                .checked_add(1)
                .ok_or_else(|| Error::Corrupt("rollback segment sequence exhausted".into()))?;
            sh.plain_high_seq = sequence;
            sh.segments.push(sequence);
            if sh.data_key.is_none() {
                sh.open_len = 0;
                sh.file_len = 0;
                sh.open_file = None;
                sh.open_buf.clear();
                sh.open_buf_base = 0;
            } else {
                sh.sorted_rollback_buffer = true;
            }
        }
        let offset = sh.open_len;
        sh.open_buf.extend_from_slice(line);
        sh.open_len += line.len() as u64;
        sh.dirty_ops = sh.dirty_ops.saturating_add(1);
        sh.dirty_bytes = sh.dirty_bytes.saturating_add(line.len() as u64);
        if op == CaptureOp::Delete {
            sh.note_delete_marker(line.len() as u64);
        }
        Ok(Loc::Legacy {
            seg: *sh.segments.last().expect("rollback segment"),
            offset,
            len: line.len() as u64,
        })
    }

    pub(super) fn append_encrypted(
        &self,
        sh: &mut Shard,
        line: &[u8],
        id: &str,
        op: CaptureOp,
        enforce_threshold: bool,
    ) -> Result<Loc> {
        fs::create_dir_all(encrypted_tail_dir(sh))?;
        fs::create_dir_all(encrypted_chunks_dir(sh))?;
        tag_backup_excluded_collection(sh)?;
        if sh.open_chunk_uuid.is_none() {
            Self::open_fresh_encrypted_tail(sh);
        }
        let max_seg = self.opts.max_segment_bytes;
        if sh.open_len > 0 && sh.open_len + line.len() as u64 > max_seg {
            Self::seal_open(&self.opts, &self.meta, sh)?;
            Self::open_fresh_encrypted_tail(sh);
        }
        let csn = self.allocate_csn(&sh.collection, id, op);
        let chunk_uuid = sh.open_chunk_uuid.expect("open chunk uuid");
        let frame_idx = sh.next_frame_counter;
        let offset_in_frame = sh.open_len.saturating_sub(sh.file_len);
        if sh.open_len == sh.file_len {
            sh.open_frame_start_csn = Some(csn);
        }
        // Same doubling as the plaintext path. A flat 4 MiB reserve on first
        // encrypted append charged every written group 4 MiB of capacity the
        // warm set could not trim until the whole store was already over
        // budget. On the live 4 GiB soak that leftover was the extra ~4.6 MiB.
        grow_open_buf_for_append(&mut sh.open_buf, line.len(), max_seg);
        sh.open_buf.extend_from_slice(line);
        sh.open_len += line.len() as u64;
        sh.max_csn = sh.max_csn.max(csn);
        sh.dirty_ops = sh.dirty_ops.saturating_add(1);
        sh.dirty_bytes = sh.dirty_bytes.saturating_add(line.len() as u64);
        if op == CaptureOp::Delete {
            sh.note_delete_marker(line.len() as u64);
        }
        if enforce_threshold
            && (sh.dirty_ops >= self.opts.max_dirty_ops
                || sh.dirty_bytes >= self.opts.max_dirty_bytes)
        {
            Self::sync_open(sh)?;
        }
        Ok(Loc::Chunk {
            chunk_uuid,
            frame_idx,
            offset_in_frame,
            len: line.len() as u64,
        })
    }

    pub(super) fn open_fresh_encrypted_tail(sh: &mut Shard) {
        sh.open_buf.clear();
        sh.open_buf_base = 0;
        sh.open_len = 0;
        sh.file_len = 0;
        sh.open_file = None;
        sh.open_chunk_uuid = Some(Uuid::new_v4());
        sh.next_frame_counter = 0;
        sh.open_frame_start_csn = None;
    }

    pub(super) fn seal_open(
        opts: &LastStoreOptions,
        meta: &Mutex<StoreMeta>,
        sh: &mut Shard,
    ) -> Result<()> {
        if sh.data_key.is_none() {
            return Ok(());
        }
        let Some(chunk_uuid) = sh.open_chunk_uuid else {
            return Ok(());
        };
        if sh.open_len == 0 {
            return Ok(());
        }
        Self::spill_encrypted_open(sh)?;
        fs::create_dir_all(encrypted_tail_dir(sh))?;
        fs::create_dir_all(encrypted_chunks_dir(sh))?;
        if sh.open_file.is_none() {
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(encrypted_tail_path(sh, chunk_uuid))?;
            sh.open_file = Some(CountedFile::new(file, Arc::clone(&sh.fd_gauge)));
        }
        let end_csn = sh.max_csn;
        let seal_payload = encode_seal_record(end_csn);
        let header = FrameHeader {
            chunk_uuid,
            shard: sh.shard,
            start_csn: end_csn.saturating_add(1),
            counter: sh.next_frame_counter,
        };
        let data_key = *sh
            .data_key
            .as_ref()
            .ok_or_else(|| Error::Config("encrypted seal without data key".into()))?;
        let encoded = encode_frame_for_shard(sh, &data_key, header, &seal_payload)?;
        {
            let f = sh.open_file.as_mut().unwrap();
            f.write_all(&encoded)?;
            durability::sync_dirty_file(f)?;
        }
        sh.next_frame_counter = sh.next_frame_counter.saturating_add(1);

        let footer_payload = encode_footer_record(sh, chunk_uuid)?;
        let footer_header = FrameHeader {
            chunk_uuid,
            shard: sh.shard,
            start_csn: end_csn.saturating_add(1),
            counter: sh.next_frame_counter,
        };
        let footer_encoded = encode_frame_for_shard(sh, &data_key, footer_header, &footer_payload)?;
        {
            let f = sh.open_file.as_mut().unwrap();
            let footer_offset = f.metadata()?.len();
            f.write_all(&footer_encoded)?;
            write_footer_trailer(f, footer_offset, footer_encoded.len() as u64)?;
            durability::sync_dirty_file(f)?;
        }
        sh.next_frame_counter = sh.next_frame_counter.saturating_add(1);
        sh.open_file = None;

        let src = encrypted_tail_path(sh, chunk_uuid);
        let dst = encrypted_chunk_path(sh, chunk_uuid);
        fs::rename(&src, &dst)?;
        sh.rename_frame_loc_paths(&src, &dst);
        sync_dir(&encrypted_chunks_dir(sh))?;
        sync_dir(&encrypted_tail_dir(sh))?;
        sync_dir(&sh.dir)?;

        let sealed = SealedChunkMeta {
            collection: sh.collection.clone(),
            shard: sh.shard,
            group_id: shard_group_from_dir(sh),
            chunk_uuid,
            path: dst,
            end_csn,
        };
        if !sh.policy.backup_excluded {
            if let Some(on_seal) = opts.on_seal.as_ref() {
                let _ = on_seal(&sealed);
            }
        }
        meta.lock().expect("poison").sealed_chunks.push(sealed);

        sh.open_buf.clear();
        sh.open_len = 0;
        sh.file_len = 0;
        sh.open_chunk_uuid = None;
        sh.next_frame_counter = 0;
        sh.open_frame_start_csn = None;
        sh.dirty_ops = 0;
        sh.dirty_bytes = 0;
        Ok(())
    }
}
