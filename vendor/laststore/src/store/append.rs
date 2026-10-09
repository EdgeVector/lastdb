use super::*;

impl LastStore {
    pub(super) fn allocate_csn(&self, collection: &str, id: &str, op: CaptureOp) -> u64 {
        let mut meta = self.meta.lock().expect("poison");
        let csn = meta.next_csn;
        meta.next_csn = meta.next_csn.saturating_add(1);
        if meta.capture_suspended {
            return csn;
        }
        let event = CaptureEvent {
            csn,
            collection: collection.to_string(),
            id: id.to_string(),
            op,
        };
        if let Some(hook) = self.opts.capture_hook.as_ref() {
            if hook(&event).is_err() {
                meta.capture_suspended = true;
                return csn;
            }
        }
        meta.capture_log.push(event);
        csn
    }

    pub(super) fn sync_open(sh: &mut Shard) -> Result<()> {
        if let Some(staged) = &sh.pending_sorted_publish {
            match fs::remove_file(staged) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            sync_dir(&sh.dir)?;
            sh.pending_sorted_publish = None;
        }
        if sh.dirty_ops == 0 && sh.dirty_bytes == 0 {
            return Ok(());
        }
        if Self::sorted_snapshot_needed(sh) {
            // Merge only after the fifth generation and its inverse exist,
            // so the rewrite can skip every superseded body.
            // An I/O failure can force rollback to retain its captured inverse
            // in the buffer. Once I/O recovers, stream that buffer straight to
            // bounded sorted files; never spill it as an oversized raw tail.
            return Self::rewrite_sorted_snapshot(sh);
        }
        Self::spill_open(sh)?;
        if let Some(f) = sh.open_file.as_mut() {
            durability::sync_dirty_file(f)?;
        }
        if sh.sorted_tail_directory_dirty {
            let directory = encrypted_tail_dir(sh);
            sync_dir(&directory)?;
            sync_dir(directory.parent().expect("tail generation parent"))?;
            sync_dir(&sh.dir)?;
            sh.sorted_tail_directory_dirty = false;
        }
        sh.dirty_ops = 0;
        sh.dirty_bytes = 0;
        Ok(())
    }

    pub(super) fn spill_open(sh: &mut Shard) -> Result<()> {
        if sh.data_key.is_some() {
            return Self::spill_encrypted_open(sh);
        }
        let buffered_end = sh.open_buf_base + sh.open_buf.len() as u64;
        if buffered_end == 0 && sh.file_len == 0 && sh.open_file.is_none() {
            // A direct sorted write has no append file. Only a prior spill
            // leaves a file to reopen after an interrupted sync.
            return Ok(());
        }
        if sh.open_file.is_none() {
            let seg = *sh.segments.last().expect("seg");
            let path = sh.dir.join(format!("{seg:010}.seg"));
            // A previous spill can have written the whole buffer before its
            // file sync failed. Reopen that file for the retry's sync; never
            // clear dirty state just because the bytes reached the file.
            let file = OpenOptions::new()
                .create(sh.file_len < buffered_end)
                .append(true)
                .open(path)?;
            sh.open_file = Some(CountedFile::new(file, Arc::clone(&sh.fd_gauge)));
        }
        if sh.file_len >= buffered_end {
            return Ok(());
        }
        // `file_len` is a file offset; `open_buf` starts at `open_buf_base`.
        let payload = &sh.open_buf[(sh.file_len - sh.open_buf_base) as usize..];
        sh.open_file.as_mut().unwrap().write_all(payload)?;
        sh.file_len = sh.open_buf_base + sh.open_buf.len() as u64;
        Ok(())
    }

    pub(super) fn spill_encrypted_open(sh: &mut Shard) -> Result<()> {
        let needs_spill = sh.file_len < sh.open_buf_base + sh.open_buf.len() as u64;
        if !needs_spill
            && sh.file_len == 0
            && sh.open_file.is_none()
            && sh.open_chunk_uuid.is_none()
        {
            // A direct sorted write has no encrypted append chunk to sync.
            return Ok(());
        }
        let data_key = sh
            .data_key
            .as_ref()
            .ok_or_else(|| Error::Config("encrypted spill without data key".into()))?;
        let chunk_uuid = if needs_spill {
            *sh.open_chunk_uuid.get_or_insert_with(Uuid::new_v4)
        } else {
            sh.open_chunk_uuid.ok_or_else(|| {
                Error::Corrupt("dirty encrypted tail has no open chunk to sync".into())
            })?
        };
        if needs_spill {
            fs::create_dir_all(encrypted_tail_dir(sh))?;
        }
        if sh.open_file.is_none() {
            let path = encrypted_tail_path(sh, chunk_uuid);
            let new_file = !path.exists();
            let file = OpenOptions::new()
                .create(needs_spill)
                .append(true)
                .open(path)?;
            sh.open_file = Some(CountedFile::new(file, Arc::clone(&sh.fd_gauge)));
            sh.sorted_tail_directory_dirty |= sh.uses_sorted_index() && new_file;
        }
        if !needs_spill {
            return Ok(());
        }
        let payload = sh.open_buf[(sh.file_len - sh.open_buf_base) as usize..].to_vec();
        let header = FrameHeader {
            chunk_uuid,
            shard: sh.shard,
            start_csn: sh.open_frame_start_csn.unwrap_or(sh.max_csn),
            counter: sh.next_frame_counter,
        };
        let encoded = encode_frame_for_shard(sh, data_key, header, &payload)?;
        let path = encrypted_tail_path(sh, chunk_uuid);
        let disk_offset = sh.open_file.as_ref().unwrap().metadata()?.len();
        if sh.uses_sorted_index()
            && disk_offset.saturating_add(encoded.len() as u64) > sorted_file::PART_BYTES
        {
            // The mutation is already indexed before sorted threshold sync.
            // Stream the tail directly into bounded sorted output instead of
            // creating an oversized encrypted append file.
            return Self::seal_sorted_tail(sh);
        }
        {
            let f = sh.open_file.as_mut().unwrap();
            if let Err(error) = f.write_all(&encoded) {
                // A partial write consumes this nonce domain. A sorted retry
                // streams its buffered records to a new generation instead.
                sh.sorted_rollback_buffer |= sh.uses_sorted_index();
                return Err(error.into());
            }
        }
        record_frame(
            sh,
            (chunk_uuid, header.counter),
            path,
            disk_offset,
            encoded.len() as u64,
            &payload,
        );
        sh.next_frame_counter = sh.next_frame_counter.saturating_add(1);
        sh.file_len = sh.open_buf_base + sh.open_buf.len() as u64;
        sh.open_frame_start_csn = None;
        if sh.uses_sorted_index() {
            sh.open_buf = Vec::new();
            sh.open_buf_base = sh.file_len;
        }
        Ok(())
    }

    pub(super) fn append(
        &self,
        sh: &mut Shard,
        line: &[u8],
        id: &str,
        op: CaptureOp,
        previous_len: Option<u64>,
    ) -> Result<Loc> {
        self.append_with_threshold(sh, line, id, op, previous_len, true)
    }

    /// Append one transaction operation without a mid-batch group sync.
    /// The transaction checks the threshold after every operation is present.
    pub(super) fn append_transaction(
        &self,
        sh: &mut Shard,
        line: &[u8],
        id: &str,
        op: CaptureOp,
        previous_len: Option<u64>,
    ) -> Result<Loc> {
        self.append_with_threshold(sh, line, id, op, previous_len, false)
    }

    pub(super) fn append_with_threshold(
        &self,
        sh: &mut Shard,
        line: &[u8],
        id: &str,
        op: CaptureOp,
        previous_len: Option<u64>,
        enforce_threshold: bool,
    ) -> Result<Loc> {
        if sh.uses_sorted_index() && sh.sorted_legacy_encrypted {
            Self::compact_sorted_shard(sh)?;
        }
        if sh.uses_sorted_index() && line.len() as u64 > sh.max_sorted_tail_bytes {
            if sh.data_key.is_some() {
                let csn = self.allocate_csn(&sh.collection, id, op);
                sh.max_csn = sh.max_csn.max(csn);
            }
            return Self::append_large_sorted(sh, line, id, op, previous_len);
        }
        if sh.uses_sorted_index()
            && sorted_unsealed_bytes(sh) > 0
            && sorted_unsealed_bytes(sh).saturating_add(line.len() as u64)
                > sh.max_sorted_tail_bytes
        {
            Self::seal_sorted_tail(sh)?;
        }
        if sh.data_key.is_some() {
            return self.append_encrypted(
                sh,
                line,
                id,
                op,
                enforce_threshold && !sh.uses_sorted_index(),
            );
        }
        fs::create_dir_all(&sh.dir)?;
        tag_backup_excluded_collection(sh)?;
        let max_seg = self.opts.max_segment_bytes;
        let roll = sh.segments.is_empty()
            || (sh.open_len > 0 && sh.open_len + line.len() as u64 > max_seg);
        if roll {
            Self::sync_open(sh)?;
            if let Some(&prev) = sh.segments.last() {
                // Only a buffer that still starts at offset 0 is the whole
                // segment. Past a base advance `open_buf` is a suffix, and
                // `seg_bytes` is indexed from the start of the segment — the
                // file already holds all of it, so hand the sealed segment to
                // `read_at`'s disk path rather than caching a partial copy
                // under a key that promises the whole thing.
                if sh.open_buf_base == 0 && !sh.open_buf.is_empty() {
                    sh.seg_bytes.insert(prev, std::mem::take(&mut sh.open_buf));
                }
            }
            let next = sh
                .plain_high_seq
                .max(sh.segments.last().copied().unwrap_or(0))
                .checked_add(1)
                .ok_or_else(|| Error::Corrupt("segment sequence exhausted".into()))?;
            sh.plain_high_seq = next;
            sh.segments.push(next);
            sh.open_len = 0;
            sh.file_len = 0;
            sh.open_file = None;
            sh.open_chunk_uuid = sh.data_key.map(|_| Uuid::new_v4());
            sh.next_frame_counter = 0;
            sh.open_buf.clear();
            sh.open_buf_base = 0;
        }
        let seg = *sh.segments.last().expect("seg");
        let offset = sh.open_len;
        // Grow toward the segment cap in doubling steps. A flat 4 MiB
        // reservation on first append made every group that had ever taken a
        // write cost the warm budget 4 MiB, nearly all of it capacity that
        // would never be filled — `estimate_shard_residency` charges
        // capacity, and on a home whose groups are mostly small that is the
        // single largest thing the budget is spent on. Doubling keeps appends
        // amortised without over-reserving a group that holds a few kilobytes,
        // and the cap keeps a large segment from over-reserving on its last
        // growth step.
        grow_open_buf_for_append(&mut sh.open_buf, line.len(), max_seg);
        sh.open_buf.extend_from_slice(line);
        sh.open_len += line.len() as u64;
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
        Ok(Loc::Legacy {
            seg,
            offset,
            len: line.len() as u64,
        })
    }

    pub(super) fn sync_point_append_threshold(&self, sh: &mut Shard) -> Result<()> {
        if Self::sorted_snapshot_needed(sh)
            || ((sh.pending_sorted_publish.is_some()
                || (sh.uses_sorted_index() && sh.data_key.is_some()))
                && (sh.dirty_ops >= self.opts.max_dirty_ops
                    || sh.dirty_bytes >= self.opts.max_dirty_bytes))
        {
            Self::sync_open(sh)?;
        }
        Ok(())
    }
}
