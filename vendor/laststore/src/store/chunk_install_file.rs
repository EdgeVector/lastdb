//! Bounded file restore. A chunk is invisible until bytes and metadata agree.

use super::*;
use sha2::{Digest, Sha256};

impl LastStore {
    /// Stream an immutable manifest chunk from a file, verify its exact stored
    /// bytes and authenticated scope, then publish it into an inactive store.
    pub fn install_manifest_chunk_file(
        &self,
        scope: &SealedChunkMeta,
        source: &Path,
        sha256: &str,
        bytes: u64,
    ) -> Result<SealedChunkMeta> {
        if scope.collection.is_empty()
            || scope.collection.contains(['/', '\\', '\0'])
            || scope.collection == "."
            || scope.collection == ".."
            || u32::from(scope.shard) >= (1u32 << self.opts.shard_bits)
            || scope
                .group_id
                .is_some_and(|group| group >= (1u32 << self.opts.hash_group_bits))
        {
            return Err(Error::Config(
                "restore chunk scope exceeds its configured layout".into(),
            ));
        }
        let metadata = fs::symlink_metadata(source)?;
        if !metadata.file_type().is_file() || metadata.len() != bytes {
            return Err(Error::Corrupt(
                "restore chunk file type or length differs".into(),
            ));
        }
        let directory = self.handle_dir(&scope.collection, scope.shard, scope.group_id);
        fs::create_dir_all(&directory)?;
        let temporary = directory.join(format!(".restore-{}.installing", Uuid::new_v4()));
        copy_verified(source, &temporary, sha256, bytes)?;
        let mut input = File::open(&temporary)?;
        let mut prefix = [0u8; 8];
        input.read_exact(&mut prefix)?;
        let result = if sorted_file::is_piece(&prefix) {
            if bytes > sorted_file::PART_BYTES {
                return Err(Error::Corrupt(
                    "sorted restore piece exceeds its physical cap".into(),
                ));
            }
            let data = fs::read(&temporary)?;
            self.install_manifest_chunk(
                &scope.collection,
                scope.shard,
                scope.group_id,
                scope.chunk_uuid,
                &data,
            )
        } else if sorted_file::is_manifest(&prefix) {
            if bytes != sorted_file::MANIFEST_BYTES as u64 {
                return Err(Error::Corrupt(
                    "sorted restore manifest length differs".into(),
                ));
            }
            self.install_manifest_chunk(
                &scope.collection,
                scope.shard,
                scope.group_id,
                scope.chunk_uuid,
                &fs::read(&temporary)?,
            )
        } else if self.opts.packaging == PackagingMode::Plain
            || sorted::Segment::recognizes(&temporary)?
        {
            self.publish_numbered_file(scope, &temporary)
        } else {
            let end_csn = verify_encrypted_file_stream(
                &temporary,
                scope,
                self.opts
                    .data_key
                    .as_ref()
                    .ok_or_else(|| Error::Config("encrypted restore has no data key".into()))?,
            )?;
            if end_csn != scope.end_csn {
                return Err(Error::Corrupt("streamed restore seal CSN differs".into()));
            }
            let chunks = directory.join("chunks");
            fs::create_dir_all(&chunks)?;
            let destination = chunks.join(format!("{}.seg", scope.chunk_uuid));
            fs::rename(&temporary, &destination)?;
            sync_dir(&chunks)?;
            self.forget_restored_scope(scope);
            Ok(SealedChunkMeta {
                path: destination,
                ..scope.clone()
            })
        };
        if temporary.exists() {
            fs::remove_file(&temporary)?;
        }
        result
    }

    fn publish_numbered_file(
        &self,
        scope: &SealedChunkMeta,
        temporary: &Path,
    ) -> Result<SealedChunkMeta> {
        let seq = plain_segment_log_seq_for_uuid(
            &scope.collection,
            scope.shard,
            scope.group_id,
            scope.chunk_uuid,
        )
        .ok_or_else(|| {
            Error::Corrupt("numbered restore chunk UUID differs from its exact scope".into())
        })?;
        if scope.end_csn != 0 {
            return Err(Error::Corrupt(
                "numbered restore chunk seal CSN differs".into(),
            ));
        }
        if sorted::Segment::recognizes(temporary)? {
            let segment = sorted::Segment::open(temporary, self.opts.data_key.as_ref())?;
            if segment.header.shard != scope.shard {
                return Err(Error::Corrupt("sorted restore chunk shard differs".into()));
            }
            segment.verify(self.opts.data_key.as_ref())?;
        }
        let directory = self.handle_dir(&scope.collection, scope.shard, scope.group_id);
        let destination = directory.join(format!("{seq:010}.seg"));
        fs::rename(temporary, &destination)?;
        sync_dir(&directory)?;
        self.complete_sorted_restore(&scope.collection, scope.shard, scope.group_id)?;
        self.forget_restored_scope(scope);
        Ok(SealedChunkMeta {
            path: destination,
            ..scope.clone()
        })
    }

    fn forget_restored_scope(&self, scope: &SealedChunkMeta) {
        let key = (scope.collection.clone(), scope.shard, scope.group_id);
        self.shards.lock().expect("poison").remove(&key);
        self.key_index.lock().expect("poison").forget(&key);
    }
}

fn copy_verified(source: &Path, destination: &Path, expected: &str, bytes: u64) -> Result<()> {
    let mut input = File::open(source)?;
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)?;
    let mut hash = Sha256::new();
    let mut count = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let size = input.read(&mut buffer)?;
        if size == 0 {
            break;
        }
        count = count.saturating_add(size as u64);
        if count > bytes {
            return Err(Error::Corrupt(
                "restore chunk exceeds its exact byte prefix".into(),
            ));
        }
        output.write_all(&buffer[..size])?;
        hash.update(&buffer[..size]);
    }
    if count != bytes || format!("{:x}", hash.finalize()) != expected {
        return Err(Error::Corrupt(
            "restore chunk file SHA or length differs".into(),
        ));
    }
    durability::sync_dirty_file(&output)?;
    Ok(())
}

struct FrameState {
    counter: u64,
    max_csn: u64,
    sealed: bool,
}

fn verify_encrypted_file_stream(
    path: &Path,
    scope: &SealedChunkMeta,
    key: &[u8; 32],
) -> Result<u64> {
    let mut input = File::open(path)?;
    let length = input.metadata()?.len();
    let mut offset = 0u64;
    let mut state = FrameState {
        counter: 0,
        max_csn: 0,
        sealed: false,
    };
    while offset < length {
        let mut first = [0u8; 8];
        input.read_exact(&mut first)?;
        if &first == FOOTER_TRAILER_MAGIC {
            if !state.sealed || length - offset != FOOTER_TRAILER_LEN as u64 {
                return Err(Error::Corrupt("invalid restore footer trailer".into()));
            }
            let mut tail = [0u8; FOOTER_TRAILER_LEN - 8];
            input.read_exact(&mut tail)?;
            break;
        }
        let mut header = vec![0u8; frame::header_size()];
        header[..8].copy_from_slice(&first);
        input.read_exact(&mut header[8..])?;
        let size = frame::encoded_len(&header)?;
        if size > frame::MAX_DECOMPRESSED_FRAME_BYTES + frame::header_size() + 64
            || size as u64 > length - offset
        {
            return Err(Error::Corrupt(
                "restore frame exceeds its bounded extent".into(),
            ));
        }
        header.resize(size, 0);
        input.read_exact(&mut header[frame::header_size()..])?;
        let decoded = frame::decode_frame(key, &header)?;
        verify_frame(&decoded, scope, &mut state)?;
        offset = offset.saturating_add(size as u64);
    }
    if !state.sealed {
        return Err(Error::Corrupt("streamed restore chunk lacks a seal".into()));
    }
    Ok(state.max_csn)
}

fn verify_frame(
    decoded: &frame::DecodedFrame,
    scope: &SealedChunkMeta,
    state: &mut FrameState,
) -> Result<()> {
    if decoded.header.shard != scope.shard
        || decoded.header.chunk_uuid != scope.chunk_uuid
        || decoded.header.counter != state.counter
    {
        return Err(Error::AeadAuthFail);
    }
    state.counter = state.counter.saturating_add(1);
    if is_seal_record(&decoded.payload) {
        let end = decode_seal_record(&decoded.payload)?;
        if state.sealed || decoded.header.start_csn != end.saturating_add(1) {
            return Err(Error::AeadAuthFail);
        }
        state.max_csn = state.max_csn.max(end);
        state.sealed = true;
    } else if is_footer_record(&decoded.payload) {
        if !state.sealed || decoded.header.start_csn != state.max_csn.saturating_add(1) {
            return Err(Error::AeadAuthFail);
        }
    } else {
        if state.sealed {
            return Err(Error::Corrupt(
                "streamed chunk has records after its seal".into(),
            ));
        }
        let count = count_complete_records(&decoded.payload, false)?;
        if count > 0 {
            state.max_csn = state
                .max_csn
                .max(decoded.header.start_csn.saturating_add(count - 1));
        }
    }
    Ok(())
}
