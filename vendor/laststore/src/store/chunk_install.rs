use super::*;

impl LastStore {
    /// Install pristine bytes for a previously known or quarantined chunk UUID.
    pub fn install_chunk(&self, chunk_uuid: Uuid, bytes: &[u8]) -> Result<SealedChunkMeta> {
        let target = find_chunk_file(&self.root, chunk_uuid, "quarantine")?
            .or(find_chunk_file(&self.root, chunk_uuid, "chunks")?)
            .ok_or_else(|| Error::Corrupt(format!("unknown chunk {chunk_uuid}")))?;
        let (collection, shard, group, old_path) = target;
        let shard_dir = self.handle_dir(&collection, shard, group);
        let mut verify_shard = Shard {
            dir: shard_dir.clone(),
            collection: collection.clone(),
            shard,
            data_key: self.opts.data_key,
            policy: self.opts.collection_policy(&collection),
            ..Default::default()
        };
        let tmp = shard_dir
            .join("chunks")
            .join(format!("{chunk_uuid}.seg.installing"));
        fs::create_dir_all(encrypted_chunks_dir(&verify_shard))?;
        fs::write(&tmp, bytes)?;
        let decoded = decode_encrypted_file(&verify_shard, chunk_uuid, &tmp, bytes, false)?;
        if !decoded.sealed {
            let _ = fs::remove_file(&tmp);
            return Err(Error::Corrupt(format!(
                "installed chunk {chunk_uuid} has no seal record"
            )));
        }
        apply_encrypted_frames(&mut verify_shard, chunk_uuid, &decoded.frames, false)?;

        let dst = encrypted_chunk_path(&verify_shard, chunk_uuid);
        fs::rename(&tmp, &dst)?;
        if old_path != dst {
            let _ = fs::remove_file(old_path);
        }
        sync_dir(&encrypted_chunks_dir(&verify_shard))?;

        let maybe_handle = {
            let map = self.shards.lock().expect("poison");
            map.handles
                .iter()
                .find(|((c, s, _), _)| c == &collection && *s == shard)
                .map(|(_, handle)| handle.clone())
        };
        if let Some(handle) = maybe_handle {
            let mut sh = handle.lock().expect("poison");
            load_verified_sealed_chunk(&mut sh, chunk_uuid, &dst)?;
        }

        Ok(SealedChunkMeta {
            collection,
            shard,
            group_id: group,
            chunk_uuid,
            path: dst,
            end_csn: decoded.max_csn,
        })
    }

    /// Install pristine sealed-chunk bytes described by an external manifest.
    ///
    /// Restore starts from an empty store, so the chunk UUID is not yet known
    /// locally. The caller supplies the manifest's collection and shard.
    ///
    /// - **Frame-AEAD** packaging: decode + authenticate frame AAD, then place
    ///   under `chunks/{uuid}.seg`.
    /// - **Plain SegmentLog** packaging (Tom's primary Mini layout): write the
    ///   numbered `{seq:010}.seg` opaque sealed unit; content-addressed
    ///   integrity is the caller's sha256 check (no frame AEAD seal records).
    pub fn install_manifest_chunk(
        &self,
        collection: &str,
        shard: u16,
        group_id: Option<u32>,
        chunk_uuid: Uuid,
        bytes: &[u8],
    ) -> Result<SealedChunkMeta> {
        if sorted_file::is_piece(bytes) {
            let directory = self.handle_dir(collection, shard, group_id);
            let path = sorted_file::install_piece(&directory, chunk_uuid, bytes)?;
            self.complete_sorted_restore(collection, shard, group_id)?;
            return Ok(SealedChunkMeta {
                collection: collection.into(),
                shard,
                group_id,
                chunk_uuid,
                path,
                end_csn: 0,
            });
        }
        // Sorted units use numbered paths under either packaging mode. Their
        // own block AEAD differs from the legacy frame-AEAD chunk container.
        if self.opts.packaging == PackagingMode::Plain || sorted::Segment::recognizes_bytes(bytes) {
            return self
                .install_numbered_segment_chunk(collection, shard, group_id, chunk_uuid, bytes);
        }

        let shard_dir = self.handle_dir(collection, shard, group_id);
        let mut verify_shard = Shard {
            dir: shard_dir.clone(),
            collection: collection.to_string(),
            shard,
            data_key: self.opts.data_key,
            policy: self.opts.collection_policy(collection),
            ..Default::default()
        };
        fs::create_dir_all(encrypted_chunks_dir(&verify_shard))?;
        let tmp = shard_dir
            .join("chunks")
            .join(format!("{chunk_uuid}.seg.installing"));
        let _ = fs::remove_file(&tmp);
        fs::write(&tmp, bytes)?;
        let decoded = match decode_encrypted_file(&verify_shard, chunk_uuid, &tmp, bytes, false) {
            Ok(decoded) => decoded,
            Err(err) => {
                let _ = fs::remove_file(&tmp);
                return Err(err);
            }
        };
        if !decoded.sealed {
            let _ = fs::remove_file(&tmp);
            return Err(Error::Corrupt(format!(
                "installed chunk {chunk_uuid} has no seal record"
            )));
        }
        if let Err(err) =
            apply_encrypted_frames(&mut verify_shard, chunk_uuid, &decoded.frames, false)
        {
            let _ = fs::remove_file(&tmp);
            return Err(err);
        }

        let dst = encrypted_chunk_path(&verify_shard, chunk_uuid);
        fs::rename(&tmp, &dst)?;
        sync_dir(&encrypted_chunks_dir(&verify_shard))?;

        let maybe_handle = {
            let map = self.shards.lock().expect("poison");
            map.handles
                .iter()
                .find(|((c, s, g), _)| c == collection && *s == shard && *g == group_id)
                .map(|(_, handle)| handle.clone())
        };
        if let Some(handle) = maybe_handle {
            let mut sh = handle.lock().expect("poison");
            load_verified_sealed_chunk(&mut sh, chunk_uuid, &dst)?;
        }

        Ok(SealedChunkMeta {
            collection: collection.to_string(),
            shard,
            group_id,
            chunk_uuid,
            path: dst,
            end_csn: decoded.max_csn,
        })
    }

    /// Install a numbered legacy/plain or sorted unit from a cloud backup.
    pub(super) fn install_numbered_segment_chunk(
        &self,
        collection: &str,
        shard: u16,
        group_id: Option<u32>,
        chunk_uuid: Uuid,
        bytes: &[u8],
    ) -> Result<SealedChunkMeta> {
        if sorted_file::is_manifest(bytes) && bytes.len() != sorted_file::MANIFEST_BYTES {
            return Err(Error::Corrupt("invalid sorted manifest length".into()));
        }
        // Recover seq from deterministic uuid (collection, shard, group, seq).
        // Prefer end_csn when the backup cutter packed seq there (0 is valid
        // for seq 0 — try direct match first, then scan a bounded range).
        let seq = plain_segment_log_seq_for_uuid(collection, shard, group_id, chunk_uuid)
            .ok_or_else(|| {
                Error::Corrupt(format!(
                    "plain segment log uuid {chunk_uuid} does not match any seq for \
                     {collection}/{shard}/{group_id:?}"
                ))
            })?;
        let shard_dir = self.handle_dir(collection, shard, group_id);
        fs::create_dir_all(&shard_dir)?;
        let dst = shard_dir.join(format!("{seq:010}.seg"));
        let tmp = shard_dir.join(format!("{seq:010}.seg.installing"));
        let _ = fs::remove_file(&tmp);
        if sorted_file::is_manifest(bytes) {
            let marker = shard_dir.join(".sorted-restore-pending");
            fs::create_dir_all(&marker)?;
            // A later intent/manifest failure must not leave a cached empty
            // group visible. Restore owns an empty store, not live writers.
            let key = (collection.to_string(), shard, group_id);
            self.shards.lock().expect("poison").remove(&key);
            self.key_index.lock().expect("poison").forget(&key);
            sync_dir(&shard_dir)?;
            // Preserve the exact generation intent before its staged manifest.
            // An empty marker directory after a crash also blocks all readers.
            let intent = marker.join(format!("{seq:010}.pending"));
            fs::write(&intent, bytes)?;
            durability::sync_dirty_file(&File::open(intent)?)?;
            sync_dir(&marker)?;
        }
        fs::write(&tmp, bytes)?;
        let result = (|| {
            if sorted_file::is_manifest(bytes) {
                // The completion path validates once, after every piece is
                // present, and publishes against the durable generation intent.
                durability::sync_dirty_file(&File::open(&tmp)?)?;
                return sync_dir(&shard_dir);
            }
            if sorted::Segment::recognizes(&tmp)? {
                let segment = sorted::Segment::open(&tmp, self.opts.data_key.as_ref())?;
                if segment.header.shard != shard {
                    return Err(Error::Corrupt(
                        "installed sorted chunk belongs to another shard".into(),
                    ));
                }
                segment.verify(self.opts.data_key.as_ref())?;
            }
            // The verified bytes must reach durability before publication.
            durability::sync_dirty_file(&File::open(&tmp)?)?;
            fs::rename(&tmp, &dst)?;
            sync_dir(&shard_dir)
        })();
        if let Err(error) = result {
            let _ = fs::remove_file(&tmp);
            return Err(error);
        }
        self.complete_sorted_restore(collection, shard, group_id)?;
        let key = (collection.to_string(), shard, group_id);
        self.shards.lock().expect("poison").remove(&key);
        self.key_index.lock().expect("poison").forget(&key);
        Ok(SealedChunkMeta {
            collection: collection.to_string(),
            shard,
            group_id,
            chunk_uuid,
            path: dst,
            end_csn: 0,
        })
    }

    pub(super) fn complete_sorted_restore(
        &self,
        collection: &str,
        shard: u16,
        group: Option<u32>,
    ) -> Result<()> {
        let directory = self.handle_dir(collection, shard, group);
        let marker = directory.join(".sorted-restore-pending");
        if !marker.exists() {
            return Ok(());
        }
        let mut pending = false;
        let mut intents = 0;
        for entry in fs::read_dir(&marker)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let sequence = name
                .strip_suffix(".pending")
                .and_then(|name| name.parse::<u64>().ok())
                .ok_or_else(|| Error::Corrupt("invalid sorted restore intent name".into()))?;
            intents += 1;
            if entry.metadata()?.len() != sorted_file::MANIFEST_BYTES as u64 {
                return Err(Error::Corrupt("incomplete sorted restore intent".into()));
            }
            let expected = fs::read(entry.path())?;
            if !sorted_file::is_manifest(&expected) {
                return Err(Error::Corrupt("invalid sorted restore intent".into()));
            }
            let staged = directory.join(format!("{sequence:010}.seg.installing"));
            let destination = directory.join(format!("{sequence:010}.seg"));
            let path = if staged.exists() {
                &staged
            } else {
                &destination
            };
            if !path.exists() {
                pending = true;
                continue;
            }
            if fs::metadata(path)?.len() != expected.len() as u64 || fs::read(path)? != expected {
                return Err(Error::Corrupt(
                    "sorted restore differs from durable intent".into(),
                ));
            }
            if !sorted_file::parts_present(path)? {
                pending = true;
                continue;
            }
            let segment = sorted::Segment::open(path, self.opts.data_key.as_ref())?;
            if segment.header.shard != shard {
                return Err(Error::Corrupt(
                    "restored sorted group has the wrong shard".into(),
                ));
            }
            segment.verify(self.opts.data_key.as_ref())?;
            if path == &staged {
                fs::rename(&staged, &destination)?;
            }
            sync_dir(&directory)?;
            fs::remove_file(entry.path())?;
            sync_dir(&marker)?;
            let key = (collection.to_string(), shard, group);
            self.shards.lock().expect("poison").remove(&key);
            self.key_index.lock().expect("poison").forget(&key);
        }
        // A crash can leave the marker empty before its first intent, or after
        // the last intent retires. Do not infer completion from absence. A
        // repeated manifest install validates a generation and clears it safely.
        if intents > 0 && !pending {
            fs::remove_dir(marker)?;
            sync_dir(&directory)?;
        }
        Ok(())
    }
}
