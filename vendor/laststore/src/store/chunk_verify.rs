use super::*;

impl LastStore {
    /// Sealed encrypted chunks that are eligible for backup upload.
    ///
    /// Collections marked `backup_excluded` return an empty list even though
    /// they still seal and compact locally. Includes plain SegmentLog numbered
    /// segs as well as frame-AEAD `chunks/` units.
    pub fn backup_chunk_paths(&self, collection: &str) -> Result<Vec<PathBuf>> {
        if self.opts.collection_policy(collection).backup_excluded {
            return Ok(Vec::new());
        }
        Ok(self
            .enumerate_chunks(collection)?
            .into_iter()
            .map(|chunk| chunk.path)
            .collect())
    }

    /// Sealed encrypted chunks for `collection`, including backup-excluded
    /// collections for local restore and verification callers.
    ///
    /// Covers both frame-AEAD `chunks/{uuid}.seg` homes and plain SegmentLog
    /// numbered `{seq:010}.seg` files (Tom's primary Mini layout).
    pub fn enumerate_chunks(&self, collection: &str) -> Result<Vec<SealedChunkMeta>> {
        let mut out = Vec::new();
        for (shard, group) in self.handles_on_disk(collection)? {
            out.extend(self.enumerate_group_chunks(collection, shard, group)?);
        }
        out.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(out)
    }

    /// Name physical chunks under exactly one known group, without opening a
    /// resident handle or reading chunk bodies. Multipart manifests contribute
    /// their named parts; this inventory is not an integrity verification.
    ///
    /// The retirement owner must hold its physical rewrite lease while it
    /// consumes this inventory. This method does not acquire that lease or
    /// authorize deletion. `None` addresses a legacy ungrouped shard explicitly.
    pub fn enumerate_group_chunks(
        &self,
        collection: &str,
        shard: u16,
        group: Option<u32>,
    ) -> Result<Vec<SealedChunkMeta>> {
        if u32::from(shard) >= (1u32 << self.opts.shard_bits)
            || group.is_some_and(|group| group >= (1u32 << self.opts.hash_group_bits))
        {
            return Err(Error::Config(
                "chunk inventory scope exceeds the configured layout".into(),
            ));
        }
        let shard_dir = self.handle_dir(collection, shard, group);
        let chunks_dir = encrypted_chunks_dir_for(&shard_dir);
        let mut files = encrypted_files(&chunks_dir)?;
        // Numbered sorted generations can be encrypted and can coexist with
        // legacy chunks. Packaging and directory presence cannot exclude them.
        // Numbered paths and chunks/<uuid>.seg paths cannot overlap.
        files.extend(plain_segment_log_files(
            &shard_dir, collection, shard, group,
        )?);
        let mut out: Vec<_> = files
            .into_iter()
            .map(|(chunk_uuid, path)| SealedChunkMeta {
                collection: collection.to_string(),
                shard,
                group_id: group,
                chunk_uuid,
                path,
                end_csn: 0,
            })
            .collect();
        out.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(out)
    }

    /// Verify one sealed encrypted chunk by UUID.
    ///
    /// This has to *find* the chunk first, which walks the whole store — up to
    /// twice, once per packaging. Callers that already hold the chunk's
    /// [`SealedChunkMeta`] (anything iterating [`Self::enumerate_chunks`])
    /// should call [`Self::verify_chunk_at`] instead; see its docs for what
    /// the search costs on a real home.
    pub fn verify_chunk(&self, chunk_uuid: Uuid) -> Result<SealedChunkMeta> {
        if let Some((collection, shard, group, path)) =
            find_chunk_file(&self.root, chunk_uuid, "chunks")?
        {
            return self.verify_frame_aead_chunk_at(&collection, shard, group, chunk_uuid, path);
        }

        // Plain SegmentLog: uuid is deterministic from collection/shard/seq.
        if let Some(meta) = find_plain_segment_log_chunk(&self.root, chunk_uuid)? {
            return self.verify_chunk_at(&meta);
        }

        Err(Error::Corrupt(format!("missing chunk {chunk_uuid}")))
    }

    /// Verify a sealed chunk whose location is already known.
    ///
    /// Same contract as [`Self::verify_chunk`] — returns the chunk's metadata,
    /// or `Corrupt` if the file is missing, unreadable, or (frame-AEAD) has no
    /// seal record — but it verifies the file `meta` already points at instead
    /// of searching the store for it.
    ///
    /// The distinction matters because `enumerate_chunks` returns full
    /// `SealedChunkMeta` values and the cloud-backup manifest walk then passed
    /// only `meta.chunk_uuid` back into `verify_chunk`, throwing away the
    /// location it had just computed. Measured on a 9.5 GB production home
    /// (32 collections, 1024 hash groups, 19,435 chunks): enumerating every
    /// chunk in the store takes 0.8 s, while re-finding them one uuid at a
    /// time costs ~0.164 s each. A publish cycle runs the enumerate+verify
    /// loop twice, so the search alone accounted for ~1.8 h of directory
    /// walking per cycle before any bytes were uploaded.
    pub fn verify_chunk_at(&self, meta: &SealedChunkMeta) -> Result<SealedChunkMeta> {
        if meta
            .path
            .extension()
            .and_then(|extension| extension.to_str())
            == Some("part")
        {
            sorted_file::verify_piece(&meta.path, meta.chunk_uuid)?;
            return Ok(meta.clone());
        }
        // Frame-AEAD chunks are the ones that live in a `chunks/` directory;
        // that is exactly the discriminator `verify_chunk` gets from
        // `find_chunk_file(.., "chunks")`, and what `enumerate_chunks` used to
        // decide which of the two listings this path came from.
        let in_chunks_dir = meta
            .path
            .parent()
            .and_then(|p| p.file_name())
            .is_some_and(|name| name == "chunks");

        if in_chunks_dir {
            return self.verify_frame_aead_chunk_at(
                &meta.collection,
                meta.shard,
                meta.group_id,
                meta.chunk_uuid,
                meta.path.clone(),
            );
        }

        // Plain SegmentLog: presence + readable bytes, as in `verify_chunk`.
        // Report a missing file the same way the search path would, so callers
        // cannot tell the two apart by error kind.
        if !meta.path.exists() {
            return Err(Error::Corrupt(format!("missing chunk {}", meta.chunk_uuid)));
        }
        if sorted::Segment::recognizes(&meta.path)? {
            let segment = sorted::Segment::open(&meta.path, self.opts.data_key.as_ref())?;
            if segment.header.shard != meta.shard {
                return Err(Error::Corrupt(
                    "sorted chunk belongs to another shard".into(),
                ));
            }
            segment.verify(self.opts.data_key.as_ref())?;
        }
        let _ = fs::metadata(&meta.path)?;
        Ok(meta.clone())
    }

    pub(super) fn verify_frame_aead_chunk_at(
        &self,
        collection: &str,
        shard: u16,
        group: Option<u32>,
        chunk_uuid: Uuid,
        path: PathBuf,
    ) -> Result<SealedChunkMeta> {
        let sh = Shard {
            dir: self.handle_dir(collection, shard, group),
            collection: collection.to_string(),
            shard,
            data_key: self.opts.data_key,
            policy: self.opts.collection_policy(collection),
            ..Default::default()
        };
        let disk = fs::read(&path)?;
        let decoded = decode_encrypted_file(&sh, chunk_uuid, &path, &disk, false)?;
        if !decoded.sealed {
            return Err(Error::Corrupt(format!(
                "sealed chunk {chunk_uuid} has no seal record"
            )));
        }
        Ok(SealedChunkMeta {
            collection: collection.to_string(),
            shard,
            group_id: group,
            chunk_uuid,
            path,
            end_csn: decoded.max_csn,
        })
    }
}
