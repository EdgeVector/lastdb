use super::*;

impl LastStore {
    /// Force-seal all dirty encrypted tails and return the chunks sealed by
    /// this call.
    // lint:fn-size-ok verbatim move from store.rs; splitting this function is separate work
    pub fn snapshot(&self) -> Result<Snapshot> {
        let before = self.meta.lock().expect("poison").sealed_chunks.len();
        let mut skipped_capped_groups = 0_u64;
        let mut keys: BTreeSet<ShardKey> = self
            .shards
            .lock()
            .expect("poison")
            .handles
            .keys()
            .cloned()
            .collect();
        keys.extend(self.pins.lock().expect("poison").handles.keys().cloned());
        if self.opts.data_key.is_some() {
            // Eviction syncs a tail but does not seal it. Discover cold groups
            // with either encrypted tail format; sealed groups need no admission.
            for collection in self.collections_on_disk()? {
                for (shard, group) in self.handles_on_disk(&collection)? {
                    let key = (collection.clone(), shard, group);
                    if !keys.contains(&key) {
                        let dir = self.handle_dir(&collection, shard, group);
                        if sorted_encrypted_tail_pending(&dir)?
                            || !encrypted_files(&encrypted_tail_dir_for(&dir))?.is_empty()
                        {
                            keys.insert(key);
                        }
                    }
                }
            }
        }
        for (c, s, g) in keys {
            // A group over the cold-load cap cannot be sealed without loading
            // it, and loading it is what the cap forbids. Skip it: its tail
            // stays unsealed on disk and out of this photograph, which is the
            // documented state of a group an operator has to reclaim. Failing
            // the whole photograph instead blocked the 2026-09-22 safe-upgrade
            // cutover on a 2.3 GB legacy `keep_small:meters` group.
            let key = (c.clone(), s, g);
            let pin = self.pin_table_handle(&key);
            let h = if let Some(handle) = pin.clone() {
                handle
            } else {
                match self.shard_handle_at(&c, s, g, WarmAdmission::Point, WarmTouch::unspecified())
                {
                    Ok(h) => h,
                    Err(Error::ColdGroupTooLarge {
                        collection,
                        shard,
                        group,
                        bytes,
                        cap,
                    }) => {
                        skipped_capped_groups = skipped_capped_groups.saturating_add(1);
                        eprintln!(
                            "LASTSTORE_SNAPSHOT_SKIPPED_CAPPED_GROUP collection={collection} shard={shard} \
                             group=0x{group:03x} bytes={bytes} cap={cap}: unsealed tail left out of this \
                             photograph; reclaim or compact the group"
                        );
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            };
            let mut sh = h.lock().expect("poison");
            if sh.data_key.is_some() && sh.uses_sorted_index() {
                let previous_high = sh.plain_high_seq;
                Self::seal_sorted_tail(&mut sh)?;
                if sh.plain_high_seq > previous_high {
                    let path = sh.dir.join(format!("{:010}.seg", sh.plain_high_seq));
                    let mut files = sorted_file::physical_parts(&path)?;
                    files.push((plain_segment_log_uuid(&c, s, g, sh.plain_high_seq), path));
                    for (chunk_uuid, path) in files {
                        let sealed = SealedChunkMeta {
                            collection: c.clone(),
                            shard: s,
                            group_id: g,
                            chunk_uuid,
                            path,
                            end_csn: sh.max_csn,
                        };
                        if !sh.policy.backup_excluded {
                            if let Some(on_seal) = &self.opts.on_seal {
                                let _ = on_seal(&sealed);
                            }
                        }
                        self.meta.lock().expect("poison").sealed_chunks.push(sealed);
                    }
                }
            } else if sh.data_key.is_some() && sh.open_len > 0 {
                Self::seal_open(&self.opts, &self.meta, &mut sh)?;
                Self::open_fresh_encrypted_tail(&mut sh);
            } else {
                Self::sync_open(&mut sh)?;
            }
            drop(sh);
            if pin.is_none() {
                self.refresh_warm_resident_bytes(&key, &h)?;
            }
        }
        let meta = self.meta.lock().expect("poison");
        Ok(Snapshot {
            sealed_chunks: meta.sealed_chunks[before..].to_vec(),
            max_csn: meta.next_csn.saturating_sub(1),
            skipped_capped_groups,
        })
    }

    /// Alias for [`Self::snapshot`] when callers need only the force-seal
    /// behavior.
    pub fn seal_all(&self) -> Result<Snapshot> {
        self.snapshot()
    }

    /// Compact every collection found under `data/`.
    pub fn compact(&self) -> Result<()> {
        for collection in self.collections_on_disk()? {
            self.compact_collection(&collection)?;
        }
        Ok(())
    }

    /// Compact one collection (rewrite live docs, delete old segments).
    pub fn compact_collection(&self, collection: &str) -> Result<()> {
        if self.opts.collection_policy(collection).never_compact {
            return Ok(());
        }
        self.compact_collection_unchecked(collection)
    }

    /// Compact the atom collection after the product layer has durably
    /// recorded retirement provenance for every currently sealed chunk.
    ///
    /// This deliberately names only `atoms`: ordinary callers remain fenced by
    /// `atoms.never_compact`, and no generic bypass exists for other protected
    /// collections. If the product cannot persist its receipt sidecar first it
    /// must not call this method.
    pub fn compact_atoms_with_retirement_provenance(&self) -> Result<()> {
        self.compact_collection_unchecked("atoms")
    }

    pub(super) fn compact_collection_unchecked(&self, collection: &str) -> Result<()> {
        for (shard, group) in self.handles_on_disk(collection)? {
            self.compact_loaded_group(collection, shard, group)?;
        }
        Ok(())
    }

    /// One group's rewrite. Scan admission, same as a collection compact.
    ///
    /// A whole-store sweep uses this so it does not evict the point working
    /// set (a promoted group stays protected). The retire-receipt pass uses
    /// the same rewrite and does not grow a second one.
    pub(super) fn compact_loaded_group(
        &self,
        collection: &str,
        shard: u16,
        group: Option<u32>,
    ) -> Result<()> {
        let handle = self.scan_handle_by_key(&(collection.to_string(), shard, group))?;
        let mut shard_handle = handle.handle().lock().expect("poison");
        Self::compact_shard(&mut shard_handle)
    }

    /// Rewrite groups a retire receipt names, and no others.
    ///
    /// Drops the shared `atom\0` partition first. Skips a group whose sidecar
    /// shows no dead bytes, a `never_compact` collection, and a directory that
    /// is already gone. Does not list a collection, including `tips`, and does
    /// not scan the corpus. Does not target the 3.2 GiB non-CAS gate.
    ///
    /// The whole pass is a no-op when [`RetiredCompactGate::blocks`] or this
    /// store's host-pressure flag is set.
    pub fn compact_retired_groups(
        &self,
        groups: &[RetiredGroupId],
        gate: RetiredCompactGate,
    ) -> Result<()> {
        if gate.blocks() || self.host_pressure_high() {
            return Ok(());
        }
        self.rewrite_retired_groups(groups)
    }

    /// [`Self::compact_retired_groups`] for the receipt at `path`.
    ///
    /// A missing file is a no-op. The pressure and footprint gate runs before
    /// the read, so a blocked pass does not open the receipt or a group.
    pub fn compact_retired_receipt(&self, path: &Path, gate: RetiredCompactGate) -> Result<()> {
        if gate.blocks() || self.host_pressure_high() {
            return Ok(());
        }
        let Some(groups) = read_retire_receipt(path)? else {
            return Ok(());
        };
        self.rewrite_retired_groups(&groups)
    }

    pub(super) fn rewrite_retired_groups(&self, groups: &[RetiredGroupId]) -> Result<()> {
        if self.opts.layout_mode != LayoutMode::HashGroup {
            return Ok(());
        }
        let mut seen: HashSet<(String, u16, u32)> = HashSet::new();
        for group in groups {
            if !seen.insert((group.collection.clone(), group.shard, group.group)) {
                continue;
            }
            self.validate_retired_group(group)?;
            if self.is_shared_atom_partition_group(group.shard, group.group) {
                continue;
            }
            if self.opts.collection_policy(&group.collection).never_compact {
                continue;
            }
            let dir = self.handle_dir(&group.collection, group.shard, Some(group.group));
            if !dir.is_dir() {
                continue;
            }
            let dead = self
                .sidecar_residue_for_group(&group.collection, group.shard, group.group)
                .map(|residue| residue.dead_bytes)
                .unwrap_or(0);
            if dead == 0 {
                continue;
            }
            self.compact_loaded_group(&group.collection, group.shard, Some(group.group))?;
        }
        Ok(())
    }

    pub(super) fn validate_retired_group(&self, group: &RetiredGroupId) -> Result<()> {
        if group.collection.is_empty()
            || group.collection.contains('/')
            || group.collection.contains('\\')
            || group.collection == "."
            || group.collection == ".."
        {
            return Err(Error::Config(
                "retire receipt collection is not one directory name".into(),
            ));
        }
        let shard_count = 1u32 << self.opts.shard_bits;
        let group_count = 1u32 << self.opts.hash_group_bits;
        if u32::from(group.shard) >= shard_count || group.group >= group_count {
            return Err(Error::Config(
                "retire receipt group is outside the configured layout".into(),
            ));
        }
        Ok(())
    }

    /// Physical groups the shared `atom\0` partition occupies.
    ///
    /// Placement only. This does not read keys. Under any other layout the
    /// partition is not a group set, so nothing is dropped here.
    pub(super) fn is_shared_atom_partition_group(&self, shard: u16, group: u32) -> bool {
        if self.opts.layout_mode != LayoutMode::HashGroup
            || self.opts.hash_group_key != HashGroupKey::PartitionPrefix
        {
            return false;
        }
        let partition = "atom\0";
        self.shard_of(partition) == shard && self.partition_groups(partition).contains(&group)
    }

    /// Remove a collection directory, but only after proving it has no live ids.
    ///
    /// This is intentionally stricter than `remove_dir_all`: callers use it as
    /// the final cutover step after a copy/delete drain. If any live id remains,
    /// the directory stays put and the caller gets a structural error.
    pub fn drop_empty_collection(&self, collection: &str) -> Result<bool> {
        let collection_dir = self.root.join("data").join(collection);
        if !collection_dir.exists() {
            return Ok(false);
        }

        let remaining = self.collection_live_key_count(collection)?;
        if remaining != 0 {
            return Err(Error::Config(format!(
                "refusing to drop non-empty collection {collection}: {remaining} live ids remain"
            )));
        }

        self.compact_collection(collection)?;
        let remaining_after_compact = self.collection_live_key_count(collection)?;
        if remaining_after_compact != 0 {
            return Err(Error::Config(format!(
                "refusing to drop non-empty collection {collection} after compact: {remaining_after_compact} live ids remain"
            )));
        }

        self.flush()?;
        {
            let mut shards = self.shards.lock().expect("poison");
            let keys = shards
                .handles
                .keys()
                .filter(|key| key.0 == collection)
                .cloned()
                .collect::<Vec<_>>();
            for key in keys {
                let _ = shards.remove(&key);
            }
        }
        {
            let mut key_index = self.key_index.lock().expect("poison");
            let keys = key_index
                .entries
                .keys()
                .filter(|key| key.0 == collection)
                .cloned()
                .collect::<Vec<_>>();
            for key in keys {
                key_index.forget(&key);
            }
        }

        fs::remove_dir_all(&collection_dir)?;
        sync_dir(&self.root.join("data"))?;
        Ok(true)
    }

    /// Verify all on-disk shards can be authenticated and indexed.
    ///
    /// For encrypted stores this walks every handle loader, which
    /// authenticates encrypted tail frames, sealed chunks, seal records, and
    /// footer records before returning. It does not hydrate document bodies
    /// beyond the frame payloads required to rebuild the local index.
    pub fn verify_integrity(&self) -> Result<()> {
        for collection in self.collections_on_disk()? {
            for (shard, group) in self.handles_on_disk(&collection)? {
                let verify_shard = Shard {
                    dir: self.handle_dir(&collection, shard, group),
                    collection: collection.clone(),
                    shard,
                    data_key: self.opts.data_key,
                    policy: self.opts.collection_policy(&collection),
                    ..Default::default()
                };
                reject_incomplete_sorted_restore(&verify_shard.dir)?;

                for (chunk_uuid, path) in encrypted_files(&encrypted_chunks_dir(&verify_shard))? {
                    let disk = fs::read(&path)?;
                    let decoded =
                        decode_encrypted_file(&verify_shard, chunk_uuid, &path, &disk, false)?;
                    if !decoded.sealed {
                        return Err(Error::Corrupt(format!(
                            "sealed chunk {chunk_uuid} has no seal record"
                        )));
                    }
                }

                for (chunk_uuid, path) in encrypted_files(&encrypted_tail_dir(&verify_shard))? {
                    let disk = fs::read(&path)?;
                    if disk.is_empty() {
                        continue;
                    }
                    let decoded =
                        decode_encrypted_file(&verify_shard, chunk_uuid, &path, &disk, true)?;
                    if decoded.frames.is_empty() {
                        return Err(Error::Corrupt(format!(
                            "encrypted tail {chunk_uuid} has no authenticated frames"
                        )));
                    }
                }

                // Whole-store sweep: scan admission, not the point segment.
                let scan = self.scan_handle_by_key(&(collection.clone(), shard, group))?;
                let loaded = scan.handle().lock().expect("poison");
                for segment in &loaded.sorted_segments {
                    segment.verify(loaded.data_key.as_ref())?;
                }
            }
        }
        Ok(())
    }
}
