use super::*;

impl LastStore {
    /// Write an id sidecar for every hash group resident right now.
    ///
    /// Eviction writes a sidecar for the groups it evicts, which covers the
    /// *cold* part of the working set. A group that is loaded and never evicted
    /// during a process lifetime never passes through that path, so without
    /// this the next process's first keys walk pays a full segment read for
    /// precisely the hottest groups — the ones the feature was meant to help
    /// most.
    ///
    /// Raising `hash_group_warm_bytes` widens that gap rather than closing it:
    /// the more of a collection stays resident, the fewer sidecars eviction
    /// ever writes. This is the other half of that trade.
    ///
    /// Called once per process from `Drop`, deliberately **not** from
    /// [`LastStore::flush`]: `flush` is on the ordinary write path, and
    /// rewriting every resident group's id list there would add real write
    /// amplification to it. Repeat calls are cheap — a group whose sidecar
    /// already matches its current segment stamps is skipped.
    ///
    /// Advisory like every other sidecar write: a failure costs the next cold
    /// walk one segment read, so nothing here is propagated.
    pub fn persist_key_sidecars(&self) {
        if !self.sidecar_enabled() {
            return;
        }
        // Only hash-group handles. A `group: None` handle is the legacy
        // segment-log index, whose directory is the shard root rather than a
        // group home; eviction skips those for the same reason.
        //
        // A pin is the write authority when both maps hold the key. Take each
        // lock separately: `existing_unpublished_authority` takes pins, then
        // the warm map, and this method must not invert that order.
        let mut chosen: BTreeMap<ShardKey, ShardHandle> = BTreeMap::new();
        {
            let warm = self.shards.lock().expect("poison");
            for (key, handle) in &warm.handles {
                if key.2.is_some() {
                    chosen.insert(key.clone(), Arc::clone(handle));
                }
            }
        }
        {
            let pins = self.pins.lock().expect("poison");
            for (key, handle) in &pins.handles {
                if key.2.is_some() {
                    chosen.insert(key.clone(), Arc::clone(handle));
                }
            }
        }

        for handle in chosen.into_values() {
            let mut sh = handle.lock().expect("poison");
            // Sync, stamp, and write all under the one lock. The eviction
            // path has to drop the lock before writing — it is removing the
            // handle — and relies on the reader replaying any suffix a writer
            // slips in afterwards. Here the handle stays, so holding it
            // across the write is stricter: no writer can append between the
            // stamp and the file it describes.
            self.repair_plain_id_sidecar_locked(&mut sh);
        }
    }

    /// Write the plain id sidecar when it does not describe this handle.
    ///
    /// A prefix read drops the group without eviction. Eviction was the writer
    /// that repaired a corrupt sidecar and refreshed one after an append. A
    /// sidecar whose stamps already match is left untouched, so a repeat read
    /// does not rewrite it. Advisory, like every other sidecar write.
    pub(super) fn repair_plain_id_sidecar_locked(&self, sh: &mut Shard) {
        if !self.sidecar_enabled() || sh.uses_sorted_index() {
            return;
        }
        if Self::sync_open(sh).is_err() {
            return;
        }
        let Ok(stamps) = keysidecar::segment_stamps(&sh.dir) else {
            return;
        };
        // A later image already owns these files. Do not advertise this
        // handle's ids as describing them.
        if !sh.matches_segment_stamps(&stamps) {
            return;
        }
        if sh.sidecar_stamps.as_deref() == Some(stamps.as_slice()) {
            return;
        }
        let Ok(ids) = sh.live_keys() else {
            return;
        };
        if keysidecar::write(&sh.dir, &stamps, sh.residue, &ids).is_ok() {
            sh.sidecar_stamps = Some(stamps);
        }
    }

    /// Sidecar residue for one group directory.
    ///
    /// Calls the key sidecar's `read_residue` on that directory and nothing else.
    /// Does not call [`Self::collection_residue`], [`Self::group_residue`], or
    /// `handles_on_disk`. `None` means the sidecar cannot be trusted; it is
    /// not a corpus scan and it is not a claim that dead bytes are zero.
    pub fn sidecar_residue_for_group(
        &self,
        collection: &str,
        shard: u16,
        group: u32,
    ) -> Option<GroupResidue> {
        let dir = self.handle_dir(collection, shard, Some(group));
        keysidecar::read_residue(&dir).map(|recorded| recorded.residue)
    }

    /// Record-byte residue of `collection`: live versus dead record bytes,
    /// summed over every hash group, **without loading a cold group**.
    ///
    /// A resident group or active write pin answers from its in-memory
    /// counters. A cold group answers from its newest sorted seal or the
    /// residue its id sidecar
    /// recorded when it was last persisted; bytes appended since count as
    /// `unknown_bytes`, never as dead. A cold group with no usable sidecar
    /// contributes its on-disk size to `unknown_bytes` and is counted in
    /// `groups_unknown`, so a caller can see how much of the plane it could
    /// not measure and decide whether the answer is worth acting on.
    ///
    /// Cost: one directory listing per collection plus, per group, either a
    /// brief lock on a resident handle, a sorted footer read, or a sidecar
    /// header read. No record body is read and no key index is rebuilt. This fits
    /// for an hourly compaction probe on a thousand-group plane; the
    /// alternative — parsing every group — is a full plane scan.
    pub fn collection_residue(&self, collection: &str) -> Result<CollectionResidue> {
        let mut out = CollectionResidue::default();
        let mut resident: HashMap<ShardKey, ShardHandle> = {
            let warm = self.shards.lock().expect("poison");
            warm.handles
                .iter()
                .filter(|(key, _)| key.0 == collection)
                .map(|(key, handle)| (key.clone(), Arc::clone(handle)))
                .collect()
        };
        // The write pin owns the current counters before its buffer and
        // sidecar reach disk. It is not a warm-set member. Choose it over
        // an older published handle and keep the map locks separate, as
        // persist_key_sidecars does for the same write authority.
        {
            let pins = self.pins.lock().expect("poison");
            for (key, handle) in &pins.handles {
                if key.0 == collection {
                    resident.insert(key.clone(), Arc::clone(handle));
                }
            }
        }
        let mut seen: HashSet<ShardKey> = HashSet::new();
        let sidecar = self.sidecar_enabled();
        for (shard, group) in self.handles_on_disk(collection)? {
            let key = (collection.to_string(), shard, group);
            seen.insert(key.clone());
            out.groups += 1;
            if let Some(handle) = resident.get(&key) {
                let sh = handle.lock().expect("poison");
                out.groups_resident += 1;
                out.live_bytes = out.live_bytes.saturating_add(sh.residue.live_bytes);
                out.dead_bytes = out.dead_bytes.saturating_add(sh.residue.dead_bytes);
                continue;
            }
            let dir = self.handle_dir(collection, shard, group);
            if self.opts.data_key.is_none() {
                if let Some((recorded, appended_bytes)) = read_plain_sorted_residue(&dir, shard)? {
                    out.groups_sealed += 1;
                    out.live_bytes = out.live_bytes.saturating_add(recorded.live_bytes);
                    out.dead_bytes = out.dead_bytes.saturating_add(recorded.dead_bytes);
                    out.unknown_bytes = out.unknown_bytes.saturating_add(appended_bytes);
                    continue;
                }
            }
            if sidecar && group.is_some() {
                if let Some(recorded) = keysidecar::read_residue(&dir) {
                    out.groups_sidecar += 1;
                    out.live_bytes = out.live_bytes.saturating_add(recorded.residue.live_bytes);
                    out.dead_bytes = out.dead_bytes.saturating_add(recorded.residue.dead_bytes);
                    out.unknown_bytes = out.unknown_bytes.saturating_add(recorded.appended_bytes);
                    continue;
                }
            }
            out.groups_unknown += 1;
            out.unknown_bytes = out
                .unknown_bytes
                .saturating_add(estimate_group_on_disk_bytes(&dir));
        }
        // A group that is resident but whose directory the listing did not
        // show yet (first put, dir not fsynced) still holds real bytes.
        for (key, handle) in resident {
            if seen.contains(&key) {
                continue;
            }
            let sh = handle.lock().expect("poison");
            out.groups += 1;
            out.groups_resident += 1;
            out.live_bytes = out.live_bytes.saturating_add(sh.residue.live_bytes);
            out.dead_bytes = out.dead_bytes.saturating_add(sh.residue.dead_bytes);
        }
        Ok(out)
    }

    /// Residue of one resident or loadable group. Test and diagnostic hook:
    /// loads the group if it is cold, so it is exact but not cheap.
    pub fn group_residue(&self, collection: &str, id: &str) -> Result<GroupResidue> {
        let h = self.point_handle(collection, id)?;
        let sh = h.lock().expect("poison");
        Ok(sh.residue)
    }
}
