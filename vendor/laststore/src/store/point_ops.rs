// lint:file-size-ok verbatim move from store.rs; splitting this file further is separate work
use super::*;

impl LastStore {
    /// Insert or replace a document. Not durable until [`Self::flush`] /
    /// group-commit threshold / drop.
    pub fn put(&self, collection: &str, id: &str, body: &[u8]) -> Result<()> {
        let gate = Self::transaction_gate_index(collection, id);
        let _guard = self.transaction_gates[gate].lock().expect("poison");
        self.put_unlocked(collection, id, body)
    }

    /// Point put for a caller that already owns the transaction stripe.
    pub(super) fn put_unlocked(&self, collection: &str, id: &str, body: &[u8]) -> Result<()> {
        let line = encode_put(id, body)?;
        let key = self.point_key(collection, id);
        let h = self.shard_handle_for_id(collection, id)?;
        {
            let mut sh = h.lock().expect("poison");
            let previous_len = sh.lookup(id)?.map(|location| location.record_len());
            let loc = self.append(&mut sh, &line, id, CaptureOp::Put, previous_len)?;
            sh.insert_index_known(id.to_string(), loc, previous_len);
            if sh.cache_legacy_bodies() {
                sh.values.insert(id.to_string(), body.to_vec());
            }
            self.sync_point_append_threshold(&mut sh)?;
        }
        self.refresh_warm_resident_bytes(&key, &h)?;
        self.note_write(id, key);
        Ok(())
    }

    /// Apply a photograph batch without a durability barrier, grouped by its
    /// destination hash group.
    ///
    /// Photograph namespaces contain one value per key. Grouping avoids one
    /// shard-cache lookup, mutex acquisition, and resident-cost refresh per
    /// entry. The restore owner must call [`Self::flush_restore_parallel`]
    /// before it exposes the store.
    pub fn restore_put_many_deferred(
        &self,
        items: Vec<(String, String, Vec<u8>)>,
        max_workers: usize,
    ) -> Result<()> {
        let mut by_group: HashMap<ShardKey, Vec<(String, Vec<u8>)>> = HashMap::new();
        for (collection, id, body) in items {
            by_group
                .entry(self.point_key(&collection, &id))
                .or_default()
                .push((id, body));
        }

        // Preserve locality as LastStore creates and opens the destination
        // files. A BTreeMap here performs a tree lookup for every photograph
        // entry. Hash first, then sort only the much smaller group set.
        let mut groups: Vec<_> = by_group.into_iter().collect();
        groups.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        if groups.is_empty() {
            return Ok(());
        }

        let workers = max_workers.max(1).min(groups.len());
        let chunk_size = groups.len().div_ceil(workers);
        let errors = std::sync::Mutex::new(Vec::new());
        // A worker starts at admit code 0. Read the caller before `thread::scope`
        // and install that code on the worker. Otherwise an interactive restore
        // stays evictable, and a lastgit restore takes the unspecified
        // over-budget publish instead of the background lease.
        let admit_code = WARM_ADMIT_CODE.with(Cell::get);
        std::thread::scope(|scope| {
            for chunk in groups.chunks_mut(chunk_size) {
                let errors = &errors;
                scope.spawn(move || {
                    with_admit_code(admit_code, || {
                        for (key, entries) in chunk {
                            if let Err(error) =
                                self.restore_group_deferred(key, std::mem::take(entries))
                            {
                                errors.lock().expect("poison").push(error);
                                break;
                            }
                        }
                    });
                });
            }
        });
        if let Some(error) = errors.into_inner().expect("poison").into_iter().next() {
            return Err(error);
        }
        Ok(())
    }

    pub(super) fn restore_group_deferred(
        &self,
        key: &ShardKey,
        entries: Vec<(String, Vec<u8>)>,
    ) -> Result<()> {
        let atom = entries.iter().any(|(id, _)| is_atom_plane_id(id));
        let handle = self.shard_handle_at(
            &key.0,
            key.1,
            key.2,
            WarmAdmission::Point,
            WarmTouch {
                class: current_admit_class(),
                atom,
            },
        )?;
        {
            let mut shard = handle.lock().expect("poison");
            for (id, body) in entries {
                let line = encode_put(&id, &body)?;
                let previous_len = shard.lookup(&id)?.map(|location| location.record_len());
                let location = self.append(&mut shard, &line, &id, CaptureOp::Put, previous_len)?;
                shard.insert_index_known(id.clone(), location, previous_len);
                if shard.cache_legacy_bodies() {
                    shard.values.insert(id, body);
                }
                self.sync_point_append_threshold(&mut shard)?;
            }
        }
        self.refresh_warm_resident_bytes(key, &handle)
    }

    /// Get a document by id.
    pub fn get(&self, collection: &str, id: &str) -> Result<Option<Vec<u8>>> {
        let key = self.point_key(collection, id);
        let h = self.shard_handle_for_id(collection, id)?;
        let out = {
            let mut sh = h.lock().expect("poison");
            Self::current_body_locked(&mut sh, id)?
        };
        self.refresh_warm_resident_bytes(&key, &h)?;
        Ok(out)
    }

    /// Copy one storage record from its hash group without publishing the group.
    ///
    /// `storage_key` is the Last Store document id. If that group's pin exists,
    /// this reads the pin and does not call `load_shard`. Otherwise it opens
    /// one unpublished group, copies the record, and drops the unpinned
    /// `Shard`. It does not call `publish_warm_handle`, `touch`, or
    /// `retain_group_ids`.
    pub fn load_point(&self, collection: &str, storage_key: &str) -> Result<Option<LoadedPoint>> {
        let key = self.point_key(collection, storage_key);
        let handle = self.open_group_unpublished(key)?;
        Self::copy_loaded_point(&handle, storage_key)
    }

    /// Copy a point only when its live authority or disk group exists. Plain
    /// metadata reads use this so an absent record does not create a group.
    pub fn load_existing_point(
        &self,
        collection: &str,
        storage_key: &str,
    ) -> Result<Option<LoadedPoint>> {
        let key = self.point_key(collection, storage_key);
        let Some(handle) = self.open_existing_group_unpublished(key)? else {
            return Ok(None);
        };
        Self::copy_loaded_point(&handle, storage_key)
    }

    pub(super) fn copy_loaded_point(
        handle: &ShardHandle,
        storage_key: &str,
    ) -> Result<Option<LoadedPoint>> {
        let body = {
            let mut shard = handle.lock().expect("poison");
            Self::current_body_locked(&mut shard, storage_key)?
        };
        Ok(body.map(|body| LoadedPoint {
            storage_key: storage_key.to_string(),
            body: Some(body),
        }))
    }

    /// Copy many storage records, opening each unpublished group once.
    ///
    /// Slot `i` is the body of `storage_keys[i]`, or `None` when that id is
    /// absent. Keys that share a group share one unpublished load.
    /// The call does not publish groups into the warm set.
    pub fn load_points(
        &self,
        collection: &str,
        storage_keys: &[String],
    ) -> Result<Vec<Option<LoadedPoint>>> {
        self.load_points_inner(collection, storage_keys, false)
    }

    /// Copy points from existing live or disk groups once per group. Missing
    /// groups do not create a directory or count as a cold load.
    pub fn load_existing_points(
        &self,
        collection: &str,
        storage_keys: &[String],
    ) -> Result<Vec<Option<LoadedPoint>>> {
        self.load_points_inner(collection, storage_keys, true)
    }

    pub(super) fn load_points_inner(
        &self,
        collection: &str,
        storage_keys: &[String],
        skip_absent_groups: bool,
    ) -> Result<Vec<Option<LoadedPoint>>> {
        if storage_keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut by_group: BTreeMap<ShardKey, Vec<(usize, &String)>> = BTreeMap::new();
        for (slot, id) in storage_keys.iter().enumerate() {
            by_group
                .entry(self.point_key(collection, id))
                .or_default()
                .push((slot, id));
        }
        let mut out = vec![None; storage_keys.len()];
        for (key, entries) in by_group {
            let handle = if skip_absent_groups {
                let Some(handle) = self.open_existing_group_unpublished(key)? else {
                    continue;
                };
                handle
            } else {
                self.open_group_unpublished(key)?
            };
            {
                let mut shard = handle.lock().expect("poison");
                for (slot, id) in entries {
                    out[slot] =
                        Self::current_body_locked(&mut shard, id)?.map(|body| LoadedPoint {
                            storage_key: (*id).clone(),
                            body: Some(body),
                        });
                }
            }
        }
        Ok(out)
    }

    /// Whether each of `storage_keys` exists, opening each unpublished group
    /// once and never copying a body.
    ///
    /// Slot `i` answers `storage_keys[i]`.
    pub fn exists_points(&self, collection: &str, storage_keys: &[String]) -> Result<Vec<bool>> {
        if storage_keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut by_group: BTreeMap<ShardKey, Vec<(usize, &String)>> = BTreeMap::new();
        for (slot, id) in storage_keys.iter().enumerate() {
            by_group
                .entry(self.point_key(collection, id))
                .or_default()
                .push((slot, id));
        }
        let mut out = vec![false; storage_keys.len()];
        for (key, entries) in by_group {
            let handle = self.open_group_unpublished(key)?;
            {
                let shard = handle.lock().expect("poison");
                for (slot, id) in entries {
                    out[slot] = shard.lookup(id.as_str())?.is_some();
                }
            }
        }
        Ok(out)
    }

    /// Owning group of `collection`/`id`.
    ///
    /// Placement is a pure function of the id and the durable layout options.
    /// A batch flush names these keys so it syncs only the groups it wrote.
    pub fn shard_key_of(&self, collection: &str, id: &str) -> ShardKey {
        self.point_key(collection, id)
    }

    /// Copy disk and pin records under one storage prefix without publishing
    /// groups.
    ///
    /// `prefix` is the storage prefix bytes (`mk:{M}:{esc(hash)}\0` for one
    /// hash). This does not apply tombstones and does not mark a resident view
    /// complete. A failed range gate returns [`Error::UnanchoredRead`] and does
    /// not call `handles_for_partition`. A passed gate opens
    /// `groups_per_partition_read()` groups one at a time and drops each
    /// unpinned `Shard` before it opens the next.
    pub fn load_hash(&self, collection: &str, prefix: &[u8]) -> Result<Vec<LoadedTip>> {
        let Ok(prefix) = std::str::from_utf8(prefix) else {
            return self.reject_unanchored_read("load_hash", prefix.len());
        };
        if !self.hash_range_gate_passes(prefix) {
            return self.reject_unanchored_read("load_hash", prefix.len());
        }
        let partition = partition_of(prefix);
        let shard = self.shard_of(partition);
        let groups = self.partition_groups(partition);
        let mut out = Vec::new();
        for group in groups {
            let key = (collection.to_string(), shard, Some(group));
            if !self.unpublished_group_present(&key) {
                continue;
            }
            let handle = self.open_group_unpublished(key)?;
            {
                let mut shard = handle.lock().expect("poison");
                let mut ids = Vec::new();
                shard.visit_keys(prefix, None, |id, _| {
                    if !id.starts_with(prefix) {
                        return false;
                    }
                    ids.push(id.to_string());
                    true
                })?;
                for id in ids {
                    let body = Self::current_body_locked(&mut shard, &id)?;
                    out.push(LoadedTip {
                        storage_key: id,
                        body,
                    });
                }
            }
        }
        Ok(out)
    }

    /// Get many documents by id, resolving each owning shard **once per
    /// shard** instead of once per id.
    ///
    /// Slot `i` of the result is the body of `ids[i]`, or `None` when absent.
    ///
    /// Prefer this over a [`Self::get`] loop for any known key set. `get`
    /// resolves the owning shard handle *and* re-estimates warm-set residency
    /// on every call, so an N-key batch driven through it costs `O(N)` shard
    /// resolutions and `O(N)` full residency estimates. Under
    /// [`LayoutMode::HashGroup`] the keys of one batch hash to scattered
    /// groups, so with a bounded warm set each key re-parses a whole group
    /// segment and rebuilds its index under the shard mutex — the same
    /// pathology that made walks quadratic before they grouped by shard.
    pub fn get_many(&self, collection: &str, ids: &[String]) -> Result<Vec<Option<Vec<u8>>>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        self.load_bodies_grouped(collection, ids)
    }

    /// Delete a document if present.
    pub fn delete(&self, collection: &str, id: &str) -> Result<()> {
        let gate = Self::transaction_gate_index(collection, id);
        let _guard = self.transaction_gates[gate].lock().expect("poison");
        self.delete_unlocked(collection, id)
    }

    /// Point delete for a caller that already owns the transaction stripe.
    pub(super) fn delete_unlocked(&self, collection: &str, id: &str) -> Result<()> {
        let key = self.point_key(collection, id);
        let h = self.shard_handle_for_id(collection, id)?;
        {
            let mut sh = h.lock().expect("poison");
            let previous_len = sh.lookup(id)?.map(|location| location.record_len());
            if previous_len.is_none() {
                return Ok(());
            }
            let line = encode_del(id)?;
            let loc = self.append(&mut sh, &line, id, CaptureOp::Delete, previous_len)?;
            sh.remove_index_known_at(id, previous_len, Some(loc));
            sh.values.remove(id);
            self.sync_point_append_threshold(&mut sh)?;
        }
        self.refresh_warm_resident_bytes(&key, &h)?;
        self.note_write(id, key);
        Ok(())
    }

    /// Replace or delete one document only when its current body is `expected`.
    ///
    /// `expected == None` requires the id to be absent; `new == None` deletes.
    /// Returns `Ok(true)` when the write applied and `Ok(false)` when the
    /// current body differed (nothing is written).
    ///
    /// The compare and the write run under the id's transaction stripe. Every
    /// point [`Self::put`], [`Self::delete`], [`Self::append_for_resident`], and
    /// [`Self::transaction`] takes the same stripe, so no writer can land
    /// between the compare and the write.
    /// A caller that did the compare with a separate [`Self::get`] could have a
    /// concurrent write silently overwritten.
    pub fn compare_and_swap(
        &self,
        collection: &str,
        id: &str,
        expected: Option<&[u8]>,
        new: Option<&[u8]>,
    ) -> Result<bool> {
        let gate = Self::transaction_gate_index(collection, id);
        let _guard = self.transaction_gates[gate].lock().expect("poison");
        let key = self.point_key(collection, id);
        let h = self.shard_handle_for_id(collection, id)?;
        let current = {
            let mut sh = h.lock().expect("poison");
            Self::current_body_locked(&mut sh, id)?
        };
        if current.as_deref() != expected {
            self.refresh_warm_resident_bytes(&key, &h)?;
            return Ok(false);
        }
        match new {
            Some(body) => self.put_unlocked(collection, id, body)?,
            None => self.delete_unlocked(collection, id)?,
        }
        Ok(true)
    }

    /// Whether a document id exists (no body load).
    pub fn exists(&self, collection: &str, id: &str) -> Result<bool> {
        let h = self.point_handle(collection, id)?;
        let sh = h.lock().expect("poison");
        Ok(sh.lookup(id)?.is_some())
    }

    /// Whether each of `ids` exists, resolving each owning group **once per
    /// group** and never reading a body.
    ///
    /// Slot `i` of the result answers `ids[i]`.
    ///
    /// Two costs separate this from an [`Self::exists`] loop, and both bite
    /// hardest under [`LayoutMode::HashGroup`], where the ids of one batch hash
    /// to scattered groups:
    ///
    /// 1. `exists` resolves a shard handle per id, so an N-key probe pays `O(N)`
    ///    resolutions and, with a bounded warm set, up to `O(N)` cold group
    ///    loads — one whole segment parsed per id.
    /// 2. `exists` always demands the *live* handle. An existence answer does
    ///    not need a body, so a group that is **not** resident can answer from
    ///    the same cheaper id tiers a keys-only walk already trusts
    ///    ([`Self::group_key_source`]: the in-memory key-index cache, then the
    ///    on-disk sidecar). A resident handle still wins, and loading a handle
    ///    drops any snapshot of that group, so this cannot answer from a
    ///    snapshot a writer has moved past.
    pub fn exists_many(&self, collection: &str, ids: &[String]) -> Result<Vec<bool>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        // Group by owning shard/group, remembering each id's slot so the caller
        // gets answers in the order it asked for.
        let mut by_group: BTreeMap<ShardKey, Vec<(usize, &String)>> = BTreeMap::new();
        for (slot, id) in ids.iter().enumerate() {
            by_group
                .entry(self.point_key(collection, id))
                .or_default()
                .push((slot, id));
        }

        let mut out = vec![false; ids.len()];
        for (key, entries) in by_group {
            match self.group_key_source(&key.0, key.1, key.2)? {
                GroupKeySource::Live(h) => {
                    let sh = h.handle().lock().expect("poison");
                    for (slot, id) in entries {
                        out[slot] = sh.lookup(id.as_str())?.is_some();
                    }
                }
                GroupKeySource::Cached(keys) => {
                    for (slot, id) in entries {
                        out[slot] = keys.contains(id.as_str());
                    }
                }
            }
        }
        Ok(out)
    }
}
