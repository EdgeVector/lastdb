use super::*;

impl LastStore {
    /// Highest seal for which every token at or below it is below `file_len`.
    ///
    /// [`Self::flush`]'s `Result<()>` does not carry this. A group-commit or
    /// [`Self::flush_scope`] does not move this past a token on a pin it did
    /// not sync.
    pub fn durable_through(&self) -> DurabilityToken {
        DurabilityToken::new(self.durable_through.load(Ordering::Acquire))
    }

    /// Flagged put (`body = Some`) or delete (`body = None`). One token per call.
    ///
    /// The pin is not a warm-set member. Group-commit still runs on that
    /// `Shard`. If a pin already exists, this writes it and does not call
    /// `load_shard`. The token is assigned in the same shard-lock section that
    /// copies the record into `open_buf`.
    pub fn append_for_resident(
        &self,
        collection: &str,
        storage_key: &str,
        body: Option<&[u8]>,
    ) -> Result<WriteAck> {
        // Same stripe as put, delete, and compare_and_swap. The compare and
        // the replace stay one critical section. A writer that skips the
        // stripe can put an older body back over a newer one.
        let gate = Self::transaction_gate_index(collection, storage_key);
        let _stripe = self.transaction_gates[gate].lock().expect("poison");
        let key = self.point_key(collection, storage_key);
        let handle = self.pin_handle_for_write(key.clone())?;
        let (ack, pin_bytes) = {
            let mut shard = handle.lock().expect("poison");
            let token = DurabilityToken::new(
                self.next_durability_token
                    .fetch_add(1, Ordering::Relaxed)
                    .saturating_add(1),
            );
            let previous = Self::current_body_locked(&mut shard, storage_key)?;
            let previous_len = shard
                .lookup(storage_key)?
                .map(|location| location.record_len());
            match body {
                Some(bytes) => {
                    let line = encode_put(storage_key, bytes)?;
                    let loc =
                        self.append(&mut shard, &line, storage_key, CaptureOp::Put, previous_len)?;
                    shard.insert_index_known(storage_key.to_string(), loc, previous_len);
                    if shard.cache_legacy_bodies() {
                        shard.values.insert(storage_key.to_string(), bytes.to_vec());
                    }
                }
                None => {
                    let line = encode_del(storage_key)?;
                    let loc = self.append(
                        &mut shard,
                        &line,
                        storage_key,
                        CaptureOp::Delete,
                        previous_len,
                    )?;
                    shard.remove_index_known_at(storage_key, previous_len, Some(loc));
                    shard.values.remove(storage_key);
                }
            }
            self.sync_point_append_threshold(&mut shard)?;
            let covered_by_file_len = shard.dirty_ops == 0 && shard.file_len >= shard.open_len;
            let pin_bytes = estimate_shard_residency_locked(&shard).total;
            (
                WriteAck {
                    token,
                    covered_by_file_len,
                    previous,
                },
                pin_bytes,
            )
        };
        self.charge_pin_bytes(&key, pin_bytes);
        drop(handle);
        self.note_write(storage_key, key.clone());
        if ack.covered_by_file_len {
            self.reap_clean_unheld_pins_in(std::slice::from_ref(&key));
        }
        Ok(ack)
    }

    pub(super) fn pin_handle_for_write(&self, key: ShardKey) -> Result<ShardHandle> {
        {
            let pins = self.pins.lock().expect("poison");
            if let Some(handle) = pins.handles.get(&key).cloned() {
                self.key_index.lock().expect("poison").mark_pinned(&key);
                return Ok(handle);
            }
        }
        // Hold the cold-load gate through pin insert. `open_group_unpublished`
        // releases that gate before this method used to insert, so a concurrent
        // `shard_handle_at` could `load_shard` and publish a second Shard.
        let gate = Self::cold_load_gate_index(&key);
        let _cold_load_guard = self.cold_load_gates[gate].lock().expect("poison");
        {
            let pins = self.pins.lock().expect("poison");
            if let Some(handle) = pins.handles.get(&key).cloned() {
                self.key_index.lock().expect("poison").mark_pinned(&key);
                return Ok(handle);
            }
        }
        let handle = if let Some(existing) = self.existing_unpublished_authority(&key) {
            existing
        } else {
            self.load_unpublished_shard(&key)?
        };
        // Lock order: pins then key_index. `reap_clean_unheld_pins` uses the
        // same order so a retain cannot insert between pin insert and this
        // record, and a reap cannot drop a newer pin's record.
        let pinned = {
            let mut pins = self.pins.lock().expect("poison");
            let pinned = pins
                .handles
                .entry(key.clone())
                .or_insert_with(|| Arc::clone(&handle))
                .clone();
            self.key_index.lock().expect("poison").mark_pinned(&key);
            pinned
        };
        self.charge_pin_from_handle(&key, &pinned);
        Ok(pinned)
    }

    pub(super) fn pin_table_handle(&self, key: &ShardKey) -> Option<ShardHandle> {
        self.pins.lock().expect("poison").handles.get(key).cloned()
    }

    /// Charge `bytes` for `key` when that key is still in the pin table.
    ///
    /// Lock order: `pins` only. Callers must not hold a shard lock.
    pub(super) fn charge_pin_bytes(&self, key: &ShardKey, bytes: u64) {
        let mut pins = self.pins.lock().expect("poison");
        if !pins.handles.contains_key(key) {
            return;
        }
        let old = pins.charged_bytes.insert(key.clone(), bytes).unwrap_or(0);
        apply_loader_pin_byte_delta(&self.loader_pin_bytes, old, bytes);
    }

    pub(super) fn charge_pin_from_handle(&self, key: &ShardKey, handle: &ShardHandle) {
        let bytes = estimate_shard_residency(handle).total;
        self.charge_pin_bytes(key, bytes);
    }

    pub(super) fn uncharge_pin_locked(pins: &mut PinTable, total: &AtomicU64, key: &ShardKey) {
        if let Some(old) = pins.charged_bytes.remove(key) {
            total.fetch_sub(old, Ordering::Relaxed);
        }
    }

    pub(super) fn assigned_through(&self) -> u64 {
        self.next_durability_token.load(Ordering::Relaxed)
    }

    pub(super) fn publish_durable_through(&self, seal: u64) {
        self.durable_through.fetch_max(seal, Ordering::Release);
    }

    /// Remove a pin only when it is clean and no caller holds a clone.
    ///
    /// This does not use the warm-set `strong_count > 2` plus `touch` skip.
    /// The table itself holds one `Arc`. A second clone is an in-flight holder.
    ///
    /// A pin never enters the warm set. Eviction writes the plain id sidecar,
    /// and snapshot seals an encrypted tail, only for a handle they can still
    /// see. Both have to happen here, before the pin is removed. This does
    /// not publish the group and does not insert the key cache.
    pub(super) fn reap_clean_unheld_pins(&self) {
        let keys: Vec<ShardKey> = self
            .pins
            .lock()
            .expect("poison")
            .handles
            .keys()
            .cloned()
            .collect();
        self.reap_clean_unheld_pins_in(&keys);
    }

    /// Reap only pins named by a foreground write's durability scope.
    pub(super) fn reap_clean_unheld_pins_in(&self, keys: &[ShardKey]) {
        let mut reap = Vec::new();
        for key in keys {
            let Some(handle) = self.pin_table_handle(key) else {
                continue;
            };
            if Arc::strong_count(&handle) != 2 {
                continue;
            }
            let Ok(mut shard) = handle.try_lock() else {
                continue;
            };
            if shard.dirty_ops != 0 || shard.dirty_bytes != 0 {
                continue;
            }
            if !self.durable_artifacts_for_pin(&mut shard) {
                continue;
            }
            if shard.dirty_ops != 0 || shard.dirty_bytes != 0 {
                continue;
            }
            drop(shard);
            if Arc::strong_count(&handle) != 2 {
                continue;
            }
            reap.push(key.clone());
        }
        if reap.is_empty() {
            return;
        }
        let mut pins = self.pins.lock().expect("poison");
        let mut key_index = self.key_index.lock().expect("poison");
        for key in reap {
            let Some(handle) = pins.handles.get(&key).cloned() else {
                continue;
            };
            if Arc::strong_count(&handle) != 2 {
                continue;
            }
            let Ok(shard) = handle.try_lock() else {
                continue;
            };
            if shard.dirty_ops != 0 || shard.dirty_bytes != 0 {
                continue;
            }
            drop(shard);
            pins.handles.remove(&key);
            key_index.forget_pin(&key);
            Self::uncharge_pin_locked(&mut pins, &self.loader_pin_bytes, &key);
        }
    }

    /// Seal an encrypted tail and write a plain id sidecar while `sh` is the
    /// pin's only holder.
    ///
    /// Returns false when that artifact is missing. The caller keeps the pin
    /// so a later flush or snapshot can retry. A false return is not a warm
    /// publish and does not insert the key cache.
    pub(super) fn durable_artifacts_for_pin(&self, sh: &mut Shard) -> bool {
        if sh.data_key.is_some() && sh.uses_sorted_index() {
            if (sorted_unsealed_bytes(sh) > 0 || sh.open_len > 0)
                && Self::seal_sorted_tail(sh).is_err()
            {
                return false;
            }
        } else if sh.data_key.is_some() && sh.open_len > 0 {
            if Self::seal_open(&self.opts, &self.meta, sh).is_err() {
                return false;
            }
            Self::open_fresh_encrypted_tail(sh);
        }
        if !self.write_plain_id_sidecar_locked(sh) {
            return false;
        }
        // The append descriptor keeps a filesystem extent past the written
        // end. Close it once the bytes are durable.
        sh.open_file = None;
        true
    }

    /// Write the plain id sidecar from the pin handle.
    ///
    /// Frame-AEAD homes and sorted groups have no sidecar. An empty group has
    /// nothing to prove. A stamp mismatch keeps the pin: the sidecar would
    /// describe a different file than the one on disk.
    pub(super) fn write_plain_id_sidecar_locked(&self, sh: &mut Shard) -> bool {
        if !self.sidecar_enabled() || sh.uses_sorted_index() || shard_group_from_dir(sh).is_none() {
            return true;
        }
        if sh.segments.is_empty() {
            return true;
        }
        // A one-block group does not get a second file. The no-load drop is
        // for a group too large to open. A fresh boot flushes many one-block
        // groups, and a sidecar file on each of them exceeds the home gauge.
        if sh.segments.len() == 1 && sh.file_len <= 4096 {
            return true;
        }
        if (sh.dirty_ops != 0 || sh.dirty_bytes != 0) && Self::sync_open(sh).is_err() {
            return false;
        }
        let Ok(stamps) = keysidecar::segment_stamps(&sh.dir) else {
            return false;
        };
        if sh.sidecar_stamps.as_deref() == Some(stamps.as_slice()) {
            return true;
        }
        if !sh.matches_segment_stamps(&stamps) {
            return false;
        }
        let Ok(ids) = sh.live_keys() else {
            return false;
        };
        if keysidecar::write(&sh.dir, &stamps, sh.residue, &ids).is_err() {
            return false;
        }
        sh.sidecar_stamps = Some(stamps);
        true
    }

    /// Hash groups currently in the loader pin table.
    ///
    /// This is a measurement, not a cap. A write pin is not a warm-set member.
    pub fn loader_groups_open_now(&self) -> u64 {
        u64::try_from(self.pins.lock().expect("poison").handles.len()).unwrap_or(u64::MAX)
    }

    /// Estimated bytes currently held by the loader pin table.
    ///
    /// This is a measurement, not a cap. The pin table updates the counter
    /// when a pin opens, a write refreshes that pin, or a reap drops it.
    pub fn loader_pin_bytes(&self) -> u64 {
        self.loader_pin_bytes.load(Ordering::Relaxed)
    }
}
