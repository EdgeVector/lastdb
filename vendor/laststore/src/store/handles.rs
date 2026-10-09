// lint:file-size-ok verbatim move from store.rs; splitting this file further is separate work
use super::*;

impl LastStore {
    pub(super) fn shard_name(&self, shard: u16) -> String {
        let bits = self.opts.shard_bits;
        if bits == 0 {
            return "0".to_string();
        }
        let w = usize::from(bits.div_ceil(4));
        format!("{shard:0w$x}")
    }

    pub(super) fn group_name(&self, group: u32) -> String {
        let w = usize::from(self.opts.hash_group_bits.div_ceil(4));
        format!("{group:0w$x}")
    }

    pub(super) fn hash_group_dir(&self, collection: &str, shard: u16, group: u32) -> PathBuf {
        self.shard_dir(collection, shard)
            .join("g")
            .join(self.group_name(group))
    }

    pub(super) fn handle_dir(&self, collection: &str, shard: u16, group: Option<u32>) -> PathBuf {
        match group {
            Some(group) => self.hash_group_dir(collection, shard, group),
            None => self.shard_dir(collection, shard),
        }
    }

    pub(super) fn shards_on_disk(&self, collection: &str) -> Result<Vec<u16>> {
        let dir = self.root.join("data").join(collection);
        let mut out = Vec::new();
        if !dir.exists() {
            return Ok(out);
        }
        for e in fs::read_dir(dir)? {
            let e = e?;
            if let Ok(n) = u16::from_str_radix(&e.file_name().to_string_lossy(), 16) {
                out.push(n);
            }
        }
        out.sort_unstable();
        Ok(out)
    }

    pub(super) fn point_handle(&self, collection: &str, id: &str) -> Result<ShardHandle> {
        self.shard_handle_for_id(collection, id)
    }

    /// Point admit that knows the id, so class and the atom plane can be recorded.
    pub(super) fn shard_handle_for_id(&self, collection: &str, id: &str) -> Result<ShardHandle> {
        let key = self.point_key(collection, id);
        let touch = WarmTouch {
            class: current_admit_class(),
            atom: is_atom_plane_id(id),
        };
        self.shard_handle_at(&key.0, key.1, key.2, WarmAdmission::Point, touch)
    }

    pub(super) fn point_key(&self, collection: &str, id: &str) -> ShardKey {
        let shard = self.shard_of(id);
        let group = match self.opts.layout_mode {
            LayoutMode::SegmentLog => None,
            LayoutMode::HashGroup => Some(self.group_of(id)),
        };
        (collection.to_string(), shard, group)
    }

    /// Record a write that already resolved `written` through [`Self::group_of`].
    ///
    /// Touched slots are the partition fanout. They stay out of the flush slice
    /// so a sibling the batch did not write is not synced with it.
    pub(super) fn note_write(&self, id: &str, written: ShardKey) {
        if !crate::placed_write::is_observing() {
            return;
        }
        let touched = match self.opts.layout_mode {
            LayoutMode::HashGroup => {
                let partition = partition_of(id);
                let shard = written.1;
                let collection = written.0.clone();
                self.partition_groups(partition)
                    .into_iter()
                    .map(|group| (collection.clone(), shard, Some(group)))
                    .collect()
            }
            LayoutMode::SegmentLog => vec![written.clone()],
        };
        crate::placed_write::record(crate::placed_write::PlacedWrite {
            id: id.to_string(),
            written,
            touched,
        });
    }

    pub(super) fn transaction_gate_index(collection: &str, id: &str) -> usize {
        let mut hash = 0xcbf29ce484222325_u64;
        for byte in collection
            .as_bytes()
            .iter()
            .copied()
            .chain(std::iter::once(0))
            .chain(id.as_bytes().iter().copied())
        {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        (hash as usize) % TRANSACTION_GATE_COUNT
    }

    /// Class and atom-plane bit for one transaction group.
    ///
    /// The class is the caller's admit code. The atom bit is set when any op
    /// in this group writes the shared atom plane. A later cold load is not
    /// required.
    pub(super) fn transaction_warm_touch(&self, ops: &[TxnOp], key: &ShardKey) -> WarmTouch {
        let atom = ops.iter().any(|op| {
            self.point_key(op.collection(), op.id()) == *key && is_atom_plane_id(op.id())
        });
        WarmTouch {
            class: current_admit_class(),
            atom,
        }
    }

    pub(super) fn cold_load_gate_index(key: &ShardKey) -> usize {
        let mut hash = 0xcbf29ce484222325_u64;
        for byte in key
            .0
            .as_bytes()
            .iter()
            .copied()
            .chain(std::iter::once(0))
            .chain(key.1.to_le_bytes())
            .chain(std::iter::once(u8::from(key.2.is_some())))
            .chain(key.2.unwrap_or_default().to_le_bytes())
        {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        (hash as usize) % COLD_LOAD_GATE_COUNT
    }

    pub(super) fn scan_handle_by_key(&self, key: &ShardKey) -> Result<ScanHandle<'_>> {
        self.scan_handle_by_key_touch(key, WarmTouch::unspecified())
    }

    pub(super) fn scan_handle_by_key_touch(
        &self,
        key: &ShardKey,
        touch: WarmTouch,
    ) -> Result<ScanHandle<'_>> {
        Ok(ScanHandle {
            store: self,
            key: key.clone(),
            handle: Some(self.shard_handle_at(&key.0, key.1, key.2, WarmAdmission::Scan, touch)?),
        })
    }

    /// Whether groups in this store may keep an on-disk id sidecar.
    ///
    /// Plain packaging only. Plain already stores ids verbatim in a
    /// structurally readable segment, so a sidecar beside it discloses nothing
    /// the segment did not; a plaintext id list next to a frame-AEAD cabinet
    /// would give away precisely what the cabinet hides. The options presets
    /// agree with this, but the check belongs on the read/write path too, so a
    /// hand-built `LastStoreOptions` cannot opt an encrypted store in.
    pub(super) fn sidecar_enabled(&self) -> bool {
        self.opts.hash_group_key_sidecar
            && !self.opts.sorted_segments
            && self.opts.layout_mode == LayoutMode::HashGroup
            && self.opts.packaging == PackagingMode::Plain
            && self.opts.data_key.is_none()
    }

    /// In-memory hash groups for `collection` that a walk must still visit.
    ///
    /// `handles_on_disk` / `handle_dir.exists()` only see directories
    /// `readdir` returns. A just-created group (first `put`, dir made by
    /// `create_dir_all` and not yet fsynced) can be invisible to that
    /// enumeration on Docker overlayfs — the Mini runner — while the
    /// authoritative handle is already resident. Local APFS does not hide
    /// it, which is why `list_prefix` after an unflushed `put` is green
    /// on a laptop and empty in CI. Unioning the warm set closes that
    /// gap without a `flush()`: the live handle is the source of truth
    /// for unflushed writes.
    pub(super) fn resident_handles_for(&self, collection: &str) -> Vec<(u16, Option<u32>)> {
        let warm = self.shards.lock().expect("poison");
        let mut out: Vec<(u16, Option<u32>)> = warm
            .handles
            .keys()
            .filter(|key| key.0 == collection)
            .map(|key| (key.1, key.2))
            .collect();
        out.sort_unstable();
        out
    }

    /// Write pins for `collection`. Not part of the warm set.
    ///
    /// Lock order: this takes `pins` and drops it before return. Callers must
    /// not hold `shards` across the call. `existing_unpublished_authority`
    /// takes `pins` then `shards`.
    pub(super) fn pin_handles_for(&self, collection: &str) -> Vec<(u16, Option<u32>)> {
        let pins = self.pins.lock().expect("poison");
        pins.handles
            .keys()
            .filter(|key| key.0 == collection)
            .map(|key| (key.1, key.2))
            .collect()
    }

    pub(super) fn union_walk_handles(
        &self,
        collection: &str,
        mut disk: Vec<(u16, Option<u32>)>,
    ) -> Vec<(u16, Option<u32>)> {
        disk.extend(self.resident_handles_for(collection));
        disk.sort_unstable();
        disk.dedup();
        disk
    }

    /// Handles a `prefix` walk has to visit.
    ///
    /// Exact, not heuristic: under [`HashGroupKey::PartitionPrefix`] a prefix
    /// that already contains the partition separator pins every matching row to
    /// one partition, hence to that partition's `fanout` groups. Any other
    /// combination falls back to the full enumeration.
    pub(super) fn handles_for_prefix(
        &self,
        collection: &str,
        prefix: &str,
    ) -> Result<Vec<(u16, Option<u32>)>> {
        self.validate_prefix_read(prefix)?;
        if self.opts.layout_mode != LayoutMode::HashGroup
            || self.opts.hash_group_key != HashGroupKey::PartitionPrefix
            || !prefix.contains(PARTITION_SEP)
        {
            self.trace_full_sweep(collection, "prefix", prefix);
            return Ok(self.union_walk_handles(collection, self.handles_on_disk(collection)?));
        }
        // `prefix` contains the separator, so partition_of(prefix) is the
        // partition of every id under it.
        self.handles_for_partition(collection, partition_of(prefix))
    }

    /// Handles a `start..end` walk has to visit.
    ///
    /// Exact, not heuristic. Every id `k` with `start <= k < end` has
    /// `lcp(start, end)` as a prefix: `k` agrees with both bounds up to the
    /// first position they differ, and diverging earlier would put `k` outside
    /// the range. So when both bounds resolve to the same separator-terminated
    /// partition, `lcp` spans that partition and every row in the range is
    /// pinned to its `fanout` groups.
    ///
    /// This is the range twin of [`Self::handles_for_prefix`], and it matters
    /// because the molecule codec's range bounds are built from one hash —
    /// `mk:{M}:{esc(hash)}\0{lo}` to `mk:{M}:{esc(hash)}\0{hi}` — so they
    /// always share a partition. Any other combination (bounds straddling
    /// partitions, no separator at all) falls back to the full enumeration.
    ///
    /// The test is `end <= partition_end(P)`, not `partition_of(end) == P`.
    /// Both accept the two-bounds-inside-one-partition case, but only the
    /// former accepts a walk over a **whole** partition, whose exclusive upper
    /// bound is by construction the first id *past* the partition and so has no
    /// separator to agree on. That is the shape every molecule-wide page-index
    /// scan takes (`mhr:{M}\0` … `mhr:{M}\1`), and under the stricter test it
    /// was the shape that always fell back to the full sweep.
    pub(super) fn handles_for_range(
        &self,
        collection: &str,
        start: &str,
        end: &str,
    ) -> Result<Vec<(u16, Option<u32>)>> {
        self.validate_range_read(start, end)?;
        let partition = partition_of(start);
        if self.opts.layout_mode != LayoutMode::HashGroup
            || self.opts.hash_group_key != HashGroupKey::PartitionPrefix
            // A partition that does not end in the separator is an id with no
            // separator at all, which pins nothing.
            || !partition.ends_with(PARTITION_SEP)
            // `start` begins with `partition`, so every id `k` in the range has
            // `partition <= start <= k < end`. When `end` also sits at or below
            // the partition's exclusive bound, every such `k` lies in
            // `[partition, partition_end)` — exactly the ids prefixed by
            // `partition`. Anything above that bound can reach a later
            // partition, and pruning would silently truncate the walk.
            || end > partition_end(partition).as_str()
        {
            self.trace_full_sweep(collection, "range", &format!("{start} .. {end}"));
            return Ok(self.union_walk_handles(collection, self.handles_on_disk(collection)?));
        }
        self.handles_for_partition(collection, partition)
    }

    pub(super) fn reject_unanchored_read<T>(
        &self,
        operation: &'static str,
        prefix_bytes: usize,
    ) -> Result<T> {
        crate::read_activity::rejection();
        self.rejected_partition_reads
            .fetch_add(1, Ordering::Relaxed);
        Err(Error::UnanchoredRead {
            operation,
            prefix_bytes,
        })
    }

    pub(super) fn validate_prefix_read(&self, prefix: &str) -> Result<()> {
        if self.opts.reads_require_partition && !prefix.contains(PARTITION_SEP) {
            return self.reject_unanchored_read("prefix", prefix.len());
        }
        Ok(())
    }

    pub(super) fn validate_range_read(&self, start: &str, end: &str) -> Result<()> {
        if self.opts.reads_require_partition {
            let partition = partition_of(start);
            if !partition.ends_with(PARTITION_SEP)
                || end < partition
                || end > partition_end(partition).as_str()
            {
                return self.reject_unanchored_read("range", start.len());
            }
        }
        Ok(())
    }

    pub(super) fn walk_all_group_handles(
        &self,
        collection: &str,
        _purpose: AllGroupsPurpose,
    ) -> Result<Vec<(u16, Option<u32>)>> {
        self.all_group_walks.fetch_add(1, Ordering::Relaxed);
        crate::read_activity::all_groups();
        Ok(self.union_walk_handles(collection, self.handles_on_disk(collection)?))
    }

    /// Env-gated report of a walk that could not be pruned and therefore
    /// enumerates every group in `collection`.
    ///
    /// A full sweep is invisible from outside: the call returns correct rows,
    /// only slowly, so an unprunable prefix can sit on a hot path for days
    /// (fold #940 found one on *every* write). `lastdb status` surfaces the
    /// durable layout, but not *which key shapes* fail to exploit it. Setting
    /// `LASTDB_TRACE_SWEEP=1` prints the collection, the fallback reason, the
    /// escaped key, and a backtrace naming the caller — which is what turns
    /// "reads are slow" into a specific prefix to fix.
    ///
    /// Off by default and behind an env read, so the hot path pays one
    /// relaxed-ordering load of a cached flag and nothing else.
    pub(super) fn trace_full_sweep(&self, collection: &str, kind: &str, key: &str) {
        if !full_sweep_trace_enabled() {
            return;
        }
        let reason = if self.opts.layout_mode != LayoutMode::HashGroup {
            "layout_mode != hash_group"
        } else if self.opts.hash_group_key != HashGroupKey::PartitionPrefix {
            "hash_group_key != partition_prefix"
        } else {
            "key has no partition separator"
        };
        eprintln!(
            "LASTDB_FULL_SWEEP collection={collection} kind={kind} reason={reason} key={:?}\n{}",
            key,
            std::backtrace::Backtrace::force_capture()
        );
    }

    /// The handles one partition's rows can occupy.
    pub(super) fn handles_for_partition(
        &self,
        collection: &str,
        partition: &str,
    ) -> Result<Vec<(u16, Option<u32>)>> {
        let shard = self.shard_of(partition);
        let groups = self.partition_groups(partition);
        Ok(groups
            .into_iter()
            .map(|group| (shard, Some(group)))
            // Skip groups with nothing on disk *and* nothing resident:
            // loading those would only add empty handles and inflate
            // `shard_loads()`. A pin, lease, or warm handle whose dir
            // `readdir` has not yet observed must still be visited.
            .filter(|(shard, group)| {
                let group = group.expect("hash-group partition walks use Some(group)");
                self.unpublished_group_present(&(collection.to_string(), *shard, Some(group)))
            })
            .collect())
    }

    pub(super) fn handles_on_disk(&self, collection: &str) -> Result<Vec<(u16, Option<u32>)>> {
        match self.opts.layout_mode {
            LayoutMode::SegmentLog => Ok(self
                .shards_on_disk(collection)?
                .into_iter()
                .map(|shard| (shard, None))
                .collect()),
            LayoutMode::HashGroup => self.hash_groups_on_disk(collection),
        }
    }

    pub(super) fn hash_groups_on_disk(&self, collection: &str) -> Result<Vec<(u16, Option<u32>)>> {
        let mut out = Vec::new();
        let collection_dir = self.root.join("data").join(collection);
        if !collection_dir.exists() {
            return Ok(out);
        }
        for shard_entry in fs::read_dir(collection_dir)? {
            let shard_entry = shard_entry?;
            if !shard_entry.file_type()?.is_dir() {
                continue;
            }
            let shard_name = shard_entry.file_name().to_string_lossy().into_owned();
            let Ok(shard) = u16::from_str_radix(&shard_name, 16) else {
                continue;
            };
            let groups_dir = shard_entry.path().join("g");
            if !groups_dir.exists() {
                continue;
            }
            for group_entry in fs::read_dir(groups_dir)? {
                let group_entry = group_entry?;
                if !group_entry.file_type()?.is_dir() {
                    continue;
                }
                let group_name = group_entry.file_name().to_string_lossy().into_owned();
                if let Ok(group) = u32::from_str_radix(&group_name, 16) {
                    out.push((shard, Some(group)));
                }
            }
        }
        out.sort_unstable();
        Ok(out)
    }
}
