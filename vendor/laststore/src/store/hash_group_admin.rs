use super::*;

impl LastStore {
    /// Evict unpinned LRU groups until resident bytes are at or under `target`.
    ///
    /// Pinned groups (an in-flight get/put still holds the handle) are skipped.
    /// This path never refuses a caller; it only drops idle groups.
    pub fn evict_hash_group_warm_set_to_bytes(&self, target: u64) -> Result<WarmEvictionReport> {
        let bytes_before = self.hash_group_warm_stats().resident_bytes;
        let groups_evicted = self.evict_hash_group_warm_set_inner(false, None, Some(target))?;
        let bytes_after = self.hash_group_warm_stats().resident_bytes;
        Ok(WarmEvictionReport {
            groups_evicted,
            bytes_before,
            bytes_after,
        })
    }

    /// The hash group index `id` is placed in, for `collection`.
    ///
    /// Placement is a pure function of the id and the durable layout options,
    /// so this reads nothing. Segment-log homes have no groups and answer `0`.
    /// Exposed for owner maintenance that must name a group without opening
    /// it (`drop_hash_group_dir`).
    pub fn group_index_of(&self, collection: &str, id: &str) -> u32 {
        let _ = collection;
        match self.opts.layout_mode {
            LayoutMode::HashGroup => self.group_of(id),
            LayoutMode::SegmentLog => 0,
        }
    }

    /// Remove one cold hash group directory whole, without loading it, after
    /// proving from its id sidecar that it holds nothing but `expected_only_id`.
    ///
    /// This is the reclaim for a group that a single key filled with
    /// superseded copies of itself until it could no longer be loaded: the
    /// primary's `metadata/0/g/025` on 2026-09-21 — 6,782 segments, 39 GB,
    /// one id (`keep_small:meters`) — which `shard_handle_at` now refuses
    /// ([`Error::ColdGroupTooLarge`]) and which `compact` cannot touch
    /// because compaction loads the group first. Dropping the directory is
    /// the only path that returns the bytes without paying the load.
    ///
    /// A group directory that does not exist returns `Ok` with
    /// `already_absent: true` and zero bytes: the reclaim is idempotent.
    ///
    /// Refuses (without touching the disk) when:
    /// - the layout is not hash-group, or `expected_only_id` does not place
    ///   in `group` — the caller named the wrong group;
    /// - the group is resident in the warm set — a live handle owns it;
    /// - there is no sidecar the store can trust for the group's ids
    ///   (`keysidecar::read_ids_tolerating_appends`), which includes every
    ///   frame-AEAD home since those write no sidecar;
    /// - the sidecar records any id other than `expected_only_id`.
    ///
    /// The id proof reads only the sidecar plus segments appended after it
    /// was written, one at a time. It never calls `load_shard`.
    ///
    /// With `execute` the directory is renamed to `<dir>.reclaim-<unix>`
    /// under the warm-set lock (so no load can race the rename), removed,
    /// the parent is fsynced, and the group's cached id set is forgotten. The
    /// next access to the group creates it empty, which is the correct state
    /// for an id whose every copy was dead bytes. Without `execute` the
    /// report carries the same measurements and the directory is untouched.
    // lint:fn-size-ok verbatim move from store.rs; splitting this function is separate work
    pub fn drop_hash_group_dir(
        &self,
        collection: &str,
        group: u32,
        expected_only_id: &str,
        execute: bool,
    ) -> Result<DroppedGroupReport> {
        if execute {
            self.require_writable()?;
        }
        if self.opts.layout_mode != LayoutMode::HashGroup {
            return Err(Error::Config(
                "drop_hash_group_dir: store is not hash-group layout".into(),
            ));
        }
        let placed = self.group_of(expected_only_id);
        if placed != group {
            return Err(Error::Config(format!(
                "drop_hash_group_dir: id places in group {placed:#05x}, not {group:#05x}"
            )));
        }
        let shard = self.shard_of(expected_only_id);
        let key: ShardKey = (collection.to_string(), shard, Some(group));
        let dir = self.hash_group_dir(collection, shard, group);
        if !dir.exists() {
            // Idempotent: a second run after a successful drop (or a home that
            // never wrote the group) has nothing to return. Answer that
            // explicitly and name the path checked, so an operator can tell
            // "already reclaimed" from a refusal.
            return Ok(DroppedGroupReport {
                collection: collection.to_string(),
                shard,
                group,
                dir,
                already_absent: true,
                ..DroppedGroupReport::default()
            });
        }
        if !dir.is_dir() {
            return Err(Error::Config(format!(
                "drop_hash_group_dir: {} exists but is not a group directory; refusing",
                dir.display()
            )));
        }
        if self.shards.lock().expect("poison").holds(&key) {
            return Err(Error::Config(format!(
                "drop_hash_group_dir: {collection} group {group:#05x} is resident in the warm \
                 set; evict it (or stop writing to it) first"
            )));
        }
        let Some(ids) = keysidecar::read_ids_tolerating_appends(&dir) else {
            return Err(Error::Config(format!(
                "drop_hash_group_dir: no trusted id sidecar for {collection} group {group:#05x} \
                 at {}; refusing to drop a group whose ids cannot be proven without a load",
                dir.display()
            )));
        };
        let foreign: Vec<&String> = ids.iter().filter(|id| *id != expected_only_id).collect();
        if !foreign.is_empty() {
            let sample: Vec<&str> = foreign.iter().take(8).map(|id| id.as_str()).collect();
            return Err(Error::Config(format!(
                "drop_hash_group_dir: {collection} group {group:#05x} holds {} id(s) other than \
                 the expected one (sample {sample:?}); refusing",
                foreign.len()
            )));
        }
        let segments = keysidecar::segment_stamps(&dir)?.len() as u64;
        let on_disk_bytes = estimate_group_on_disk_bytes(&dir);
        let mut report = DroppedGroupReport {
            collection: collection.to_string(),
            shard,
            group,
            dir: dir.clone(),
            segments,
            on_disk_bytes,
            ids: ids.into_iter().collect(),
            dropped: false,
            already_absent: false,
        };
        if !execute {
            return Ok(report);
        }
        let unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let aside = dir.with_file_name(format!(
            "{}.reclaim-{unix}",
            dir.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("group")
        ));
        {
            // Hold the warm-set lock across the residency re-check and the
            // rename: `shard_handle_at` takes this lock before it decides to
            // load, so a loader either sees the handle-less key and then a
            // missing directory (an empty group), or waits here.
            let warm = self.shards.lock().expect("poison");
            if warm.holds(&key) {
                return Err(Error::Config(format!(
                    "drop_hash_group_dir: {collection} group {group:#05x} became resident \
                     during the drop; refusing"
                )));
            }
            fs::rename(&dir, &aside)?;
        }
        self.key_index.lock().expect("poison").forget(&key);
        fs::remove_dir_all(&aside)?;
        if let Some(parent) = dir.parent() {
            if let Ok(parent_dir) = fs::File::open(parent) {
                let _ = parent_dir.sync_all();
            }
        }
        report.dropped = true;
        Ok(report)
    }

    /// [`Self::flush`] durability barriers since open (see the field docs).
    pub fn flush_barriers(&self) -> u64 {
        self.flush_barriers.load(Ordering::Relaxed)
    }

    /// Groups `sync_data`'d by those barriers since open (see the field docs).
    ///
    /// `groups_synced / flush_barriers` is the average barrier width. A
    /// transaction's barrier should be about as wide as the transaction; much
    /// wider means it is paying for writes it did not make.
    pub fn groups_synced(&self) -> u64 {
        self.groups_synced.load(Ordering::Relaxed)
    }

    /// Width of the last [`Self::flush_scope`], in groups.
    ///
    /// This is `scope.len()` for that call. It is not [`Self::groups_synced`].
    pub fn groups_synced_last_flush(&self) -> u64 {
        self.groups_synced_last_flush.load(Ordering::Relaxed)
    }

    /// Groups the last scoped flush named that were not in the written set.
    pub fn flush_foreign_groups(&self) -> u64 {
        self.flush_foreign_groups.load(Ordering::Relaxed)
    }

    /// Publish [`Self::flush_foreign_groups`] for the scoped flush about to run.
    ///
    /// Not a second flush. The store syncs the slice it is given and cannot
    /// see which of those keys the batch wrote.
    pub fn set_flush_foreign_groups(&self, count: u64) {
        self.flush_foreign_groups.store(count, Ordering::Relaxed);
    }

    /// Ids stepped over by keys-only passes since open (see the field docs).
    ///
    /// A *paged* walk should visit about one page's worth plus a constant per
    /// group. Growth with the size of the band rather than the page is the
    /// quadratic-paging signature.
    pub fn walk_ids_visited(&self) -> u64 {
        self.walk_ids_visited.load(Ordering::Relaxed)
    }

    /// Product read-shape rejections. One relaxed load for request telemetry.
    pub fn rejected_partition_reads(&self) -> u64 {
        self.rejected_partition_reads.load(Ordering::Relaxed)
    }

    /// Explicit startup/admin all-group passes, including physical key counts.
    pub fn all_group_walks(&self) -> u64 {
        self.all_group_walks.load(Ordering::Relaxed)
    }

    /// Ids skipped by a walk because their row vanished between the keys pass
    /// and the bodies pass (see the field docs).
    ///
    /// Expected to be non-zero on a home taking concurrent deletes — the
    /// converging write that displaces migration-era rows is exactly such a
    /// writer. A store with no deleter running that reports vanishes is
    /// reporting index entries without bodies, which is real corruption.
    pub fn walk_vanished_ids(&self) -> u64 {
        self.walk_vanished_ids.load(Ordering::Relaxed)
    }

    /// Frame compression totals for `collection` since this process opened the store.
    pub fn frame_compression_stats_for(&self, collection: &str) -> FrameCompressionStats {
        self.frame_compression_stats
            .lock()
            .expect("poison")
            .get(collection)
            .copied()
            .unwrap_or_default()
    }

    /// Copy the live document set into a separate fresh hash-group home and
    /// verify full key/value parity before returning success.
    ///
    /// The source is read-only. The destination must not exist or must be an
    /// empty directory; an interrupted destination is intentionally not
    /// resumed or promoted automatically.
    pub fn migrate_to_hash_group(
        &self,
        destination: impl AsRef<Path>,
        destination_options: LastStoreOptions,
    ) -> Result<LayoutMigrationReport> {
        self.migrate_to_hash_group_with(destination, destination_options, |_, _, body| {
            Ok(body.to_vec())
        })
    }

    /// Copy the live document set while transforming each value before it is
    /// written. This is used by upper layers that must remove a legacy value
    /// envelope while moving protection into hash-group frame AEAD.
    pub fn migrate_to_hash_group_with<F>(
        &self,
        destination: impl AsRef<Path>,
        mut destination_options: LastStoreOptions,
        mut transform: F,
    ) -> Result<LayoutMigrationReport>
    where
        F: FnMut(&str, &str, &[u8]) -> Result<Vec<u8>>,
    {
        let destination = destination.as_ref();
        ensure_empty_migration_destination(destination)?;
        destination_options.layout_mode = LayoutMode::HashGroup;
        // New migrations always emit plain packaging (restart-safe open tails).
        // Atom body secrecy is an upper-layer concern (content field seal).
        destination_options.packaging = PackagingMode::Plain;
        destination_options.data_key = None;
        destination_options.layout_epoch = self.opts.layout_epoch.saturating_add(1);
        let target = Self::open_with(destination, destination_options)?;

        let mut collections = BTreeMap::new();
        for collection in self.collections_on_disk()? {
            // Source ids are lexicographically ordered, while destination
            // placement is hash ordered. Writing in source order repeatedly
            // evicts and reopens the same bounded hash-group handles, creating
            // one tiny encrypted tail per revisit on large collections.
            // Decorate and sort keys only; values remain streamed one at a
            // time so migration memory is proportional to id metadata rather
            // than the live data set.
            let mut ids = self
                .list_prefix_keys(&collection, "")?
                .into_iter()
                .map(|id| (target.shard_of(&id), target.group_of(&id), id))
                .collect::<Vec<_>>();
            ids.sort_unstable();
            let count = ids.len() as u64;
            for (_, _, id) in ids {
                let body = self
                    .get(&collection, &id)?
                    .ok_or_else(|| Error::Corrupt(format!("id vanished during migration: {id}")))?;
                let transformed = transform(&collection, &id, &body)?;
                target.put(&collection, &id, &transformed)?;
            }
            if count > 0 {
                collections.insert(collection, count);
            }
        }
        target.flush()?;

        verify_migration_parity(self, &target, &collections, &mut transform)?;
        let total_documents = collections.values().copied().sum();
        Ok(LayoutMigrationReport {
            collections,
            total_documents,
        })
    }

    /// Root directory of this store.
    pub fn path(&self) -> &Path {
        &self.root
    }

    /// Options used when this store was opened.
    pub fn options(&self) -> &LastStoreOptions {
        &self.opts
    }

    /// Highest commit sequence number assigned or accepted from the open floor.
    pub fn csn_high_water(&self) -> u64 {
        let meta = self.meta.lock().expect("poison");
        meta.next_csn.saturating_sub(1)
    }

    /// Whether capture has been suspended after a capture hook failure.
    pub fn capture_suspended(&self) -> bool {
        self.meta.lock().expect("poison").capture_suspended
    }

    /// In-memory CDC tail captured since this store handle opened.
    pub fn capture_log(&self) -> Vec<CaptureEvent> {
        self.meta.lock().expect("poison").capture_log.clone()
    }

    /// Immutable chunks sealed since this store handle opened.
    pub fn sealed_chunks(&self) -> Vec<SealedChunkMeta> {
        self.meta.lock().expect("poison").sealed_chunks.clone()
    }
}
