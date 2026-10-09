use super::*;

impl LastStore {
    /// Load bodies for `ids`, resolving each owning shard **once per shard**
    /// instead of once per id, preserving the order of `ids`.
    ///
    /// [`Self::get`] resolves the owning shard handle and then re-estimates
    /// warm-set residency on every call, so driving a walk through it costs
    /// `O(ids)` shard resolutions *and* `O(ids)` full residency estimates.
    /// Under [`LayoutMode::HashGroup`] that is pathological: the bounded warm
    /// set evicts a group between rows, so each row re-parses a whole group
    /// segment and rebuilds its index under the shard mutex — a walk returning
    /// a few hundred rows did hundreds of full shard loads and pinned every
    /// other worker on the shard lock (the 0.23.1 read regression).
    ///
    /// Grouping by owning shard keeps a shard resident for exactly the span of
    /// its own rows: each shard is parsed at most once per walk, residency is
    /// re-estimated once per shard, and only one shard is held at a time so the
    /// warm budget is still honoured. Body semantics match [`Self::get`],
    /// including the plaintext `values` cache.
    ///
    /// ## An id that vanishes mid-walk is skipped, not an error
    ///
    /// A walk lists ids and then hydrates them, and it holds no snapshot across
    /// those two passes. A row deleted in between therefore has an id and no
    /// body — an ordinary outcome under a concurrent writer, not damage. This
    /// used to raise `Error::Corrupt` and fail the whole scan, so one delete
    /// racing one walk turned into a 500 for a read that was otherwise
    /// perfectly serviceable, observed on real data as
    /// `corrupt: id vanished during walk: mk:4cd2bacc…`.
    ///
    /// It is skipped rather than surfaced as an empty body because a caller
    /// cannot tell those apart, and a deleted row must read as absent. The skip
    /// is counted in [`Self::walk_vanished_ids`] so it stays observable: the
    /// same missing body would also be produced by a genuine index-entry-
    /// without-body corruption, and silently dropping that is how a store hides
    /// its own damage. `None` here means only "not in the index" — a body that
    /// is present but unreadable still fails the walk, from `read_at`.
    ///
    /// Consequence for the paged callers, and why they no longer wear it: a
    /// skipped id means this function returns fewer rows than it was handed
    /// ids, so a page could come back shorter than its `limit` while more rows
    /// existed beyond it. Every keyset-paging caller treats a short page as
    /// end-of-data, so under a concurrent deleter they stopped early — silently
    /// and mid-range. An earlier version of this note called that harmless
    /// because "fold_db's own callers pass `after: None` and page by `limit`
    /// rather than by cursor". **That was wrong**: the GC plane walks
    /// (`for_each_row_under_prefix`, the `gc-atoms` atom walk,
    /// `rekey_atoms_to_partition_prefix`) are all cursor loops over
    /// `scan_range_paged`, and a truncated reference walk makes `gc-atoms`
    /// delete live atom bodies rather than merely reclaim less.
    ///
    /// So the refill now lives one level up, in [`Self::list_range_paged`] and
    /// [`Self::list_prefix_paged`], which top a short page back up from the key
    /// listing and page on the last id *listed* rather than the last id
    /// *returned*. A short page from those means the range really is done.
    /// This function keeps its own contract unchanged — skip the vanished id,
    /// count it, return what hydrated.
    pub(super) fn load_bodies_by_shard(
        &self,
        collection: &str,
        ids: Vec<String>,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        let bodies = self.load_bodies_grouped(collection, &ids)?;
        let mut vanished = 0u64;
        let rows: Vec<(String, Vec<u8>)> = ids
            .into_iter()
            .zip(bodies)
            .filter_map(|(id, body)| match body {
                Some(body) => Some((id, body)),
                None => {
                    vanished += 1;
                    None
                }
            })
            .collect();
        if vanished > 0 {
            self.walk_vanished_ids
                .fetch_add(vanished, Ordering::Relaxed);
        }
        Ok(rows)
    }

    /// Bodies for `ids`, resolving each owning shard **once per shard**.
    ///
    /// Slot `i` of the result is the body of `ids[i]`, or `None` when that id
    /// is absent. Shared by the walk hydrate path and the public batch
    /// [`Self::get_many`]; see [`Self::load_bodies_by_shard`] for why grouping
    /// by shard matters under [`LayoutMode::HashGroup`].
    pub(super) fn load_bodies_grouped(
        &self,
        collection: &str,
        ids: &[String],
    ) -> Result<Vec<Option<Vec<u8>>>> {
        // Group ids by owning shard, remembering each id's slot so the caller
        // still gets them in the order they asked for.
        let mut by_shard: BTreeMap<ShardKey, Vec<(usize, String)>> = BTreeMap::new();
        for (slot, id) in ids.iter().enumerate() {
            by_shard
                .entry(self.point_key(collection, id))
                .or_default()
                .push((slot, id.clone()));
        }

        let mut bodies: Vec<Option<Vec<u8>>> = vec![None; ids.len()];
        for (key, entries) in by_shard {
            // Holding `h` across the whole span keeps this shard pinned against
            // `evict_hash_group_warm_set` (strong_count > 2), so its rows cannot
            // force a reload of the group we are already reading.
            // Scan sets the atom-plane flag from the id and does not stamp
            // or record an owner class.
            let atom = entries.iter().any(|(_, id)| is_atom_plane_id(id));
            let h = self.scan_handle_by_key_touch(
                &key,
                WarmTouch {
                    class: AdmitClass::Unspecified,
                    atom,
                },
            )?;
            {
                let mut sh = h.handle().lock().expect("poison");
                for (slot, id) in &entries {
                    bodies[*slot] = Self::current_body_locked(&mut sh, id)?;
                }
            }
            // `ScanHandle::drop` trims reproducible bytes, refreshes the full
            // charge once for this group, and enforces the scan segment.
        }

        Ok(bodies)
    }

    /// Keys only in half-open `[start, end)`, ascending, at most `limit`.
    pub fn list_range_keys_paged(
        &self,
        collection: &str,
        start: &str,
        end: &str,
        limit: usize,
    ) -> Result<Vec<String>> {
        self.validate_range_read(start, end)?;
        if limit == 0 || start >= end {
            return Ok(Vec::new());
        }
        let mut merged = BoundedMerge::new(limit);
        let mut visited = 0u64;
        let bounds = start.to_string()..end.to_string();
        for (shard, group) in self.handles_for_range(collection, start, end)? {
            match self.group_key_source_with_window(
                collection,
                shard,
                group,
                Some(keysidecar::KeyWindow {
                    start,
                    end: Some(end),
                    after_id: None,
                    limit,
                }),
            )? {
                GroupKeySource::Live(h) => {
                    let sh = h.handle().lock().expect("poison");
                    visited += sh.visit_keys(start, Some(end), |id, _| {
                        merged.offer(id) != Offer::StopGroup
                    })?;
                }
                GroupKeySource::Cached(keys) => {
                    for id in keys.range(bounds.clone()) {
                        visited += 1;
                        if merged.offer(id) == Offer::StopGroup {
                            break;
                        }
                    }
                }
            }
        }
        self.walk_ids_visited.fetch_add(visited, Ordering::Relaxed);
        Ok(merged.into_vec())
    }

    /// Greatest decimal `u64` suffix after the last `marker` among ids under
    /// `prefix`.
    ///
    /// This keys-only aggregate visits every physical group at most once and
    /// holds only the current maximum. It does not materialize the matching
    /// ids or hydrate their bodies.
    pub fn max_u64_id_suffix(
        &self,
        collection: &str,
        prefix: &str,
        marker: &str,
    ) -> Result<Option<u64>> {
        if marker.is_empty() {
            return Err(Error::Config(
                "max_u64_id_suffix marker must not be empty".into(),
            ));
        }
        let start = prefix.to_string();
        let mut greatest = None;
        let mut visited = 0u64;
        for (shard, group) in self.handles_for_prefix(collection, prefix)? {
            match self.group_key_source(collection, shard, group)? {
                GroupKeySource::Live(handle) => {
                    let shard = handle.handle().lock().expect("poison");
                    shard.visit_keys(&start, None, |id, _| {
                        if !id.starts_with(prefix) {
                            return false;
                        }
                        visited += 1;
                        if let Some(value) = id
                            .rsplit_once(marker)
                            .and_then(|(_, suffix)| suffix.parse::<u64>().ok())
                        {
                            greatest =
                                Some(greatest.map_or(value, |current: u64| current.max(value)));
                        }
                        true
                    })?;
                }
                GroupKeySource::Cached(keys) => {
                    visited += fold_max_u64_id_suffix(
                        keys.range(start.clone()..),
                        prefix,
                        marker,
                        &mut greatest,
                    );
                }
            }
        }
        self.walk_ids_visited.fetch_add(visited, Ordering::Relaxed);
        Ok(greatest)
    }
}
