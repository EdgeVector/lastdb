// lint:file-size-ok verbatim move from store.rs; splitting this file further is separate work
use super::*;

impl LastStore {
    /// Documents with ids in the half-open range `[start, end)` (sorted).
    ///
    /// Empty if `start >= end`. Does not require a shared prefix, but for
    /// multi-shard stores this still visits every shard (correct, not always
    /// optimal).
    pub fn list_range(
        &self,
        collection: &str,
        start: &str,
        end: &str,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        self.list_range_paged(collection, start, end, usize::MAX)
    }

    /// At most `limit` docs in half-open `[start, end)`.
    ///
    /// **A short page means the range is EXHAUSTED.** That is what every
    /// keyset-paging caller reads it as, and it is the invariant this call has
    /// to keep, because [`Self::load_bodies_by_shard`] drops ids whose bodies
    /// vanished between the listing pass and the hydrate pass. Returning that
    /// shorter vector directly made "fewer rows than I asked for" ambiguous
    /// between *the range is done* and *a concurrent writer deleted one of
    /// these* — and every cursor loop in fold_db resolves that ambiguity the
    /// unsafe way, by stopping.
    ///
    /// That is not a rare race. `LogicalMainLastStoreKvStore::converging_deletes`
    /// makes **every write a deleter** on a home that still holds
    /// migration-era legacy rows, so ordinary mutation traffic shortens pages.
    /// The consequence is worst in
    /// `DbOperations::collect_referenced_atom_uuids`, where a truncated walk
    /// does not mean "reclaimed less" but "did not see the references that keep
    /// live atom bodies alive": `gc-atoms` then treats every atom named only
    /// past the truncation point as an orphan and deletes the bodies of live
    /// records. It had never fired only because `gc-atoms` had never finished a
    /// pass on a real home — which is precisely what the paging work is for.
    ///
    /// So the page is refilled from the key listing until it is full or the
    /// listing runs dry. Callers keep their `rows.len() < limit` test and it is
    /// true again.
    pub fn list_range_paged(
        &self,
        collection: &str,
        start: &str,
        end: &str,
        limit: usize,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        self.validate_range_read(start, end)?;
        if limit == 0 || start >= end {
            return Ok(Vec::new());
        }
        let mut rows: Vec<(String, Vec<u8>)> = Vec::new();
        let mut cursor = start.to_string();
        let mut resumed = false;
        while rows.len() < limit {
            let want = limit - rows.len();
            // The range start is INCLUSIVE, so a resumed listing re-reads the
            // cursor id and drops it; it must ask for one more than it needs in
            // order to make progress. Restarting *at* the cursor rather than
            // synthesising a successor keeps this free of any assumption about
            // how ids are ordered or escaped.
            let ask = if resumed {
                want.saturating_add(1)
            } else {
                want
            };
            let ids = self.list_range_keys_paged(collection, &cursor, end, ask)?;
            if ids.is_empty() {
                break;
            }
            // The LISTING, not the hydration, proves the range is done.
            let listing_full = ids.len() == ask;
            // Last id LISTED — advances even when its own body vanished.
            let last_listed = ids[ids.len() - 1].clone();
            let ids: Vec<String> = if resumed {
                ids.into_iter().filter(|id| *id != cursor).collect()
            } else {
                ids
            };
            rows.extend(self.load_bodies_by_shard(collection, ids)?);
            if !listing_full {
                break;
            }
            // A listing that yielded nothing past the cursor cannot advance;
            // stop rather than spin.
            if last_listed == cursor {
                break;
            }
            cursor = last_listed;
            resumed = true;
        }
        rows.truncate(limit);
        Ok(rows)
    }

    /// Read a half-open range while resolving at most `max_handles` physical
    /// shard/group handles.
    ///
    /// This is an operator-maintenance primitive, not a product list query.
    /// Rows are ordered inside each physical handle, but not globally across
    /// handles. The opaque physical position is therefore the only valid
    /// cursor. Use [`Self::list_range_paged`] when global key order is part of
    /// the access contract.
    ///
    /// New handles that sort before the cursor can appear during a live pass.
    /// They are intentionally picked up on the next lap. Maintenance callers
    /// must treat a completed walk as a lap, not as a permanent snapshot.
    pub fn list_range_physical_paged(
        &self,
        collection: &str,
        start: &str,
        end: &str,
        cursor: Option<&PhysicalRangeCursor>,
        limit: usize,
        max_handles: usize,
    ) -> Result<PhysicalRangePage> {
        if self.opts.reads_require_partition {
            return self.reject_unanchored_read("physical_range: use walk_all_groups", start.len());
        }
        self.walk_all_groups(
            collection,
            start..end,
            cursor,
            limit,
            max_handles,
            AllGroupsPurpose::Admin,
        )
    }

    /// Return any matching key for initial format detection. This explicit
    /// startup pass stops at its first match without fetching a record value.
    /// Sorted groups load their footer and bounded tail; legacy group recovery
    /// retains its existing loader behavior.
    /// The result follows physical group order, not global key order.
    pub fn probe_key_prefix_at_startup(
        &self,
        collection: &str,
        prefix: &str,
    ) -> Result<Option<String>> {
        for (shard, group) in self.walk_all_group_handles(collection, AllGroupsPurpose::Startup)? {
            let mut found = None;
            match self.group_key_source(collection, shard, group)? {
                GroupKeySource::Live(handle) => {
                    let shard = handle.handle().lock().expect("poison");
                    shard.visit_keys(prefix, None, |id, _| {
                        if id.starts_with(prefix) {
                            found = Some(id.to_string());
                        }
                        false
                    })?;
                }
                GroupKeySource::Cached(keys) => {
                    found = keys
                        .range(prefix.to_string()..)
                        .next()
                        .filter(|key| key.starts_with(prefix))
                        .cloned();
                }
            }
            if found.is_some() {
                self.walk_ids_visited.fetch_add(1, Ordering::Relaxed);
                return Ok(found);
            }
        }
        Ok(None)
    }

    /// Load node-local schema catalog metadata for initial load or refresh.
    /// The catalog stays resident; this is not a molecule/atom query fallback.
    /// Each group contributes only keys under the requested prefix.
    pub fn load_local_catalog_prefix(
        &self,
        collection: &str,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        if !matches!(
            collection,
            "schemas" | "schema_states" | "schema_superseded_by"
        ) {
            return self
                .reject_unanchored_read("catalog load: non-catalog collection", prefix.len());
        }
        let mut rows = Vec::new();
        for (shard, group) in self.walk_all_group_handles(collection, AllGroupsPurpose::Startup)? {
            let mut keys = Vec::new();
            let source = self.group_key_source(collection, shard, group)?;
            match &source {
                GroupKeySource::Live(handle) => {
                    let shard = handle.handle().lock().expect("poison");
                    shard.visit_keys(prefix, None, |id, _| {
                        if !id.starts_with(prefix) {
                            return false;
                        }
                        keys.push(id.to_string());
                        true
                    })?;
                }
                GroupKeySource::Cached(cached) => {
                    keys.extend(
                        cached
                            .range(prefix.to_string()..)
                            .take_while(|key| key.starts_with(prefix))
                            .cloned(),
                    );
                }
            }
            self.walk_ids_visited
                .fetch_add(keys.len() as u64, Ordering::Relaxed);
            // Keep the key-pass lease through the grouped body read. A tiny
            // warm budget must not evict this group between the two passes.
            let bodies = self.get_many(collection, &keys)?;
            for (key, value) in keys.into_iter().zip(bodies) {
                // A concurrent removal may make a previously visible key absent.
                if let Some(value) = value {
                    rows.push((key, value));
                }
            }
            drop(source);
        }
        Ok(rows)
    }

    /// Explicit startup/admin access across physical groups. It is never a
    /// fallback for a product query. Each page bounds handles and returned rows.
    pub fn walk_all_groups(
        &self,
        collection: &str,
        range: std::ops::Range<&str>,
        cursor: Option<&PhysicalRangeCursor>,
        limit: usize,
        max_handles: usize,
        purpose: AllGroupsPurpose,
    ) -> Result<PhysicalRangePage> {
        self.walk_all_groups_bounded(
            collection,
            (range.start, Some(range.end)),
            cursor,
            limit,
            max_handles,
            purpose,
        )
    }

    /// Explicit bounded startup integrity walk, including every encoded key.
    /// An absent upper bound is essential: no finite string covers all keys.
    pub fn walk_all_groups_at_startup(
        &self,
        collection: &str,
        cursor: Option<&PhysicalRangeCursor>,
        limit: usize,
        max_handles: usize,
    ) -> Result<PhysicalRangePage> {
        self.walk_all_groups_bounded(
            collection,
            ("", None),
            cursor,
            limit,
            max_handles,
            AllGroupsPurpose::Startup,
        )
    }

    pub(super) fn walk_all_groups_bounded(
        &self,
        collection: &str,
        bounds: (&str, Option<&str>),
        cursor: Option<&PhysicalRangeCursor>,
        limit: usize,
        max_handles: usize,
        purpose: AllGroupsPurpose,
    ) -> Result<PhysicalRangePage> {
        if limit == 0 || max_handles == 0 || bounds.1.is_some_and(|end| bounds.0 >= end) {
            return self.walk_all_groups_in_handles(
                collection,
                bounds,
                cursor,
                limit,
                max_handles,
                &[],
            );
        }
        let handles = self.walk_all_group_handles(collection, purpose)?;
        self.walk_all_groups_in_handles(collection, bounds, cursor, limit, max_handles, &handles)
    }

    pub(super) fn walk_all_groups_in_handles(
        &self,
        collection: &str,
        bounds: (&str, Option<&str>),
        cursor: Option<&PhysicalRangeCursor>,
        limit: usize,
        max_handles: usize,
        handles: &[(u16, Option<u32>)],
    ) -> Result<PhysicalRangePage> {
        let (start, end) = bounds;
        let cold_before = self.shard_loads();
        if limit == 0 || max_handles == 0 || end.is_some_and(|end| start >= end) {
            return Ok(PhysicalRangePage {
                rows: Vec::new(),
                next_cursor: cursor.cloned(),
                row_handle: None,
                handles_visited: 0,
                cold_shard_loads: 0,
            });
        }

        let cursor_handle = cursor.map(|position| (position.shard, position.group_id));
        let mut handle_index = cursor_handle.map_or(0, |wanted| {
            handles
                .binary_search(&wanted)
                .unwrap_or_else(std::convert::identity)
        });
        let mut after_id = cursor
            .filter(|position| {
                handles.get(handle_index).copied() == Some((position.shard, position.group_id))
            })
            .and_then(|position| position.after_id.clone());
        let mut rows = Vec::new();
        let mut handles_visited = 0u64;
        let mut row_handle = None;
        let mut next_cursor = None;

        while handle_index < handles.len()
            && handles_visited < max_handles as u64
            && rows.len() < limit
        {
            let (shard, group_id) = handles[handle_index];
            handles_visited += 1;
            let want = limit - rows.len();
            // Retain the lease between key selection and the grouped body read.
            let window = keysidecar::KeyWindow {
                start,
                end,
                after_id: after_id.as_deref(),
                limit: want.saturating_add(1),
            };
            let source =
                self.group_key_source_with_window(collection, shard, group_id, Some(window))?;
            let ids = self.list_range_keys_in_handle(
                &source,
                (start, end),
                after_id.as_deref(),
                want.saturating_add(1),
            )?;
            let handle_has_more = ids.len() > want;
            let selected: Vec<String> = ids.into_iter().take(want).collect();
            let last_selected = selected.last().cloned();
            if !selected.is_empty() {
                row_handle = Some((shard, group_id));
                rows.extend(self.load_bodies_by_shard(collection, selected)?);
            }
            drop(source);

            if handle_has_more {
                next_cursor = Some(PhysicalRangeCursor {
                    shard,
                    group_id,
                    after_id: last_selected,
                });
                break;
            }

            handle_index += 1;
            after_id = None;
            if let Some((next_shard, next_group)) = handles.get(handle_index).copied() {
                next_cursor = Some(PhysicalRangeCursor {
                    shard: next_shard,
                    group_id: next_group,
                    after_id: None,
                });
            } else {
                next_cursor = None;
            }
        }

        self.walk_ids_visited
            .fetch_add(rows.len() as u64, Ordering::Relaxed);
        Ok(PhysicalRangePage {
            rows,
            next_cursor,
            row_handle,
            handles_visited,
            cold_shard_loads: self.shard_loads().saturating_sub(cold_before),
        })
    }

    /// Keys in one physical handle, ordered and bounded.
    pub(super) fn list_range_keys_in_handle(
        &self,
        source: &GroupKeySource,
        bounds: (&str, Option<&str>),
        after_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<String>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let (start, end) = bounds;
        let range_start = after_id.map_or(start, |after| after.max(start));
        let mut out = Vec::with_capacity(limit);
        match source {
            GroupKeySource::Live(handle) => {
                let shard = handle.handle().lock().expect("poison");
                shard.visit_keys(range_start, end, |id, _| {
                    if after_id != Some(id) {
                        out.push(id.to_string());
                    }
                    out.len() < limit
                })?;
            }
            GroupKeySource::Cached(keys) => {
                for id in keys.range(range_start.to_string()..) {
                    if end.is_some_and(|end| id.as_str() >= end) {
                        break;
                    }
                    if after_id == Some(id.as_str()) {
                        continue;
                    }
                    out.push(id.clone());
                    if out.len() == limit {
                        break;
                    }
                }
            }
        }
        Ok(out)
    }
}
