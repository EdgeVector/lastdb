use super::*;

impl LastStore {
    /// List documents whose ids start with `prefix` (sorted by id).
    ///
    /// Loads bodies. Prefer [`Self::list_prefix_keys`] when only ids are needed.
    pub fn list_prefix(&self, collection: &str, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        self.list_prefix_paged(collection, prefix, None, usize::MAX)
    }

    /// Ids only under `prefix` (sorted). Does **not** read or decode bodies —
    /// cheap discovery for GC, key lists, and outbox counters.
    pub fn list_prefix_keys(&self, collection: &str, prefix: &str) -> Result<Vec<String>> {
        self.list_prefix_keys_paged(collection, prefix, None, usize::MAX)
    }

    /// At most `limit` documents under `prefix`, ascending by id.
    ///
    /// `after`: exclusive cursor — only ids **strictly greater** than `after`
    /// (and still under `prefix`). Use the last id of the previous page.
    /// `limit == 0` → empty without scanning.
    /// A short page means the prefix is EXHAUSTED, never that a body vanished:
    /// the page is refilled from the key listing until it is full or the
    /// listing runs dry. See [`Self::list_range_paged`] for why that invariant
    /// is load-bearing rather than cosmetic.
    ///
    /// Ids use live authority, a valid key cache, or a verified sidecar.
    /// Only groups with selected ids need a body read, once per group.
    /// Cold fallbacks and body reads use unpublished handles, so this path
    /// does not admit storage groups into the warm set. A prefix that is one
    /// molecule hash uses `load_hash` instead of this function.
    pub fn list_prefix_paged(
        &self,
        collection: &str,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        self.validate_prefix_read(prefix)?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut rows: Vec<(String, Vec<u8>)> = Vec::new();
        let mut cursor: Option<String> = after.map(str::to_string);
        while rows.len() < limit {
            let want = limit - rows.len();
            let mut page =
                self.unpublished_prefix_keys_paged(collection, prefix, cursor.as_deref(), want)?;
            if page.ids.is_empty() {
                break;
            }
            // The LISTING, not the hydration, is what proves the prefix is
            // done: a full listing whose bodies came up short means rows
            // vanished mid-walk, and more may lie beyond them.
            let listing_full = page.ids.len() == want;
            // `after` is exclusive, so the last id LISTED always makes forward
            // progress — even when its own body vanished.
            let last_listed = page.ids[page.ids.len() - 1].clone();
            for id in page.ids {
                if let Some(body) = page.bodies.remove(&id) {
                    rows.push((id, body));
                }
            }
            if !listing_full {
                break;
            }
            cursor = Some(last_listed);
        }
        rows.truncate(limit);
        Ok(rows)
    }

    /// Select ids before fetching their bodies through grouped unpublished
    /// reads. A vanished listed id is counted and the caller refills the page.
    pub(super) fn unpublished_prefix_keys_paged(
        &self,
        collection: &str,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<UnpublishedPrefixPage> {
        let ids = self.unpublished_prefix_ids_paged(collection, prefix, after, limit)?;
        let points = self.load_existing_points(collection, &ids)?;
        let mut bodies = HashMap::new();
        let mut vanished = 0u64;
        for (id, point) in ids.iter().zip(points) {
            match point.and_then(|point| point.body) {
                Some(body) => {
                    bodies.insert(id.clone(), body);
                }
                None => vanished += 1,
            }
        }
        if vanished > 0 {
            self.walk_vanished_ids
                .fetch_add(vanished, Ordering::Relaxed);
        }
        Ok(UnpublishedPrefixPage { ids, bodies })
    }

    /// Use the same authoritative id tiers as a keys-only pass, without its
    /// cold scan admission. Pins remain visible before their directory does.
    fn unpublished_prefix_ids_paged(
        &self,
        collection: &str,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<String>> {
        self.validate_prefix_read(prefix)?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        let start = match after {
            Some(a) if a.starts_with(prefix) => a.to_string(),
            _ => prefix.to_string(),
        };
        let skip_exact = after.filter(|a| a.starts_with(prefix));
        let mut merged = BoundedMerge::new(limit);
        let mut visited = 0u64;
        for (shard, group) in self.prefix_page_handles(collection, prefix)? {
            if let Some(keys) = self.cached_group_keys(collection, shard, group, None)? {
                visited +=
                    collect_prefix(keys.range(start.clone()..), prefix, skip_exact, &mut merged);
                continue;
            }
            let key = (collection.to_string(), shard, group);
            let handle = self.open_group_unpublished(key.clone())?;
            // Pins lock, then the shard lock. A reap holds the pins lock and
            // then the shard, so this check must not run under the shard lock.
            let pinned = self
                .pin_table_handle(&key)
                .is_some_and(|pin| Arc::ptr_eq(&pin, &handle));
            {
                let mut shard = handle.lock().expect("poison");
                shard.visit_keys(&start, None, |id, _| {
                    if !id.starts_with(prefix) {
                        return false;
                    }
                    visited += 1;
                    skip_exact == Some(id) || merged.offer(id) != Offer::StopGroup
                })?;
                // The group is not kept. Eviction used to rewrite a stale or
                // corrupt sidecar on the way out. Do that here, and skip a
                // sidecar whose stamps already match. A pin keeps its own
                // writer, including the one-block file rule.
                if !pinned {
                    self.repair_plain_id_sidecar_locked(&mut shard);
                }
            }
        }
        self.walk_ids_visited.fetch_add(visited, Ordering::Relaxed);
        Ok(merged.into_vec())
    }

    /// Disk groups for `prefix`, plus every write pin in the collection.
    ///
    /// [`Self::handles_for_prefix`] already unions the warm set. A pin is not
    /// in that set. Docker overlayfs can hide a just-created pin directory
    /// from `readdir`, so the pin list is the source for that group.
    pub(super) fn prefix_page_handles(
        &self,
        collection: &str,
        prefix: &str,
    ) -> Result<Vec<(u16, Option<u32>)>> {
        let mut handles = self.handles_for_prefix(collection, prefix)?;
        handles.extend(self.pin_handles_for(collection));
        handles.sort_unstable();
        handles.dedup();
        Ok(handles)
    }

    /// Physical maintenance count of live keys, without a global sorted merge
    /// or body hydration. Each group is visited once through the same bounded
    /// live/key-cache path as key walks. Concurrent mutations can change later
    /// groups; this is report metadata, not a transactional query result.
    pub fn collection_live_key_count(&self, collection: &str) -> Result<u64> {
        let mut count = 0u64;
        for (shard, group) in self.walk_all_group_handles(collection, AllGroupsPurpose::Admin)? {
            let len = match self.group_key_source(collection, shard, group)? {
                GroupKeySource::Live(h) => {
                    let shard = h.handle().lock().expect("poison");
                    if shard.sorted_segments.is_empty() {
                        shard.index.len()
                    } else {
                        shard.visit_keys("", None, |_, _| true)? as usize
                    }
                }
                GroupKeySource::Cached(keys) => keys.len(),
            };
            count = count
                .checked_add(len as u64)
                .ok_or_else(|| Error::Corrupt("collection key count overflow".into()))?;
        }
        Ok(count)
    }

    /// Keys-only counterpart of [`Self::list_prefix_paged`].
    pub fn list_prefix_keys_paged(
        &self,
        collection: &str,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<String>> {
        self.validate_prefix_read(prefix)?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        // Range start: after exclusive if after is under prefix; else prefix.
        let start = match after {
            Some(a) if a.starts_with(prefix) => {
                // Exclusive: start at the next possible string after `a`.
                // BTreeMap range is inclusive on start, so we skip `a` in the loop.
                a.to_string()
            }
            _ => prefix.to_string(),
        };
        let skip_exact = after.filter(|a| a.starts_with(prefix));
        let mut merged = BoundedMerge::new(limit);
        let mut visited = 0u64;
        for (shard, group) in self.handles_for_prefix(collection, prefix)? {
            match self.group_key_source(collection, shard, group)? {
                GroupKeySource::Live(h) => {
                    let sh = h.handle().lock().expect("poison");
                    sh.visit_keys(&start, None, |id, _| {
                        if !id.starts_with(prefix) {
                            return false;
                        }
                        visited += 1;
                        skip_exact == Some(id) || merged.offer(id) != Offer::StopGroup
                    })?;
                }
                GroupKeySource::Cached(keys) => {
                    visited += collect_prefix(
                        keys.range(start.clone()..),
                        prefix,
                        skip_exact,
                        &mut merged,
                    );
                }
            }
        }
        self.walk_ids_visited.fetch_add(visited, Ordering::Relaxed);
        Ok(merged.into_vec())
    }
}
