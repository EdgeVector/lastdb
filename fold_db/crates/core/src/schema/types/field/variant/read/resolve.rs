use super::*;
pub(super) type TipMatch = (
    KeyValue,
    crate::db_operations::atom_store::PerKeyRecord,
    String,
);

impl FieldVariant {
    /// Resolves field values by refreshing the field, applying filters, and
    /// fetching atom content. See prior docs for tombstone visibility.
    pub async fn resolve_value(
        &mut self,
        db_ops: &Arc<DbOperations>,
        filter: Option<HashRangeFilter>,
        include_tombstones: bool,
    ) -> Result<HashMap<KeyValue, FieldValue>, SchemaError> {
        self.resolve_value_windowed(db_ops, filter, None, include_tombstones)
            .await
    }

    /// [`Self::resolve_value`], but hydrating only the requested `window` of the
    /// matched keys.
    ///
    /// `window` is `(offset, limit)` over the [`KeyValue::cmp_page_order`]
    /// enumeration of the keys this filter matched — applied **after** the
    /// molecule tombstone-flag gate and **before** any atom body is loaded.
    /// That placement is the whole point: key enumeration walks the molecule
    /// tip index, which is cheap and already bounded by the filter, while body
    /// hydration reads, decrypts and materializes an atom per row. Slicing
    /// after hydration (what a key-restricted read did before) pays
    /// `O(partition)` to return `O(page)`, on every page request.
    ///
    /// A window is only meaningful for a filter that selects a set of keys.
    /// `Page` / `PageAfter` already carry their own bounds and must not be
    /// windowed twice, so callers pass `None` for those; this is enforced one
    /// level up, where the filter's shape is known.
    ///
    /// Rows can still drop out after the window is taken — a content tombstone
    /// or an unresolvable atom ref — so a full page in gives at most a full
    /// page out. That is not new: every push-down path can come up short, and
    /// the node reconciles `has_more` against the counted total rather than
    /// against the page length.
    pub async fn resolve_value_windowed(
        &mut self,
        db_ops: &Arc<DbOperations>,
        filter: Option<HashRangeFilter>,
        window: Option<KeyWindow>,
        include_tombstones: bool,
    ) -> Result<HashMap<KeyValue, FieldValue>, SchemaError> {
        self.require_supported_read(filter.as_ref())?;
        use crate::schema::types::key_comparison::normalize;
        let profile = self.read_range_profile(db_ops).await?;
        // A field with no molecule and no active comparison context has no range type.
        let (filter, window) = match profile {
            Some(profile) => (
                normalize::canonical_filter(profile, filter)?,
                normalize::canonical_window(profile, window)?,
            ),
            None => (filter, window),
        };
        crate::schema::types::key_comparison::KeyComparisonContext::run_for_molecule(
            profile,
            captured_field_read(
                db_ops,
                self.resolve_value_windowed_captured(db_ops, filter, window, include_tombstones)
                    .boxed(),
            ),
        )
        .await
    }

    async fn resolve_value_windowed_captured(
        &mut self,
        db_ops: &Arc<DbOperations>,
        filter: Option<HashRangeFilter>,
        window: Option<KeyWindow>,
        include_tombstones: bool,
    ) -> Result<HashMap<KeyValue, FieldValue>, SchemaError> {
        use crate::schema::types::field::fetch_atoms_with_key_metadata_async_with_prefix;

        let core = self
            .resolve_core_model(db_ops, filter.as_ref(), window.clone(), include_tombstones)
            .await?;
        if let Some(values) = core {
            return Ok(values);
        }

        if let Some(resolved) = self
            .resolve_hash_key_fast(db_ops, filter.as_ref(), window.clone(), include_tombstones)
            .await?
        {
            self.record_resolved_tip_keys(&resolved);
            return Ok(resolved);
        }

        let storage_prefix_owned: Option<String> = self.common().storage_prefix.clone();
        let mut matches_with_meta = self
            .collect_matches(db_ops, filter, include_tombstones)
            .await?;

        if let Some(window) = window {
            // `collect_matches` hands back a Vec built from a HashMap, so its
            // order is randomized per call. Sorting is not a nicety here: an
            // unsorted slice would return a different arbitrary subset on every
            // request, so consecutive pages would overlap and drop rows.
            matches_with_meta.sort_by(|(a, ..), (b, ..)| a.cmp_page_order(b));
            matches_with_meta = match window {
                KeyWindow::Offset { offset, limit } => matches_with_meta
                    .into_iter()
                    .skip(offset)
                    .take(limit)
                    .collect(),
                KeyWindow::After { after, limit } => matches_with_meta
                    .into_iter()
                    .filter(|(key, ..)| key.cmp_page_order(&after).is_gt())
                    .take(limit)
                    .collect(),
            };
        }

        let mut resolved = fetch_atoms_with_key_metadata_async_with_prefix(
            db_ops,
            matches_with_meta,
            storage_prefix_owned.as_deref(),
            self.common().molecule_uuid().map(String::as_str),
        )
        .await?;

        // Count content tombstones after the window. Key metadata alone can
        // miss them, so the page's delivered count must retain this signal.
        retain_visible_values(&mut resolved, include_tombstones);

        self.stamp_molecule_meta(&mut resolved);
        self.record_resolved_tip_keys(&resolved);
        Ok(resolved)
    }

    pub(super) async fn resolve_hash_key_fast(
        &self,
        db_ops: &Arc<DbOperations>,
        filter: Option<&HashRangeFilter>,
        window: Option<KeyWindow>,
        include_tombstones: bool,
    ) -> Result<Option<HashMap<KeyValue, FieldValue>>, SchemaError> {
        let Some(filter) = filter else {
            return Ok(None);
        };
        let hash = match filter {
            HashRangeFilter::HashKey(hash) => hash,
            HashRangeFilter::HashRangePrefix { hash, .. }
            | HashRangeFilter::HashRangeRange { hash, .. }
                if self.kind == FieldKind::HashRange && window.is_some() =>
            {
                hash
            }
            _ => return Ok(None),
        };
        let Some(molecule_uuid) = self.common().molecule_uuid().cloned() else {
            return Ok(None);
        };

        let storage_prefix = self.common().storage_prefix();

        // A window that selects nothing selects nothing on every path below, and
        // saying so here keeps the empty case from depending on which branch a
        // field kind happens to take.
        if matches!(
            window,
            Some(KeyWindow::Offset { limit: 0, .. } | KeyWindow::After { limit: 0, .. })
        ) {
            return Ok(Some(HashMap::new()));
        }

        let (matches, window_applied) = match self.kind {
            FieldKind::Hash => (
                Self::hash_field_tip_match(db_ops, &molecule_uuid, hash, storage_prefix).await?,
                false,
            ),
            FieldKind::HashRange => {
                Self::hash_range_tip_matches(
                    db_ops,
                    &molecule_uuid,
                    filter,
                    storage_prefix,
                    &window,
                    include_tombstones,
                )
                .await?
            }
            _ => return Ok(None),
        };

        let visible_matches =
            Self::page_visible_matches(matches, window, window_applied, include_tombstones);
        if visible_matches.is_empty() {
            return Ok(Some(HashMap::new()));
        }
        Self::materialize_tip_matches(
            db_ops,
            &molecule_uuid,
            storage_prefix,
            include_tombstones,
            visible_matches,
        )
        .await
        .map(Some)
    }

    /// Tip record for a single `Hash` field key (empty when absent).
    pub(super) async fn hash_field_tip_match(
        db_ops: &Arc<DbOperations>,
        molecule_uuid: &str,
        hash: &str,
        storage_prefix: Option<&str>,
    ) -> Result<Vec<TipMatch>, SchemaError> {
        let hit = db_ops
            .atoms()
            .get_query_hash_tip(molecule_uuid, hash, storage_prefix)
            .await?;
        Ok(hit
            .into_iter()
            .map(|(base, record)| (KeyValue::new(Some(hash.to_string()), None), record, base))
            .collect())
    }

    /// Tip records for a selector under one hash, plus whether the window was
    /// already applied while selecting them.
    pub(super) async fn hash_range_tip_matches(
        db_ops: &Arc<DbOperations>,
        molecule_uuid: &str,
        filter: &HashRangeFilter,
        storage_prefix: Option<&str>,
        window: &Option<KeyWindow>,
        include_tombstones: bool,
    ) -> Result<(Vec<TipMatch>, bool), SchemaError> {
        Self::scan_hash_range_matches(
            db_ops,
            molecule_uuid,
            filter,
            storage_prefix,
            window,
            include_tombstones,
        )
        .await
    }

    /// Stamp stable molecule identity; keyed fields retain the selected writer.
    /// Partial molecule counters describe the read's materialization, not the
    /// selected logical record, so query metadata does not expose them.
    pub(crate) fn stamp_molecule_meta(&self, resolved: &mut HashMap<KeyValue, FieldValue>) {
        let mol_uuid = self.common().molecule_uuid().cloned();
        let mol_writer_pubkey = self.molecule_writer_pubkey();
        for fv in resolved.values_mut() {
            fv.molecule_uuid = mol_uuid.clone();
            if let Some(pk) = mol_writer_pubkey.clone() {
                fv.writer_pubkey = Some(pk);
            }
        }
    }

    pub(super) fn entry_meta_at(
        &self,
        kv: &KeyValue,
    ) -> (Option<crate::atom::KeyMetadata>, Option<String>) {
        (
            self.key_metadata_at_key(kv).cloned(),
            self.atom_entry_at_key(kv)
                .map(|entry| entry.writer_pubkey().to_string()),
        )
    }
}
