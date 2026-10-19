//! Select a named layout before its result window, with bounded body batches.

use super::*;
use crate::schema::types::field::fetch_atoms_with_key_metadata_async_with_prefix;

struct NamedWindow {
    skip: usize,
    limit: usize,
    after: Option<KeyValue>,
    values: HashMap<KeyValue, FieldValue>,
    had_spine: bool,
    refilling: bool,
}

impl NamedWindow {
    fn new(window: KeyWindow) -> Self {
        let (skip, limit, after) = match window {
            KeyWindow::Offset { offset, limit } => (offset, limit, None),
            KeyWindow::After { after, limit } => (0, limit, Some(after)),
        };
        Self {
            skip,
            limit,
            after,
            values: HashMap::new(),
            had_spine: false,
            refilling: false,
        }
    }

    fn ask(&self) -> usize {
        let remaining = self
            .skip
            .saturating_add(self.limit.saturating_sub(self.values.len()))
            .clamp(1, 1024);
        if self.refilling {
            remaining.max(64)
        } else {
            remaining
        }
    }

    fn accept(&mut self, keys: Vec<KeyValue>, mut values: HashMap<KeyValue, FieldValue>) {
        for key in keys {
            let Some(value) = values.remove(&key) else {
                continue;
            };
            if !key.hash_matches_scalar(&value.value) {
                continue;
            }
            if self.skip > 0 {
                self.skip -= 1;
            } else if self.values.len() < self.limit {
                self.values.insert(key, value);
            }
        }
        self.refilling = true;
    }
}

impl FieldVariant {
    /// The caller holds its field binding and captured memory/disk read scope.
    /// The presence flag comes from selected tips, before hydration or slicing.
    pub(crate) async fn resolve_named_window(
        &mut self,
        db_ops: &Arc<DbOperations>,
        filter: &HashRangeFilter,
        window: KeyWindow,
        include_tombstones: bool,
    ) -> Result<(HashMap<KeyValue, FieldValue>, bool), SchemaError> {
        self.require_supported_read(Some(filter))?;
        let mut selection = NamedWindow::new(window);
        if selection.limit == 0 {
            return Ok((selection.values, false));
        }
        if let (FieldKind::HashRange, HashRangeFilter::HashKey(hash)) = (self.kind, filter) {
            self.resolve_named_hash_pages(db_ops, hash, include_tombstones, &mut selection)
                .await?;
        } else {
            self.resolve_named_tip_batches(db_ops, filter, include_tombstones, &mut selection)
                .await?;
        }
        self.stamp_molecule_meta(&mut selection.values);
        self.record_resolved_tip_keys(&selection.values);
        Ok((selection.values, selection.had_spine))
    }

    async fn resolve_named_hash_pages(
        &self,
        db_ops: &Arc<DbOperations>,
        hash: &str,
        include_tombstones: bool,
        selection: &mut NamedWindow,
    ) -> Result<(), SchemaError> {
        let Some(molecule) = self.common().molecule_uuid() else {
            return Ok(());
        };
        while selection.values.len() < selection.limit {
            let ask = selection.ask();
            let window = selection.after.clone().map_or_else(
                || KeyWindow::offset(0, ask),
                |after| KeyWindow::after(after, ask),
            );
            let (rows, window_applied) = Self::hash_range_tip_matches(
                db_ops,
                molecule,
                hash,
                self.common().storage_prefix(),
                &Some(window.clone()),
                include_tombstones,
            )
            .await?;
            let rows =
                Self::page_visible_matches(rows, Some(window), window_applied, include_tombstones);
            selection.had_spine |= !rows.is_empty();
            let exhausted = rows.len() < ask;
            let keys: Vec<_> = rows.iter().map(|(key, ..)| key.clone()).collect();
            let frontier = keys.last().cloned();
            let values = Self::materialize_tip_matches(
                db_ops,
                molecule,
                self.common().storage_prefix(),
                include_tombstones,
                rows,
            )
            .await?;
            selection.accept(keys, values);
            let Some(frontier) = frontier else { break };
            if selection
                .after
                .as_ref()
                .is_some_and(|after| !frontier.cmp_page_order(after).is_gt())
            {
                return Err(SchemaError::InvalidData(
                    "named-layout tip page did not advance".into(),
                ));
            }
            selection.after = Some(frontier);
            if exhausted {
                break;
            }
        }
        Ok(())
    }

    async fn resolve_named_tip_batches(
        &mut self,
        db_ops: &Arc<DbOperations>,
        filter: &HashRangeFilter,
        include_tombstones: bool,
        selection: &mut NamedWindow,
    ) -> Result<(), SchemaError> {
        let mut tips = self
            .collect_matches(db_ops, Some(filter.clone()), include_tombstones)
            .await?;
        selection.had_spine = !tips.is_empty();
        tips.sort_by(|(left, ..), (right, ..)| left.cmp_page_order(right));
        tips.dedup_by(|(left, ..), (right, ..)| left == right);
        if let Some(after) = &selection.after {
            tips.retain(|(key, ..)| key.cmp_page_order(after).is_gt());
        }
        let mut tips = tips.into_iter();
        while selection.values.len() < selection.limit {
            let batch: Vec<_> = tips.by_ref().take(selection.ask()).collect();
            if batch.is_empty() {
                break;
            }
            let keys = batch.iter().map(|(key, ..)| key.clone()).collect();
            let mut values = fetch_atoms_with_key_metadata_async_with_prefix(
                db_ops,
                batch,
                self.common().storage_prefix(),
                self.common().molecule_uuid().map(String::as_str),
                None,
                None,
            )
            .await?;
            retain_visible_values(&mut values, include_tombstones);
            selection.accept(keys, values);
        }
        Ok(())
    }

    /// Only an actual empty tip set permits a projection to establish the keys.
    pub(crate) async fn has_primary_tips(
        &self,
        db_ops: &Arc<DbOperations>,
        filter: Option<&HashRangeFilter>,
        include_tombstones: bool,
    ) -> Result<bool, SchemaError> {
        if let (FieldKind::HashRange, Some(HashRangeFilter::HashKey(hash)), Some(molecule)) =
            (self.kind, filter, self.common().molecule_uuid())
        {
            let (tips, _) = Self::hash_range_tip_matches(
                db_ops,
                molecule,
                hash,
                self.common().storage_prefix(),
                &Some(KeyWindow::offset(0, 1)),
                include_tombstones,
            )
            .await?;
            return Ok(tips.iter().any(|(_, record, _)| {
                include_tombstones || !record.meta.as_ref().is_some_and(|meta| meta.tombstoned)
            }));
        }
        let probe = match filter {
            Some(HashRangeFilter::Page { .. }) => Some(HashRangeFilter::Page {
                offset: 0,
                limit: 1,
            }),
            _ => filter.cloned(),
        };
        Ok(!self
            .cloned_without_molecule()
            .collect_matches(db_ops, probe, include_tombstones)
            .await?
            .is_empty())
    }
}
