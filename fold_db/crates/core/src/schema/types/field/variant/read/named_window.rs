//! Select a named layout before its result window, with bounded body batches.

use super::*;
use crate::schema::types::field::fetch_atoms_with_key_metadata_async_with_prefix;

/// Query semantics can require another declared coordinate's field value.
/// Owned batches avoid a borrow across the next physical tip read.
type FieldValues = HashMap<KeyValue, FieldValue>;
type Membership<'a> = dyn FnMut(FieldValues) -> BoxFuture<'a, Result<(FieldValues, FieldValues), SchemaError>>
    + Send
    + 'a;

struct NamedWindow {
    skip: usize,
    limit: usize,
    after: Option<KeyValue>,
    values: HashMap<KeyValue, FieldValue>,
    layout_values: FieldValues,
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
            layout_values: HashMap::new(),
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

    fn accept(
        &mut self,
        keys: Vec<KeyValue>,
        (mut values, mut layout_values): (FieldValues, FieldValues),
    ) {
        for key in keys {
            let Some(value) = values.remove(&key) else {
                continue;
            };
            if self.skip > 0 {
                self.skip -= 1;
            } else if self.values.len() < self.limit {
                if let Some(layout_value) = layout_values.remove(&key) {
                    self.layout_values.insert(key.clone(), layout_value);
                }
                self.values.insert(key, value);
            }
        }
        self.refilling = true;
    }
}

impl FieldVariant {
    /// The caller holds its field binding and captured memory/disk read scope.
    /// The presence flag comes from selected tips, before hydration or slicing.
    pub(crate) async fn resolve_named_window<'a>(
        &mut self,
        db_ops: &Arc<DbOperations>,
        filter: &HashRangeFilter,
        window: KeyWindow,
        include_tombstones: bool,
        membership: &mut Membership<'a>,
    ) -> Result<(FieldValues, FieldValues, bool), SchemaError> {
        self.require_supported_read(Some(filter))?;
        let mut selection = NamedWindow::new(window);
        if selection.limit == 0 {
            return Ok((selection.values, selection.layout_values, false));
        }
        if self.kind == FieldKind::HashRange
            && matches!(
                filter,
                HashRangeFilter::HashKey(_)
                    | HashRangeFilter::HashRangePrefix { .. }
                    | HashRangeFilter::HashRangeRange { .. }
            )
        {
            self.resolve_named_hash_pages(
                db_ops,
                filter,
                include_tombstones,
                &mut selection,
                membership,
            )
            .await?;
        } else {
            self.resolve_named_tip_batches(
                db_ops,
                filter,
                include_tombstones,
                &mut selection,
                membership,
            )
            .await?;
        }
        self.stamp_molecule_meta(&mut selection.values);
        self.record_resolved_tip_keys(&selection.values);
        Ok((
            selection.values,
            selection.layout_values,
            selection.had_spine,
        ))
    }

    async fn resolve_named_hash_pages<'a>(
        &self,
        db_ops: &Arc<DbOperations>,
        filter: &HashRangeFilter,
        include_tombstones: bool,
        selection: &mut NamedWindow,
        membership: &mut Membership<'a>,
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
                filter,
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
            selection.accept(keys, membership(values).await?);
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

    async fn resolve_named_tip_batches<'a>(
        &mut self,
        db_ops: &Arc<DbOperations>,
        filter: &HashRangeFilter,
        include_tombstones: bool,
        selection: &mut NamedWindow,
        membership: &mut Membership<'a>,
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
            )
            .await?;
            retain_visible_values(&mut values, include_tombstones);
            selection.accept(keys, membership(values).await?);
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
        if let (
            FieldKind::HashRange,
            Some(
                filter @ (HashRangeFilter::HashKey(_)
                | HashRangeFilter::HashRangePrefix { .. }
                | HashRangeFilter::HashRangeRange { .. }),
            ),
            Some(molecule),
        ) = (self.kind, filter, self.common().molecule_uuid())
        {
            let (tips, _) = Self::hash_range_tip_matches(
                db_ops,
                molecule,
                filter,
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
