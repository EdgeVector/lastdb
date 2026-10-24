//! Bounded raw-key union, existing namespace winners, then the output window.

use super::primary_selection::{PrimaryRequest, PrimarySelection};
use super::*;
use crate::schema::types::field::FieldKind;
use crate::schema::types::FieldVariant;

const CANDIDATE_BATCH_LIMIT: usize = 256;
const SOURCE_READ_CONCURRENCY: usize = 4;

struct ReceivedWindow {
    after: Option<KeyValue>,
    skip: usize,
    limit: usize,
    refill: bool,
    selected: PrimarySelection,
}

impl ReceivedWindow {
    fn new(window: &KeyWindow, own_prefix: &KeySource) -> Self {
        let (after, skip, limit) = match window {
            KeyWindow::Offset { offset, limit } => (None, *offset, *limit),
            KeyWindow::After { after, limit } => (Some(after.clone()), 0, *limit),
        };
        Self {
            after,
            skip,
            limit,
            refill: false,
            selected: PrimarySelection::new(HashMap::new(), own_prefix),
        }
    }

    fn batch_limit(&self) -> usize {
        let needed = self
            .skip
            .saturating_add(self.limit.saturating_sub(self.selected.values.len()));
        needed.clamp(if self.refill { 64 } else { 1 }, CANDIDATE_BATCH_LIMIT)
    }

    fn accept(&mut self, keys: Vec<KeyValue>, mut batch: PrimarySelection) {
        self.selected.had_spine |= batch.had_spine;
        let visible = batch.values.len();
        self.refill = visible < keys.len();
        for key in keys {
            let Some(value) = batch.values.remove(&key) else {
                continue;
            };
            if self.skip > 0 {
                self.skip -= 1;
            } else if self.selected.values.len() < self.limit {
                if let Some(source) = batch.sources.remove(&key) {
                    self.selected.sources.insert(key.clone(), source);
                }
                self.selected.values.insert(key, value);
            }
        }
    }
}

impl HashRangeQueryProcessor {
    pub(super) fn can_page_received(field: &FieldVariant, request: &PrimaryRequest<'_>) -> bool {
        !request.received.is_empty()
            && field.kind == FieldKind::HashRange
            && request.window.is_some()
            && matches!(
                request.filter,
                Some(
                    HashRangeFilter::HashKey(_)
                        | HashRangeFilter::HashRangePrefix { .. }
                        | HashRangeFilter::HashRangeRange { .. }
                )
            )
    }

    pub(super) async fn resolve_received_window(
        &self,
        schema: &Schema,
        field: &mut FieldVariant,
        request: &PrimaryRequest<'_>,
    ) -> Result<PrimarySelection, SchemaError> {
        let window = request.window.as_ref().ok_or_else(|| {
            SchemaError::InvalidData("received page requires an output window".into())
        })?;
        let filter = request.filter.as_ref().ok_or_else(|| {
            SchemaError::InvalidData("received page requires one hash selector".into())
        })?;
        let mut state = ReceivedWindow::new(window, &request.own_prefix);
        while state.selected.values.len() < state.limit {
            let (keys, exhausted) = self
                .received_candidate_keys(
                    field,
                    request,
                    filter,
                    state.after.clone(),
                    state.batch_limit(),
                )
                .await?;
            let Some(frontier) = keys.last().cloned() else {
                return Ok(state.selected);
            };
            if state
                .after
                .as_ref()
                .is_some_and(|after| !frontier.cmp_page_order(after).is_gt())
            {
                return Err(SchemaError::InvalidData(
                    "received candidate frontier did not advance".into(),
                ));
            }
            let exact = PrimaryRequest {
                name: request.name,
                filter: Some(HashRangeFilter::HashRangeKeys(
                    keys.iter()
                        .map(|key| {
                            (
                                key.hash.clone().unwrap_or_default(),
                                key.range.clone().unwrap_or_default(),
                            )
                        })
                        .collect(),
                )),
                window: None,
                include_tombstones: request.include_tombstones,
                received: request.received,
                own_prefix: request.own_prefix.clone(),
                named_layout: request.named_layout,
            };
            let batch = crate::db_operations::with_refilled_query_row_drops(
                self.resolve_primary_complete(schema, field, &exact),
            )
            .await?;
            state.accept(keys, batch);
            state.after = Some(frontier);
            if exhausted {
                break;
            }
        }
        Ok(state.selected)
    }

    /// Every source contributes its first B keys. Their union contains the
    /// first B global keys: any omitted source key has B earlier source keys.
    /// Keep only B keys after each merge, so memory does not grow with sources.
    async fn received_candidate_keys(
        &self,
        field: &FieldVariant,
        request: &PrimaryRequest<'_>,
        filter: &HashRangeFilter,
        after: Option<KeyValue>,
        limit: usize,
    ) -> Result<(Vec<KeyValue>, bool), SchemaError> {
        let namespaces = std::iter::once(request.own_prefix.clone())
            .chain(request.received.iter().cloned().map(Some));
        let pages = stream::iter(namespaces)
            .map(|namespace| {
                let mut source = field.cloned_without_molecule();
                source.common_mut().set_storage_prefix(namespace);
                let after = after.clone();
                async move {
                    source
                        .hash_range_candidate_page(&self.db_ops, filter, after, limit)
                        .await
                }
            })
            .buffered(SOURCE_READ_CONCURRENCY);
        futures::pin_mut!(pages);
        let mut keys = Vec::new();
        let mut exhausted = true;
        while let Some(page) = pages.next().await {
            let page = page?;
            exhausted &= page.exhausted;
            keys.extend(page.keys);
            keys.sort_by(KeyValue::cmp_page_order);
            keys.dedup();
            if keys.len() > limit {
                // Even exhausted source pages can have unconsumed union keys.
                exhausted = false;
                keys.truncate(limit);
            }
        }
        Ok((keys, exhausted))
    }
}
