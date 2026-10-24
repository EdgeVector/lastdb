//! Bounded raw-key union, existing namespace winners, then the output window.

use super::primary_selection::{PrimaryRequest, PrimarySelection};
use super::*;
use crate::schema::types::field::FieldKind;
use crate::schema::types::FieldVariant;

const CANDIDATE_BATCH_LIMIT: usize = 256;
const SOURCE_READ_CONCURRENCY: usize = 4;
const CACHED_SOURCE_PAGES: usize = 4;

#[derive(Default)]
struct CandidatePage {
    keys: Vec<KeyValue>,
    exhausted: bool,
}

#[derive(Default)]
struct CandidatePages {
    pages: [Option<CandidatePage>; CACHED_SOURCE_PAGES],
}

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
        let mut discovery = CandidatePages::default();
        while state.selected.values.len() < state.limit {
            let (keys, exhausted) = self
                .received_candidate_keys(
                    field,
                    request,
                    filter,
                    state.after.clone(),
                    state.batch_limit(),
                    &mut discovery,
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
        discovery: &mut CandidatePages,
    ) -> Result<(Vec<KeyValue>, bool), SchemaError> {
        let mut retained = std::mem::take(&mut discovery.pages);
        let namespaces = std::iter::once(request.own_prefix.clone())
            .chain(request.received.iter().cloned().map(Some));
        let pages = stream::iter(namespaces)
            .enumerate()
            .map(|(position, namespace)| {
                let mut source = field.cloned_without_molecule();
                source.common_mut().set_storage_prefix(namespace);
                let after = after.clone();
                let cached = retained
                    .get_mut(position)
                    .map(|page| page.take().unwrap_or_default());
                async move {
                    self.received_candidate_source(&source, filter, after, limit, cached)
                        .await
                        .map(|page| (position, page))
                }
            })
            .buffered(SOURCE_READ_CONCURRENCY);
        futures::pin_mut!(pages);
        let mut keys = Vec::new();
        let mut exhausted = true;
        while let Some(page) = pages.next().await {
            let (position, (source_keys, source_exhausted, cached)) = page?;
            if let Some(cached) = cached {
                discovery.pages[position] = Some(cached);
            }
            exhausted &= source_exhausted;
            keys.extend(source_keys);
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

    /// Only four source pages survive a refill round. Other sources retain
    /// their bounded reread behavior, so temporary memory stays independent
    /// of the number of received namespaces.
    async fn received_candidate_source(
        &self,
        field: &FieldVariant,
        filter: &HashRangeFilter,
        after: Option<KeyValue>,
        limit: usize,
        cached: Option<CandidatePage>,
    ) -> Result<(Vec<KeyValue>, bool, Option<CandidatePage>), SchemaError> {
        let Some(mut cached) = cached else {
            let page = field
                .hash_range_candidate_page(&self.db_ops, filter, after, limit)
                .await?;
            return Ok((page.keys, page.exhausted, None));
        };
        if let Some(after) = &after {
            cached.keys.retain(|key| key.cmp_page_order(after).is_gt());
        }
        while cached.keys.len() < limit && !cached.exhausted {
            let cursor = cached.keys.last().cloned().or_else(|| after.clone());
            let page = field
                .hash_range_candidate_page(
                    &self.db_ops,
                    filter,
                    cursor.clone(),
                    limit - cached.keys.len(),
                )
                .await?;
            if (page.keys.is_empty() && !page.exhausted)
                || page.keys.last().is_some_and(|key| {
                    cursor
                        .as_ref()
                        .is_some_and(|cursor| !key.cmp_page_order(cursor).is_gt())
                })
            {
                return Err(SchemaError::InvalidData(
                    "cached received candidate page did not advance".into(),
                ));
            }
            cached.exhausted = page.exhausted;
            cached.keys.extend(page.keys);
        }
        let keys = cached.keys.iter().take(limit).cloned().collect();
        let exhausted = cached.exhausted && cached.keys.len() <= limit;
        Ok((keys, exhausted, Some(cached)))
    }
}
