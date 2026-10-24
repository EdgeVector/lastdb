//! Bounded raw-key union, existing namespace winners, then the output window.

use super::primary_selection::{PrimaryRequest, PrimarySelection};
use super::*;
use crate::schema::types::field::FieldKind;
use crate::schema::types::FieldVariant;

const CANDIDATE_BATCH_LIMIT: usize = 256;
const SOURCE_READ_CONCURRENCY: usize = 4;
const CACHED_SOURCE_PAGES: usize = 4;

mod candidates;

type CandidateTip = (
    KeyValue,
    crate::db_operations::atom_store::PerKeyRecord,
    String,
);

#[derive(Default)]
struct CandidatePage {
    tips: Vec<CandidateTip>,
    exhausted: bool,
}

#[derive(Default)]
pub(super) struct CandidatePages {
    pages: [Option<CandidatePage>; CACHED_SOURCE_PAGES],
    after: Option<KeyValue>,
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
        let mut round = 0usize;
        while state.selected.values.len() < state.limit {
            round += 1;
            let limit = state.batch_limit();
            let (keys, exhausted) = self
                .received_candidate_keys(
                    field,
                    request,
                    filter,
                    state.after.clone(),
                    limit,
                    &mut discovery,
                )
                .await
                .map_err(|error| {
                    received_resource_error(
                        error,
                        "discovery",
                        round,
                        limit,
                        state.selected.values.len(),
                    )
                })?;
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
                discovery: Some(&discovery),
            };
            let batch = crate::db_operations::with_refilled_query_row_drops(
                self.resolve_primary_complete(schema, field, &exact),
            )
            .await
            .map_err(|error| {
                received_resource_error(
                    error,
                    "resolution",
                    round,
                    keys.len(),
                    state.selected.values.len(),
                )
            })?;
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
        discovery.after.clone_from(&after);
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

    /// The cached page must prove every requested key before it can replace
    /// the existing exact-key resolver. Receipts and body visibility use the
    /// normal field reader; a received scope retains its preload lifecycle.
    pub(super) async fn resolve_cached_primary_values(
        &self,
        field: &FieldVariant,
        request: &PrimaryRequest<'_>,
        filter: Option<&HashRangeFilter>,
        position: usize,
        namespace: Option<&str>,
    ) -> Result<Option<HashMap<KeyValue, FieldValue>>, SchemaError> {
        let Some((discovery, HashRangeFilter::HashRangeKeys(keys))) = request.discovery.zip(filter)
        else {
            return Ok(None);
        };
        let Some(tips) = discovery.selected_tips(position, keys) else {
            return Ok(None);
        };
        if position != 0 {
            if let Some(runtime) = self.db_ops.namespaced_store().disk_pack_runtime() {
                runtime
                    .preload_current_field(Arc::clone(&self.db_ops), field, namespace)
                    .await?;
            }
        }
        let mut source = field.cloned_without_molecule();
        source
            .common_mut()
            .set_storage_prefix(namespace.map(str::to_owned));
        source
            .materialize_hash_range_candidates(&self.db_ops, tips, request.include_tombstones)
            .await
            .map(Some)
    }
}

fn received_resource_error(
    error: SchemaError,
    phase: &'static str,
    round: usize,
    candidates: usize,
    accepted: usize,
) -> SchemaError {
    match error {
        SchemaError::ReadResourceLimit { detail } => SchemaError::ReadResourceLimit {
            detail: format!(
                "{detail}; received phase={phase}, round={round}, candidates={candidates}, accepted={accepted}"
            ),
        },
        error => error,
    }
}
