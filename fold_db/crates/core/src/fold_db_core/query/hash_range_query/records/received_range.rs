//! A range read under one hash that merges the own scope with the received
//! scopes. A node with no sender reads the own scope alone through the same
//! pages.
//!
//! Each scope reads its record keys one bounded page at a time. The first `n`
//! distinct keys of the merged order are among the first `n` keys of each
//! scope, so one merge step reads up to `n` keys from each scope, keeps the
//! first `n` distinct keys, and reads only the records at those keys, in the
//! scopes that hold them. The step size follows the window, and the read ends
//! when the window has its rows. A long range costs its window, not its
//! length.

use super::has_any;
use super::received::Candidate;
use crate::db_operations::atom_store::{AtomStore, KeyedRecord, RecordBound, RecordSpan, SpanKey};
use crate::fold_db_core::query::hash_range_query::HashRangeQueryProcessor;
use crate::schema::types::field::KeyWindow;
use crate::schema::types::{KeyValue, KeyValuePageOrder};
use crate::schema::{Schema, SchemaError};
use std::collections::{BTreeMap, VecDeque};

/// The smallest and the largest number of keys one merge step reads from
/// each scope.
const MIN_STEP: usize = 32;
const MAX_STEP: usize = 1024;

/// The unread keys of one scope.
struct ScopeKeys {
    span: Option<RecordSpan>,
    keys: VecDeque<SpanKey>,
    cursor: Option<String>,
    done: bool,
}

impl ScopeKeys {
    fn new(span: Option<RecordSpan>) -> Self {
        Self {
            done: span.is_none(),
            span,
            keys: VecDeque::new(),
            cursor: None,
        }
    }

    /// Hold at least `wanted` keys, or every key that remains.
    async fn fill(&mut self, atoms: &AtomStore, wanted: usize) -> Result<(), SchemaError> {
        let Some(span) = self.span.as_ref().filter(|_| !self.done) else {
            return Ok(());
        };
        if self.keys.len() >= wanted {
            return Ok(());
        }
        let (keys, next) = atoms
            .record_span_page(span, self.cursor.as_deref(), wanted - self.keys.len())
            .await?;
        self.keys.extend(keys);
        self.done = next.is_none();
        self.cursor = next;
        Ok(())
    }
}

impl HashRangeQueryProcessor {
    /// The rows of one range under one hash over `scopes`, in page order and
    /// bounded by the window.
    pub(super) async fn received_range(
        &self,
        schema: &Schema,
        hash: &str,
        bound: RecordBound<'_>,
        scopes: &[Option<String>],
        selected: &[String],
        window: Option<KeyWindow>,
    ) -> Result<Vec<KeyedRecord>, SchemaError> {
        let (after, mut skip, limit) = match window {
            None => (None, 0, usize::MAX),
            Some(KeyWindow::Offset { offset, limit }) => (None, offset, limit),
            Some(KeyWindow::After { after, limit }) => (Some(after), 0, limit),
        };
        let atoms = self.db_ops.atoms();
        let from_range = after.as_ref().and_then(|after| after.range.as_deref());
        let mut sources = Vec::with_capacity(scopes.len());
        for scope in scopes {
            let span = atoms.record_span(schema, hash, bound, from_range, scope.as_deref())?;
            sources.push(ScopeKeys::new(span));
        }
        let mut rows = Vec::new();
        while rows.len() < limit {
            let step = skip
                .saturating_add(limit - rows.len())
                .clamp(MIN_STEP, MAX_STEP);
            futures::future::try_join_all(
                sources.iter_mut().map(|source| source.fill(atoms, step)),
            )
            .await?;
            let candidates = next_keys(&mut sources, step);
            if candidates.is_empty() {
                break;
            }
            for (key, record) in self.received_winners(candidates).await? {
                // A cursor of another hash can tie on the range.
                if after
                    .as_ref()
                    .is_some_and(|after| key.cmp_page_order(after).is_le())
                    || !has_any(&record, selected)
                {
                    continue;
                }
                if skip > 0 {
                    skip -= 1;
                    continue;
                }
                rows.push((key, record));
                if rows.len() == limit {
                    break;
                }
            }
        }
        Ok(rows)
    }
}

/// The first `step` distinct keys of the merged order, each with the physical
/// keys of the scopes that hold it, in scope order. Each scope that is not
/// done holds at least `step` keys, so no unread key comes before the last
/// key taken. The taken keys leave the scopes.
fn next_keys(sources: &mut [ScopeKeys], step: usize) -> Vec<Candidate> {
    let mut merged: BTreeMap<&str, (&KeyValue, Vec<String>)> = BTreeMap::new();
    for source in sources.iter() {
        for key in &source.keys {
            merged
                .entry(key.range.as_str())
                .or_insert_with(|| (&key.key, Vec::new()))
                .1
                .push(key.physical.clone());
        }
    }
    let mut last = None;
    let mut candidates = Vec::with_capacity(step.min(merged.len()));
    for (range, (key, physical)) in merged.into_iter().take(step) {
        last = Some(range.to_owned());
        candidates.push((key.clone(), physical));
    }
    let Some(last) = last else {
        return candidates;
    };
    for source in sources.iter_mut() {
        while source.keys.front().is_some_and(|key| key.range <= last) {
            source.keys.pop_front();
        }
    }
    candidates
}
