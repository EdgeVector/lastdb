//! Exact public keys bound the shortcut before any logical body copy.

use super::*;
use crate::schema::types::field::KeyWindow;

const MAX_KEY_BYTES: usize = 4 * 1024 * 1024;

pub(super) fn keys(query: &Query) -> Option<Vec<KeyValue>> {
    if query.fields.is_empty()
        || query.as_of.is_some()
        || query.rehydrate_depth.is_some()
        || query.include_tombstones
        || query.expected_total_count.is_some()
        || query.value_filters.is_some()
        || query.field_predicates.is_some()
        || query.order_by.is_some()
        || query.predicate_limit.is_some()
    {
        return None;
    }
    let borrowed: Vec<_> = match query.filter.as_ref()? {
        HashRangeFilter::HashRangeKey { hash, range } => vec![(Some(hash), Some(range))],
        HashRangeFilter::HashRangeKeys(keys) if keys.len() <= 10_000 => keys
            .iter()
            .map(|(hash, range)| (Some(hash), Some(range)))
            .collect(),
        // A real multi-range group returns two-component public keys, so its
        // result cannot satisfy these exact one-component aliases.
        HashRangeFilter::HashKey(hash) => vec![(Some(hash), None)],
        HashRangeFilter::RangeKey(range) => vec![(None, Some(range))],
        _ => return None,
    };
    let bytes = borrowed.iter().try_fold(0usize, |total, (hash, range)| {
        total
            .checked_add(hash.map_or(0, String::len))?
            .checked_add(range.map_or(0, String::len))
    })?;
    if borrowed.is_empty() || bytes > MAX_KEY_BYTES {
        return None;
    }
    let mut keys: Vec<_> = borrowed
        .into_iter()
        .map(|(hash, range)| KeyValue::new(hash.cloned(), range.cloned()))
        .collect();
    keys.sort_by(KeyValue::cmp_page_order);
    keys.dedup();
    (query.fields.len().checked_mul(keys.len())? <= 10_000).then_some(keys)
}

pub(crate) fn covers_window(query: &Query, window: Option<&KeyWindow>) -> bool {
    let Some(keys) = keys(query) else {
        return false;
    };
    match window {
        None => true,
        Some(KeyWindow::Offset { offset: 0, limit }) => *limit >= keys.len(),
        _ => false,
    }
}
