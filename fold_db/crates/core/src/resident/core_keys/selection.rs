//! Bounded ordered candidate selection without full tip or atom values.

use super::{
    CompactRangeKey, CoreKeyDomain, CoreKeyError, CoreKeyLease, CoreStoredKey, RangeKeyEncoding,
};
use std::ops::Bound;

const MAX_PAGE_KEYS: usize = 4096;

#[derive(Clone, Copy)]
pub(crate) struct CoreKeyRange<'a> {
    pub(crate) hash: &'a str,
    pub(crate) start: Bound<&'a str>,
    pub(crate) end: Bound<&'a str>,
    pub(crate) after: Option<&'a str>,
    pub(crate) prefix: Option<&'a str>,
    pub(crate) limit: usize,
    pub(crate) examined_cap: usize,
    pub(crate) include_tombstones: bool,
}

#[derive(Debug, Default)]
pub(crate) struct CoreKeyPage {
    pub(crate) keys: Vec<CoreStoredKey>,
    pub(crate) exhausted: bool,
    pub(crate) last_examined: Option<String>,
    pub(crate) examined: usize,
}

impl CoreKeyRange<'_> {
    pub(crate) fn all(hash: &str, limit: usize) -> CoreKeyRange<'_> {
        CoreKeyRange {
            hash,
            start: Bound::Unbounded,
            end: Bound::Unbounded,
            after: None,
            prefix: None,
            limit,
            examined_cap: MAX_PAGE_KEYS,
            include_tombstones: false,
        }
    }
}

impl CoreKeyDomain {
    /// Loading access is reserved for the private builder. Query callers use
    /// a captured lease; old leases remain charged after domain retirement.
    pub(crate) fn select(&self, request: CoreKeyRange<'_>) -> Result<CoreKeyPage, CoreKeyError> {
        if request.limit == 0
            || request.limit > MAX_PAGE_KEYS
            || request.examined_cap == 0
            || request.examined_cap > MAX_PAGE_KEYS
        {
            return Err(CoreKeyError::PageLimit);
        }
        let mut start = encode_bound(request.start, self.range_encoding);
        let end = encode_bound(request.end, self.range_encoding);
        if let Some(prefix) = request.prefix {
            raise_start(
                &mut start,
                CompactRangeKey::from_storage(prefix, self.range_encoding),
                false,
            );
        }
        if let Some(after) = request.after {
            raise_start(
                &mut start,
                CompactRangeKey::from_storage(after, self.range_encoding),
                true,
            );
        }
        if empty_bounds(&start, &end) {
            return Ok(CoreKeyPage {
                exhausted: true,
                ..CoreKeyPage::default()
            });
        }
        let state = self.state.lock().map_err(|_| CoreKeyError::Poisoned)?;
        let mut ranges = state.keys.ranges(request.hash, start, end).peekable();
        let mut page = CoreKeyPage {
            keys: Vec::with_capacity(request.limit),
            ..CoreKeyPage::default()
        };
        while page.keys.len() < request.limit && page.examined < request.examined_cap {
            let Some((range, visibility)) = ranges.next() else {
                break;
            };
            if request
                .prefix
                .is_some_and(|prefix| !range.starts_with(prefix))
            {
                page.exhausted = true;
                return Ok(page);
            }
            page.examined += 1;
            let stored = range.to_storage();
            page.last_examined = Some(stored.clone());
            if request.include_tombstones || !visibility.tombstoned() {
                page.keys.push(CoreStoredKey {
                    hash: request.hash.to_string(),
                    range: stored,
                });
            }
        }
        page.exhausted = ranges.peek().is_none_or(|(range, _)| {
            request
                .prefix
                .is_some_and(|prefix| !range.starts_with(prefix))
        });
        Ok(page)
    }
}

impl CoreKeyLease {
    pub(crate) fn select(&self, request: CoreKeyRange<'_>) -> Result<CoreKeyPage, CoreKeyError> {
        self.domain.select(request)
    }

    pub(crate) fn page(
        &self,
        limit: usize,
        after: Option<(&str, &str)>,
        include_tombstones: bool,
    ) -> Result<CoreKeyPage, CoreKeyError> {
        if limit == 0 || limit > MAX_PAGE_KEYS {
            return Err(CoreKeyError::PageLimit);
        }
        let after_key = after
            .map(|(range, _)| CompactRangeKey::from_storage(range, self.domain.range_encoding));
        let bound = after_key.as_ref().zip(after.map(|(_, hash)| hash));
        let state = self
            .domain
            .state
            .lock()
            .map_err(|_| CoreKeyError::Poisoned)?;
        let (keys, exhausted) = state.keys.page(limit, bound, include_tombstones);
        Ok(CoreKeyPage {
            examined: keys.len(),
            keys,
            exhausted,
            last_examined: None,
        })
    }
}

fn encode_bound(value: Bound<&str>, encoding: RangeKeyEncoding) -> Bound<CompactRangeKey> {
    value.map(|value| CompactRangeKey::from_storage(value, encoding))
}

fn raise_start(start: &mut Bound<CompactRangeKey>, value: CompactRangeKey, excluded: bool) {
    let replace = match start {
        Bound::Unbounded => true,
        Bound::Included(current) => value >= *current,
        Bound::Excluded(current) => value > *current,
    };
    if replace {
        *start = if excluded {
            Bound::Excluded(value)
        } else {
            Bound::Included(value)
        };
    }
}

fn empty_bounds(start: &Bound<CompactRangeKey>, end: &Bound<CompactRangeKey>) -> bool {
    match (start, end) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => false,
        (Bound::Included(start), Bound::Included(end)) => start > end,
        (
            Bound::Included(start) | Bound::Excluded(start),
            Bound::Included(end) | Bound::Excluded(end),
        ) => start >= end,
    }
}
