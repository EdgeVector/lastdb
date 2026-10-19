//! A primary's presence survives membership filtering and result windows.

use super::*;
use std::collections::HashSet;

pub(super) struct PrimaryRequest<'a> {
    pub name: &'a str,
    pub filter: Option<HashRangeFilter>,
    pub window: Option<KeyWindow>,
    pub include_tombstones: bool,
    pub received: &'a [String],
    pub own_prefix: KeySource,
    pub named_hash: Option<String>,
}

pub(super) struct PrimarySelection {
    pub values: HashMap<KeyValue, FieldValue>,
    pub sources: HashMap<KeyValue, KeySource>,
    pub had_spine: bool,
}

impl PrimarySelection {
    pub fn new(values: HashMap<KeyValue, FieldValue>, source: KeySource) -> Self {
        let had_spine = !values.is_empty();
        let sources = values
            .keys()
            .map(|key| (key.clone(), source.clone()))
            .collect();
        Self {
            values,
            sources,
            had_spine,
        }
    }

    pub fn retain_named(&mut self, wanted: Option<&str>) {
        if let Some(wanted) = wanted {
            self.values
                .retain(|_, value| HashRangeQueryProcessor::field_value_is_hash(value, wanted));
            self.sources.retain(|key, _| self.values.contains_key(key));
        }
    }

    pub fn bound(&mut self, window: KeyWindow) {
        let mut keys: Vec<_> = self.values.keys().cloned().collect();
        keys.sort_by(KeyValue::cmp_page_order);
        let kept = match window {
            KeyWindow::Offset { offset, limit } => {
                keys.into_iter().skip(offset).take(limit).collect()
            }
            KeyWindow::After { after, limit } => keys
                .into_iter()
                .filter(|key| key.cmp_page_order(&after).is_gt())
                .take(limit)
                .collect(),
        };
        self.retain_keys(kept);
    }

    pub fn retain_keys(&mut self, keys: HashSet<KeyValue>) {
        self.values.retain(|key, _| keys.contains(key));
        self.sources.retain(|key, _| keys.contains(key));
    }
}
