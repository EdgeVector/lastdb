//! A primary's presence survives membership filtering and result windows.

use super::*;
use crate::schema::types::KeyValuePageOrder;
use std::collections::HashSet;

pub(super) struct PrimaryRequest<'a> {
    pub name: &'a str,
    pub filter: Option<HashRangeFilter>,
    pub window: Option<KeyWindow>,
    pub include_tombstones: bool,
    pub received: &'a [String],
    pub own_prefix: KeySource,
    pub named_layout: bool,
    pub discovery: Option<&'a super::received_window::CandidatePages>,
}

pub(super) struct PrimarySelection {
    pub values: HashMap<KeyValue, FieldValue>,
    /// Range-field bodies that proved membership, from each key's chosen source.
    pub layout_values: HashMap<KeyValue, FieldValue>,
    pub sources: HashMap<KeyValue, KeySource>,
    pub had_spine: bool,
}

impl PrimarySelection {
    pub fn new(values: HashMap<KeyValue, FieldValue>, source: &KeySource) -> Self {
        let had_spine = !values.is_empty();
        let sources = values
            .keys()
            .map(|key| (key.clone(), source.clone()))
            .collect();
        Self {
            values,
            layout_values: HashMap::new(),
            sources,
            had_spine,
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
        self.retain_keys(&kept);
    }

    pub fn retain_keys(&mut self, keys: &HashSet<KeyValue>) {
        self.values.retain(|key, _| keys.contains(key));
        self.layout_values.retain(|key, _| keys.contains(key));
        self.sources.retain(|key, _| keys.contains(key));
    }
}
