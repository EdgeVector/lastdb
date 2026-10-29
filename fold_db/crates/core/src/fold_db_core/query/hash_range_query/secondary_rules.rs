//! Storage-free source batches, coordinate alignment and field concurrency.

use std::collections::HashMap;

use crate::schema::types::field::FieldVariant;
use crate::schema::types::key_value::KeyValue;

use super::{KeySource, SecondaryPending};

/// Production defaults to serial field reads. A value above one opts in to
/// bounded overlap for current-head queries only.
pub(super) fn secondary_field_concurrency(value: Option<&str>, request: Option<usize>) -> usize {
    request
        .or_else(|| value.and_then(|value| value.parse::<usize>().ok()))
        .filter(|value| *value > 0)
        .unwrap_or(1)
        .min(8)
}

/// Keep caller key order and duplicates within each selected storage source.
pub(super) fn keys_by_prefix(
    keys: &[KeyValue],
    sources: &HashMap<KeyValue, KeySource>,
) -> HashMap<KeySource, Vec<KeyValue>> {
    let mut by_prefix: HashMap<KeySource, Vec<KeyValue>> = HashMap::new();
    for key in keys {
        let source = sources.get(key).cloned().unwrap_or(None);
        by_prefix.entry(source).or_default().push(key.clone());
    }
    by_prefix
}

pub(super) struct SecondarySlotBatch {
    pub(super) storage_slots: Vec<(String, String)>,
    pub(super) api_keys: Vec<KeyValue>,
}

/// Admit coordinates and their API keys together so the later zip stays aligned.
pub(super) fn slot_batch(field: &FieldVariant, keys: &[KeyValue]) -> SecondarySlotBatch {
    let mut storage_slots = Vec::with_capacity(keys.len());
    let mut api_keys = Vec::with_capacity(keys.len());
    for key in keys {
        if let Some(slot) = field.slot_coordinates(key) {
            storage_slots.push(slot);
            api_keys.push(key.clone());
        }
    }
    SecondarySlotBatch {
        storage_slots,
        api_keys,
    }
}

/// Retain original pending positions; reverse extraction keeps the last duplicate.
pub(super) fn pending_slots_by_prefix(
    pending: &[SecondaryPending],
) -> HashMap<KeySource, HashMap<String, Vec<usize>>> {
    let mut scopes: HashMap<KeySource, HashMap<String, Vec<usize>>> = HashMap::new();
    for (slot, entry) in pending.iter().enumerate() {
        scopes
            .entry(entry.storage_prefix.clone())
            .or_default()
            .entry(entry.field_name.clone())
            .or_default()
            .push(slot);
    }
    scopes
}
