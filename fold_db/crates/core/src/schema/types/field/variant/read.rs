use chrono::{DateTime, Utc};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::db_operations::DbOperations;
use crate::schema::types::field::{HashRangeFilter, KeyWindow, KeyedAtomMatch};
use crate::schema::types::key_value::KeyValue;
use crate::schema::types::SchemaError;

use super::super::filter_utils::DEFAULT_UNFILTERED_PAGE_LIMIT;
use super::{FieldKind, FieldValue, FieldVariant};

mod collect;
mod core_model;
mod core_receipts;
mod refresh;
mod resolve;
mod resolve_scan;

/// One live field slot from an authoritative, hydrated hash-partition walk.
///
/// Unlike the public query resolver, this internal form never drops a key
/// whose tip or atom body cannot resolve. Aggregate repair uses that stronger
/// contract before it certifies a summary generation.
#[derive(Debug, Clone)]
pub(crate) struct AuthoritativeFieldMatch {
    pub key: KeyValue,
    pub entry: crate::atom::AtomEntry,
    pub value: serde_json::Value,
}

/// Content tombstones can lack the key metadata flag. Keep all paths identical.
fn retain_visible_values(values: &mut HashMap<KeyValue, FieldValue>, include_tombstones: bool) {
    if !include_tombstones {
        values.retain(|_, value| {
            let live = !crate::atom::is_tombstone_value(&value.value);
            if !live {
                crate::db_operations::note_tombstoned_row();
            }
            live
        });
    }
}

/// Map storage-form range (OPE hex) → API plaintext for query responses.
fn api_key_value_from_storage(kv: KeyValue) -> KeyValue {
    match kv.range.as_deref() {
        Some(r) if !r.is_empty() => {
            let range = crate::crypto::E2eKeys::ope_decode_range_plaintext(r)
                .unwrap_or_else(|| r.to_string());
            KeyValue::new(kv.hash, Some(range))
        }
        _ => kv,
    }
}
