use futures::{future::BoxFuture, FutureExt};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::db_operations::DbOperations;
use crate::schema::types::field::{HashRangeFilter, KeyWindow, KeyedAtomMatch};
use crate::schema::types::key_value::KeyValue;
use crate::schema::types::SchemaError;

use super::super::filter_utils::DEFAULT_UNFILTERED_PAGE_LIMIT;
use super::{FieldKind, FieldValue, FieldVariant};

mod candidate_page;
mod collect;
mod core_model;
mod core_receipts;
mod named_window;
mod refresh;
mod resolve;
mod resolve_scan;

impl FieldVariant {
    fn require_supported_read(&self, filter: Option<&HashRangeFilter>) -> Result<(), SchemaError> {
        use crate::schema::types::schema::DeclarativeSchemaType;

        let layout = match self.kind {
            FieldKind::Single => DeclarativeSchemaType::Single,
            FieldKind::Hash => DeclarativeSchemaType::Hash,
            FieldKind::Range => DeclarativeSchemaType::Range,
            FieldKind::HashRange => DeclarativeSchemaType::HashRange,
        };
        crate::schema::require_logical_layout(&layout)?;
        crate::schema::require_logical_filter(filter)
    }
}

async fn captured_field_read<T>(
    db_ops: &DbOperations,
    future: BoxFuture<'_, Result<T, SchemaError>>,
) -> Result<T, SchemaError> {
    let scope = db_ops.read_scope().await?;
    let future = crate::storage::disk_pack::logical_paths::field::query_scope(future);
    scope.run_result(future).await
}

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
