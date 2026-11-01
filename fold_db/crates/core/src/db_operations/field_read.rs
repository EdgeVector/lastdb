//! The read path for one schema field.
//!
//! [`FieldVariant`] is a value: its key shape, its common metadata, and an
//! optional hydrated molecule. This module owns every step that loads that
//! value or its atoms from memory or disk: tip selection, molecule refresh,
//! key windows, read receipts, and atom body hydration. Each function takes
//! the field as an argument, so the schema model never calls the store.

use futures::{future::BoxFuture, FutureExt};
use std::collections::HashMap;
use std::sync::Arc;

use crate::db_operations::DbOperations;
use crate::schema::types::field::filter_utils::DEFAULT_UNFILTERED_PAGE_LIMIT;
use crate::schema::types::field::{
    FieldKind, FieldValue, FieldVariant, HashRangeFilter, KeyWindow, KeyedAtomMatch,
};
use crate::schema::types::key_value::KeyValue;
use crate::schema::types::SchemaError;

mod candidate_page;
mod collect;
mod core_model;
mod core_receipts;
mod fetch;
mod named_window;
mod refresh;
mod resolve;
mod resolve_scan;

pub(crate) use candidate_page::{hash_range_candidate_page, materialize_hash_range_candidates};
pub use collect::collect_matches;
pub use fetch::fetch_atoms_with_key_metadata_async_with_prefix;
pub(crate) use fetch::{fetch_field_atom_batches, FieldAtomBatch};
pub(crate) use named_window::{has_primary_tips, resolve_named_window};
pub use refresh::refresh_from_db;
pub use resolve::{resolve_value, resolve_value_windowed};

use core_model::resolve_core_model;
use core_receipts::{note_loaded_tip, note_query_tip_candidates};
use refresh::{record_resolved_tip_keys, refresh_for_read};
use resolve::hash_range_tip_matches;
use resolve_scan::{
    api_rows_from_scan, materialize_tip_matches, page_visible_matches, scan_hash_range_matches,
};

/// One selected tip: `(api key, tip record, physical tip key)`.
type TipMatch = (
    KeyValue,
    crate::db_operations::atom_store::PerKeyRecord,
    String,
);

async fn read_range_profile(
    field: &FieldVariant,
    db_ops: &DbOperations,
) -> Result<Option<schema_types::molecule_range_profile::MoleculeRangeProfile>, SchemaError> {
    let Some(molecule) = field.common().molecule_uuid() else {
        return Ok(crate::schema::types::key_comparison::KeyComparisonContext::current_profile());
    };
    db_ops.atoms().load_molecule_key_bundle(molecule).await?;
    Ok(Some(
        db_ops
            .atoms()
            .key_codec_for_molecule(molecule)?
            .range_profile(),
    ))
}

async fn captured_field_read<T>(
    db_ops: &DbOperations,
    future: BoxFuture<'_, Result<T, SchemaError>>,
) -> Result<T, SchemaError> {
    let scope = db_ops.read_scope().await?;
    let future = crate::storage::disk_pack::logical_paths::field::query_scope(future);
    scope.run_result(future).await
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

/// The exact molecule codec defines the stored range representation.
fn api_key_value_from_storage(
    mut kv: KeyValue,
    codec: &crate::atom::MoleculeKeyCodec,
) -> Result<KeyValue, SchemaError> {
    if let Some(range) = kv.range.as_deref() {
        kv.range = Some(
            codec
                .api_range(range)
                .map_err(|error| SchemaError::InvalidData(error.to_string()))?,
        );
    }
    Ok(kv)
}
