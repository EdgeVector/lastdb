//! Per-key molecule store paths.

use crate::atom::molecule_key_codec;
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use serde_json::Value;
use std::collections::HashMap;

#[cfg(any(feature = "sharing", test))]
use super::super::helpers::hash_range_page_index_complete_item;
use super::super::helpers::{hash_key_lookup_item, hash_range_page_index_item_for_record_key};
use super::super::types::{ChangedKey, MoleculeData, MoleculeHeader, PerKeyRecord};
use super::super::AtomStore;

fn is_tip_item_key(key: &str) -> bool {
    key.starts_with("mk:") || key.contains(":mk:")
}

fn tip_item_keys(items: &[(String, Value)]) -> Vec<String> {
    items
        .iter()
        .filter(|(key, _)| is_tip_item_key(key))
        .map(|(key, _)| key.clone())
        .collect()
}

fn is_legacy_zero_clock(entry: &crate::atom::AtomEntry) -> bool {
    entry.written_at == 0 && entry.logical_counter == 0 && entry.mutation_uuid.is_empty()
}

/// Product molecule store must never emit `ref:{M}` whole-molecule blobs.
///
/// Those keys map to the cold `legacy_blob_refs` collection. Create/update
/// paths use per-key tips (`mk:`/`mh:`/…). Inventory + purge (and sync
/// migrate-on-receive for peer residual) still address existing `ref:` rows;
/// this guard only blocks the product store from reintroducing them.
fn refuse_legacy_ref_blob_store_items(items: &[(String, Value)]) -> Result<(), SchemaError> {
    for (key, _) in items {
        let bare = key
            .rsplit_once(":ref:")
            .map_or_else(|| key.clone(), |(_, rest)| format!("ref:{rest}"));
        if bare.starts_with("ref:") {
            return Err(SchemaError::InvalidData(format!(
                "refusing product molecule store write of legacy ref: key `{key}` \
                 (whole-molecule blobs are residual-only; use per-key mk:/mh:)"
            )));
        }
    }
    Ok(())
}

/// Which key domain an in-memory [`MoleculeData`]'s slots are in.
///
/// This is **not** a property of the molecule type — it is a property of the
/// path that hydrated it, and the two disagree:
///
/// - [`AtomStore::load_molecule_for_write`] keeps **API form**, decoding
///   nothing, because the write path holds the caller's plaintext keys.
/// - [`AtomStore::load_all_mk_records`] (the full load behind
///   `FieldVariant::refresh_from_db`) reassembles each slot by decoding the
///   `mk:` key it was stored under, so its slots are **storage form**:
///   HMAC-blinded under `BlindV1`, OPE-encoded under `OpeV1`.
///
/// A full rewrite has to know which it was handed. Encoding a molecule that is
/// already encoded produces `blind(blind(hash))` / `ope(ope(range))` — keys no
/// lookup will ever derive, so every surviving slot is rewritten out from under
/// every future read. The rows are still on disk and still list, which is what
/// makes it so quiet: only a keyed read can tell.
///
/// This used to be implicit, and both production callers of the full rewrite
/// (`purge_records_bulk` and `repair_dangling_tips`) held storage-form
/// molecules while the rewrite assumed API form. Making it an argument means a
/// new caller has to state what it holds instead of inheriting the wrong
/// default.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(any(feature = "sharing", test))]
pub(crate) enum MoleculeKeyDomain {
    /// Slots already carry storage-form segments and must be written verbatim.
    Storage,
}

mod changed_keys;
mod generation_activation;
mod items;
mod single;
