//! Per-key molecule store paths.

use crate::atom::molecule_key_codec;
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use serde_json::Value;
use std::collections::HashMap;

use super::super::conflict_record::Recorded;
use super::super::counter::{TipChange, TipChangeSet};
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
pub(crate) enum MoleculeKeyDomain {
    /// Slots already carry storage-form segments and must be written verbatim.
    Storage,
}

mod changed_keys;
pub(in crate::db_operations::atom_store) use changed_keys::accounted_items;
mod generation_activation;
mod items;
mod single;
