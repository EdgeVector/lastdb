//! The rows a molecule owns in one storage scope, and how the drain reads the
//! state that decides their removal.
//!
//! | Rows | Count | Removed by |
//! |---|---|---|
//! | `mk:{M}:...` tip records | `slots` | the key phase, through the planner |
//! | `mgd:v1:{M}:...` generation delete markers | none (one for each slot an erase removed) | the row phase |
//! | `mgr:v1:{M}:...` generation records | none | the row phase |
//! | `rdel:v2:...` delete barriers of the tip keys | none | the row phase |
//! | `mgp:v1:{M}` generation pointer | none | the final batch |
//! | `mcc:{M}` conflict id list | `records` | the final batch |
//! | `mh:{M}` header | the header holds `slots` and `records` | the final batch |
//! | `mref:v1:d\0{scope}\0{M}` candidate row | none | the final batch |
//!
//! The binding rows (`mref:v1:e:` and `mref:v1:n:`) are not on the list: a dead
//! molecule has none. The unbind that took `bindings` to zero deleted them, and
//! a molecule that still has one is refused (`judge.rs`).
//!
//! The molecule key bundle (`mkb:{M}`, `mkw:{M}:...`) is node-global: one bundle
//! serves the molecule in every scope. A counter of one scope cannot tell that
//! no other scope uses the bundle, so the drain leaves it (the instance reclaim
//! of a catalog does the same).

use super::candidate::Candidate;
use super::judge::MoleculeReads;
use crate::atom::molecule_key_codec::{
    header_key, molecule_generation_delete_prefix, molecule_generation_pointer_key,
    molecule_record_prefix, MOLECULE_GENERATION_RECORD_PREFIX,
};
use crate::db_operations::atom_store::molecule_ref_edges::{molecule_target, MOLECULE_REF_PREFIX};
use crate::db_operations::atom_store::AtomStore;
use crate::schema::types::field::build_storage_key;
use crate::schema::SchemaError;
use serde_json::Value;
use std::fmt::Write;

/// The ranges of the rows that the row phase deletes, in order. Each is a prefix
/// that may span many hash groups.
pub(super) fn row_prefixes(candidate: &Candidate) -> Vec<String> {
    let scope = candidate.scope.as_deref();
    let molecule = candidate.molecule.as_str();
    vec![
        build_storage_key(scope, &molecule_generation_delete_prefix(molecule)),
        build_storage_key(
            scope,
            &format!("{MOLECULE_GENERATION_RECORD_PREFIX}{molecule}:"),
        ),
        barrier_prefix(scope, molecule),
    ]
}

/// The prefix of the tip records of the molecule in its scope.
pub(super) fn tip_prefix(candidate: &Candidate) -> String {
    build_storage_key(
        candidate.scope.as_deref(),
        &molecule_record_prefix(&candidate.molecule),
    )
}

/// The prefix of the delete barriers (`rdel:v2:`) of the tip keys of a molecule.
/// A barrier key spells the bytes of its tip key in hex, so the hex of the tip
/// prefix is a prefix of every barrier of the molecule.
fn barrier_prefix(scope: Option<&str>, molecule: &str) -> String {
    let scope_text = scope.map(|scope| format!("{scope}:")).unwrap_or_default();
    let tip_prefix = format!("{scope_text}{}", molecule_record_prefix(molecule));
    let mut key = format!("{scope_text}rdel:v2:");
    for byte in tip_prefix.bytes() {
        let _ = write!(key, "{byte:02x}");
    }
    key
}

/// The single keys the final batch deletes, with the candidate row.
pub(super) fn closing_keys(candidate: &Candidate) -> Vec<Vec<u8>> {
    let scope = candidate.scope.as_deref();
    let molecule = candidate.molecule.as_str();
    vec![
        build_storage_key(scope, &molecule_generation_pointer_key(molecule)).into_bytes(),
        build_storage_key(scope, &format!("mcc:{molecule}")).into_bytes(),
        build_storage_key(scope, &header_key(molecule)).into_bytes(),
        candidate.key().into_bytes(),
    ]
}

impl AtomStore {
    /// Read the header, the `bindings` counter and the binding records of a
    /// molecule. The caller holds the write gate, so the reads see every accepted
    /// batch and the pending batch. Two reads by key and one range read of at
    /// most one row.
    pub(super) async fn read_molecule_reads(
        &self,
        candidate: &Candidate,
    ) -> Result<MoleculeReads, SchemaError> {
        let scope = candidate.scope.as_deref();
        let molecule = &candidate.molecule;
        let header_row = build_storage_key(scope, &header_key(molecule));
        let header = self
            .main_store
            .get_items::<Value>(std::slice::from_ref(&header_row))
            .await
            .map_err(|error| SchemaError::InvalidData(format!("read molecule header: {error}")))?
            .pop()
            .flatten();
        let bindings = self
            .read_binding_counters(std::slice::from_ref(molecule), scope)
            .await?
            .pop()
            .flatten();
        let prefix = build_storage_key(
            scope,
            &format!("{MOLECULE_REF_PREFIX}{}\0", molecule_target(molecule)),
        );
        let (start, end) = crate::kind_partition::exact_prefix_bounds(&prefix);
        let found = self
            .main_store
            .inner()
            .scan_range_paged(start.as_bytes(), end.as_bytes(), 1)
            .await
            .map_err(|error| {
                SchemaError::InvalidData(format!("read molecule binding records: {error}"))
            })?;
        Ok(MoleculeReads {
            header,
            bindings,
            binding_records: !found.is_empty(),
        })
    }
}
