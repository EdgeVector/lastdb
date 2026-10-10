//! Read-only key helpers for the offline dropped-schema planner.
//!
//! The planner runs outside this crate. It must name the exact keys that
//! production code writes. These helpers call the production builders, so the
//! planner never keeps a second copy of a key formula.

mod metadata;
pub use metadata::{offline_delete_history_atom, offline_molecule_shape};
mod source;
pub use super::atom_ref_edges::{
    offline_atom_ref_root, offline_history_atom_roots, OfflineAtomRefRoot,
};
pub use source::*;

use super::atom_ref_edges::tip_edge;
use super::{MoleculeRefEdge, PerKeyRecord};
use crate::atom::molecule_key_codec;
use crate::schema::SchemaError;

/// One decoded `mk:` row and the compact reverse-edge key of its tip.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TipEdgeKey {
    /// Molecule id exactly as spelled in the `mk:` key.
    pub molecule_uuid: String,
    /// Atom named by the tip.
    pub atom_uuid: String,
    /// `written_at` of the tip, in the unit the writer used.
    pub written_at: u64,
    /// True when the tip links to an archived tip version (`tv`) chain.
    pub has_prev_tip: bool,
    /// True when the tip is a tombstone.
    pub tombstoned: bool,
    /// The `aref:v2:e:` key of the tip edge. `None` when the atom id is not a
    /// 64-character SHA-256 hex digest. Production writes no compact edge for
    /// such an atom.
    pub edge_key_v2: Option<String>,
}

/// Decode one `mk:` row and build the compact edge key of its tip.
///
/// `mk_key` is the full personal-domain key. `value` is the plaintext tip
/// record. A key or value that does not decode is an error. The caller must
/// not guess an edge key.
pub fn compact_tip_edge_key(mk_key: &str, value: &[u8]) -> Result<TipEdgeKey, SchemaError> {
    let rest = mk_key
        .strip_prefix(molecule_key_codec::MK_PREFIX)
        .ok_or_else(|| SchemaError::InvalidData("tip key does not start with mk:".to_string()))?;
    let (molecule_uuid, _) = rest
        .split_once(':')
        .ok_or_else(|| SchemaError::InvalidData("tip key has no molecule id".to_string()))?;
    let (disk_hash, disk_range) = molecule_key_codec::decode_hash_range(mk_key, molecule_uuid)
        .ok_or_else(|| SchemaError::InvalidData("tip key has no hash and range".to_string()))?;
    let record: PerKeyRecord = serde_json::from_slice(value)
        .map_err(|error| SchemaError::InvalidData(format!("decode tip record: {error}")))?;
    let edge = tip_edge(molecule_uuid, &disk_hash, &disk_range, &record.entry, true);
    Ok(TipEdgeKey {
        molecule_uuid: molecule_uuid.to_string(),
        atom_uuid: record.entry.atom_uuid.clone(),
        written_at: record.entry.written_at,
        has_prev_tip: !record.entry.prev_tip_id.is_empty(),
        tombstoned: record.meta.as_ref().is_some_and(|meta| meta.tombstoned),
        edge_key_v2: edge.storage_key_v2(None).ok(),
    })
}

/// Prefix of every `mref:v1:e:` edge that targets `molecule_uuid`.
///
/// The prefix ends with the NUL that separates the target from the source. It
/// is cut from a real production edge key. Pass the molecule id exactly as one
/// spelling. The two spellings of a molecule id have two prefixes.
pub fn molecule_ref_target_prefix(molecule_uuid: &str) -> Option<String> {
    let key = MoleculeRefEdge {
        molecule_uuid: molecule_uuid.to_string(),
        source: String::new(),
        edge_type: String::new(),
    }
    .storage_key(None);
    let end = key.find('\0')?;
    Some(key[..=end].to_string())
}
