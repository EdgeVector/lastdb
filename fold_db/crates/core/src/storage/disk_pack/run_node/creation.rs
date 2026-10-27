//! Commit typed creation proofs before the clear node marker can publish.

use super::{map, RunHome, StorageResult, RUN_NODE_BYTES};
use crate::atom::molecule_key_codec::TYPED_KEY_FORMAT;
use crate::crypto::content_key_proof;
use crate::db_operations::AtomStore;
use crate::storage::disk_pack::{kv, run_admission::TYPED_NODE_BYTES};
use laststore::disk_run::home::HomeMutation;

pub(super) fn initialize(home: &RunHome, node_key: &[u8; 32]) -> StorageResult<&'static [u8]> {
    // An existing home never reaches this selection. Explicit old encodings
    // retain a way to create compatibility homes and offline converter inputs.
    if legacy_requested() {
        return Ok(RUN_NODE_BYTES);
    }
    let (collection, key) = kv::coordinate("main", AtomStore::CURRENT_KEY_FORMAT_KEY.as_bytes());
    let format = HomeMutation {
        collection,
        key,
        body: Some(TYPED_KEY_FORMAT.to_vec()),
    };
    let proof = HomeMutation {
        collection: content_key_proof::NAMESPACE.into(),
        key: super::super::native_routing::encode(content_key_proof::KEY),
        body: Some(content_key_proof::create(node_key)?),
    };
    // RunHome publishes both records in one authenticated durable CURRENT.
    // A crash before the node marker leaves only recreatable creation state.
    home.commit(vec![format, proof]).map_err(map)?;
    Ok(TYPED_NODE_BYTES)
}

fn legacy_requested() -> bool {
    [
        ("LASTDB_HASH_KEY_ENCODING", "plain"),
        ("LASTDB_RANGE_KEY_ENCODING", "ope_v1"),
    ]
    .into_iter()
    .any(|(name, legacy)| std::env::var(name).is_ok_and(|value| value.eq_ignore_ascii_case(legacy)))
}
