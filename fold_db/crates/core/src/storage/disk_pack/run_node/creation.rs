//! Commit the creation proofs before the clear node marker can publish.

use super::{map, RunHome, StorageResult};
use crate::storage::content_key_proof;
use crate::storage::disk_pack::{kv, run_admission::RECORD_NODE_BYTES};
use crate::storage::format::{CURRENT_KEY_FORMAT_KEY, CURRENT_KEY_FORMAT_VALUE, MAIN_NAMESPACE};
use laststore::disk_run::home::HomeMutation;

/// Every new home has the current format. The caller publishes the returned
/// node marker after this commit.
pub(super) fn initialize(home: &RunHome, node_key: &[u8; 32]) -> StorageResult<&'static [u8]> {
    let (collection, key) = kv::coordinate(MAIN_NAMESPACE, CURRENT_KEY_FORMAT_KEY.as_bytes());
    let format = HomeMutation {
        collection,
        key,
        body: Some(CURRENT_KEY_FORMAT_VALUE.to_vec()),
    };
    let proof = HomeMutation {
        collection: content_key_proof::NAMESPACE.into(),
        key: super::super::native_routing::encode(content_key_proof::KEY),
        body: Some(content_key_proof::create(node_key)?),
    };
    // RunHome publishes both records in one authenticated durable CURRENT.
    // A crash before the node marker leaves only recreatable creation state.
    home.commit(vec![format, proof]).map_err(map)?;
    Ok(RECORD_NODE_BYTES)
}
