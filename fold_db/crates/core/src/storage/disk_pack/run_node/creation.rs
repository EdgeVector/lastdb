//! Commit the creation proofs before the clear node marker can publish.

use super::{map, RunHome, StorageResult};
use crate::atom::molecule_key_codec::TYPED_KEY_FORMAT;
use crate::records::key_format::CURRENT_KEY_FORMAT_KEY;
use crate::storage::content_key_proof;
use crate::storage::disk_pack::{
    kv,
    run_admission::{RECORD_NODE_BYTES, TYPED_NODE_BYTES},
};
use laststore::disk_run::home::HomeMutation;

/// A new home is format 6 until the record model release. A development node
/// sets this flag to create a format 7 (record model) home instead. The record
/// model is not complete before that release, so such a home is for
/// development only.
const RECORD_HOME_ENV: &str = "LASTDB_RECORD_HOME";

pub(super) fn initialize(home: &RunHome, node_key: &[u8; 32]) -> StorageResult<&'static [u8]> {
    let (collection, key) = kv::coordinate("main", CURRENT_KEY_FORMAT_KEY.as_bytes());
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
    if env_flag::var_truthy(RECORD_HOME_ENV) {
        return Ok(RECORD_NODE_BYTES);
    }
    Ok(TYPED_NODE_BYTES)
}
