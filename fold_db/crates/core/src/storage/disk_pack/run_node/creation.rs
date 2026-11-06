//! Commit the creation proofs before the clear node marker can publish.

use super::{map, RunHome, StorageResult};
use crate::storage::content_key_proof;
use crate::storage::disk_pack::{kv, run_admission::RECORD_NODE_BYTES, shared_layout::Scope};
use crate::storage::format::metadata::{
    initial_local_author, FILE_RETENTION_COMPLETE, FILE_RETENTION_KEY, LOCAL_AUTHOR_KEY,
    NATIVE_SHARED_SCOPE_KEY,
};
use crate::storage::format::{
    CURRENT_KEY_FORMAT_KEY, CURRENT_KEY_FORMAT_VALUE, MAIN_NAMESPACE, METADATA_NAMESPACE,
    SHARED_NODE_BYTES,
};
use laststore::disk_run::home::HomeMutation;

/// Every new home has the current format. The caller publishes the returned
/// node marker after this commit.
pub(super) fn initialize(
    home: &RunHome,
    node_key: &[u8; 32],
    scope: Option<&Scope>,
) -> StorageResult<&'static [u8]> {
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
    let mut rows = vec![format, proof];
    let marker = if let Some(scope) = scope {
        rows.push(HomeMutation {
            collection: METADATA_NAMESPACE.into(),
            key: NATIVE_SHARED_SCOPE_KEY.into(),
            body: Some(scope.encode()?),
        });
        SHARED_NODE_BYTES
    } else {
        rows.extend(local_rows(home.identity())?);
        RECORD_NODE_BYTES
    };
    home.commit(rows).map_err(map)?;
    Ok(marker)
}

pub(super) fn initialize_private(home: &RunHome) -> StorageResult<()> {
    home.commit(local_rows(home.identity())?).map_err(map)?;
    Ok(())
}

fn local_rows(stream: uuid::Uuid) -> StorageResult<Vec<HomeMutation>> {
    let (collection, key) = kv::coordinate(METADATA_NAMESPACE, LOCAL_AUTHOR_KEY.as_bytes());
    let author = HomeMutation {
        collection,
        key,
        body: Some(initial_local_author(stream)?),
    };
    let (collection, key) = kv::coordinate(METADATA_NAMESPACE, FILE_RETENTION_KEY.as_bytes());
    let file_retention = HomeMutation {
        collection,
        key,
        body: Some(FILE_RETENTION_COMPLETE.to_vec()),
    };
    Ok(vec![author, file_retention])
}
