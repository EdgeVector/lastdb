//! Native logical-store construction and decrypt-integrity check.

use crate::crypto::E2eKeys;
use crate::db_operations::DbOperations;
use crate::error::{FoldDbError, FoldDbResult};
use std::sync::Arc;

pub(super) async fn open_db_operations(
    store: Arc<dyn crate::storage::traits::NamespacedStore>,
    e2e_keys: &E2eKeys,
) -> FoldDbResult<DbOperations> {
    let hash_key_encoding = crate::atom::HashKeyEncoding::from_env_or_default();
    let range_key_encoding = crate::atom::RangeKeyEncoding::from_env_or_default();
    tracing::info!(
        ?hash_key_encoding,
        ?range_key_encoding,
        "molecule key storage encoding (default blind_v1+ope_v1)"
    );
    let hash_key_codec = crate::atom::MoleculeKeyCodec::with_encodings(
        hash_key_encoding,
        range_key_encoding,
        Some(e2e_keys.index_key()),
        Some(e2e_keys.ope_key()),
    );
    DbOperations::from_namespaced_store_with_atom_and_molecule_keys(
        store,
        Some(e2e_keys.encryption_key()),
        hash_key_codec,
        Some(e2e_keys.encryption_key()),
    )
    .await
    .map_err(|e| FoldDbError::Config(e.to_string()))
}
