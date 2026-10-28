//! Native logical-store construction and decrypt-integrity check.

use crate::crypto::E2eKeys;
use crate::db_operations::DbOperations;
use crate::error::{FoldDbError, FoldDbResult};
use std::sync::Arc;

pub(super) async fn open_db_operations(
    store: Arc<dyn crate::storage::traits::NamespacedStore>,
    e2e_keys: &E2eKeys,
) -> FoldDbResult<DbOperations> {
    // A typed home blinds every hash; the molecule contract orders ranges.
    let hash_key_codec = crate::atom::MoleculeKeyCodec::with_encodings(
        crate::atom::HashKeyEncoding::BlindV1,
        crate::atom::RangeKeyEncoding::Plain,
        Some(e2e_keys.index_key()),
        Some(e2e_keys.ope_key()),
    );
    DbOperations::from_namespaced_store_with_content_and_molecule_keys(
        store,
        Some(e2e_keys.encryption_key()),
        hash_key_codec,
        Some(e2e_keys.encryption_key()),
    )
    .await
    .map_err(|e| FoldDbError::Config(e.to_string()))
}
