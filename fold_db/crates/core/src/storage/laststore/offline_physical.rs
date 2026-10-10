//! Exact physical collection readers for stopped-home administrative proofs.

use super::*;
use crate::crypto::CryptoProvider;
use crate::storage::encrypting_store::EncryptingKvStore;

/// Bypass logical-main routing and duplicate collapse. Both handles address
/// exactly the named physical collection. They inherit LastStore's read-only
/// mode when the caller opens the existing home read-only.
///
/// The plaintext policy is the normal at-rest policy. Mixed raw journal keys
/// still require the cloud engine's exact-key reader.
pub fn offline_physical_collection_pair(
    store: Arc<LastStore>,
    collection: &str,
    crypto: Arc<dyn CryptoProvider>,
) -> (Arc<dyn KvStore>, Arc<dyn KvStore>) {
    let raw: Arc<dyn KvStore> = Arc::new(LastStoreKvStore::with_logical(
        store,
        collection.to_string(),
        None,
        Arc::default(),
    ));
    let seam: Arc<dyn KvStore> = if LASTSTORE_PLAINTEXT_NAMESPACES.contains(&collection)
        || collection == crate::storage::encrypting_namespaced_store::STRICT_MARKER_NAMESPACE
    {
        Arc::clone(&raw)
    } else {
        Arc::new(EncryptingKvStore::new(collection, Arc::clone(&raw), crypto))
    };
    (raw, seam)
}
