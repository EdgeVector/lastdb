//! One physical store boundary for normal LastDB construction.

use crate::error::FoldDbResult;
use crate::storage::disk_pack::{NativeWarmSet, PackNamespacedStore};
use crate::storage::traits::NamespacedStore;
use std::path::Path;
use std::sync::Arc;

pub(super) fn open_primary_store(
    path: &Path,
    node_key: &[u8; 32],
) -> FoldDbResult<Arc<dyn NamespacedStore>> {
    // Native open creates only an empty home or reopens its own format. It
    // refuses legacy data; startup never selects or migrates another engine.
    // A disk format v2 home derives its home key from the node key. The
    // database owns the warm set and opens the store with it.
    Ok(Arc::new(PackNamespacedStore::open_keyed(
        path,
        Some(node_key),
        Arc::new(NativeWarmSet::default()),
    )?))
}
