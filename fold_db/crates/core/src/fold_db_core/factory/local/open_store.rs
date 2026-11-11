//! One physical store boundary for normal LastDB construction.

use crate::error::FoldDbResult;
use crate::storage::disk_pack::PackNamespacedStore;
use std::path::Path;

pub(super) fn open_primary_store(
    path: &Path,
    node_key: &[u8; 32],
) -> FoldDbResult<PackNamespacedStore> {
    // Native open creates an empty home or reopens a home in the current format.
    Ok(PackNamespacedStore::open_keyed(path, Some(node_key))?)
}
