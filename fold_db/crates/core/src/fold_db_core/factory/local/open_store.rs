//! One physical store boundary for normal LastDB construction.

use crate::error::FoldDbResult;
use crate::storage::disk_pack::PackNamespacedStore;
use crate::storage::traits::NamespacedStore;
use std::path::Path;
use std::sync::Arc;

pub(super) fn open_primary_store(path: &Path) -> FoldDbResult<Arc<dyn NamespacedStore>> {
    // Native open creates only an empty home or reopens its own format. It
    // refuses legacy data; startup never selects or migrates another engine.
    Ok(Arc::new(PackNamespacedStore::open(path)?))
}
