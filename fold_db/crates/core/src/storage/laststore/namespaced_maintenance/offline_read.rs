//! Open the physical store for offline read operations without side effects.

use super::*;
use std::fs;

impl LastStoreNamespacedStore {
    /// Read the existing high-water marker if present, without creating or
    /// updating it. The offline reader attaches no writable marker or memo.
    pub fn open_for_offline_read(path: &Path, mut opts: LastStoreOptions) -> StorageResult<Self> {
        let marker = high_water_path_for_store_root(path);
        match fs::read(&marker) {
            Ok(bytes) => {
                let state: high_water::LastStoreHighWater = serde_json::from_slice(&bytes)
                    .map_err(|error| {
                        StorageError::BackendError(format!(
                            "read high-water {}: {error}",
                            marker.display()
                        ))
                    })?;
                if state.version != 1 {
                    return Err(StorageError::BackendError(format!(
                        "unsupported high-water version {}",
                        state.version
                    )));
                }
                opts.csn_floor = state.csn_high_water;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(StorageError::BackendError(format!(
                    "read high-water {}: {error}",
                    marker.display()
                )))
            }
        }
        opts.data_key = None;
        let store = LastStore::open_read_only(path, opts).map_err(LastStoreKvStore::map_error)?;
        Ok(Self::assemble(Arc::new(store), None))
    }
}
