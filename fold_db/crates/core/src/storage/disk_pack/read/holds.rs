//! One logical-record hold per exact coordinate used by a native read scope.

use super::super::*;
use std::collections::BTreeSet;

pub(super) struct CallRecordHolds {
    logical: Arc<Mutex<LogicalResidentSet>>,
    keys: Mutex<BTreeSet<(String, String)>>,
}

impl CallRecordHolds {
    pub(super) fn new(logical: Arc<Mutex<LogicalResidentSet>>) -> Self {
        Self {
            logical,
            keys: Mutex::new(BTreeSet::new()),
        }
    }

    pub(super) fn hold(
        &self,
        logical: &mut LogicalResidentSet,
        collection: &str,
        key: &str,
    ) -> StorageResult<()> {
        let coordinate = (collection.to_string(), key.to_string());
        let mut held = self
            .keys
            .lock()
            .map_err(|_| StorageError::BackendError("native read hold lock is poisoned".into()))?;
        if held.contains(&coordinate) {
            return Ok(());
        }
        if !logical.hold_record(collection, key) {
            return Err(StorageError::ReadResourceLimit {
                detail: "native logical-record capacity is held by active calls".into(),
            });
        }
        held.insert(coordinate);
        Ok(())
    }
}

impl Drop for CallRecordHolds {
    fn drop(&mut self) {
        // No other caller can reach this last owner. Do not panic during unwind.
        let keys = self
            .keys
            .get_mut()
            .unwrap_or_else(|error| error.into_inner());
        if let Ok(mut logical) = self.logical.lock() {
            logical.release_records(std::mem::take(keys));
        }
    }
}
