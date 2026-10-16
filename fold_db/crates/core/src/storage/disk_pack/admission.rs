//! Exact stored-record protection owned across complete append/publication.

use super::*;
use std::collections::BTreeSet;

pub(super) struct WriteProtection {
    logical: Arc<Mutex<LogicalResidentSet>>,
    keys: BTreeSet<(String, String)>,
}

impl WriteProtection {
    /// Own every final coordinate together before any native append begins.
    /// A partial reservation is returned on failure, without admitting a write.
    pub(super) fn reserve(
        runtime: &PackRuntime,
        operations: &[HomeMutation],
    ) -> StorageResult<Self> {
        let mut protection = Self {
            logical: Arc::clone(&runtime.logical),
            keys: BTreeSet::new(),
        };
        {
            let mut logical = runtime
                .logical
                .lock()
                .expect("native logical write protection");
            for operation in operations {
                let key = (operation.collection.clone(), operation.key.clone());
                if protection.keys.contains(&key) {
                    continue;
                }
                if !logical.hold_record(&key.0, &key.1) {
                    logical.release_records(std::mem::take(&mut protection.keys));
                    return Err(StorageError::WriteResourceLimit {
                        detail: "native complete write cannot reserve protected logical records"
                            .into(),
                    });
                }
                protection.keys.insert(key);
            }
        }
        Ok(protection)
    }

    /// Invalidate the old fetched bytes while this owner still protects every key.
    pub(super) fn invalidate(&self) {
        let mut logical = self
            .logical
            .lock()
            .expect("native logical write protection");
        for (collection, key) in &self.keys {
            logical.forget_record(collection, key);
        }
    }
}

impl Drop for WriteProtection {
    fn drop(&mut self) {
        if let Ok(mut logical) = self.logical.lock() {
            logical.release_records(std::mem::take(&mut self.keys));
        }
    }
}
