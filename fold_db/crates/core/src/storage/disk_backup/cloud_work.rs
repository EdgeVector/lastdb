//! One active native cloud operation shares the process allocation budget.

use crate::storage::error::{StorageError, StorageResult};
use std::sync::atomic::{AtomicBool, Ordering};

static ACTIVE: AtomicBool = AtomicBool::new(false);

/// Hold this admission for the complete export or restore and its live readers.
/// Paused durable work retains its files independently of this process guard.
#[must_use = "retain the guard for the complete native cloud operation"]
pub struct NativeCloudWorkGuard {
    _private: (),
}

impl NativeCloudWorkGuard {
    pub fn acquire() -> StorageResult<Self> {
        ACTIVE
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                StorageError::InvalidOperation(
                    "one native cloud export or restore is already active".into(),
                )
            })?;
        Ok(Self { _private: () })
    }
}

impl Drop for NativeCloudWorkGuard {
    fn drop(&mut self) {
        ACTIVE.store(false, Ordering::Release);
    }
}
