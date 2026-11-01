//! A separate byte allowance, including build work and read-held old domains.

use super::CoreKeyError;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct CoreKeyBudget {
    limit: usize,
    used: AtomicUsize,
    peak: AtomicUsize,
}

#[derive(Clone, Copy, Debug, serde::Serialize)]
pub(crate) struct CoreKeyBudgetStats {
    pub(crate) limit_bytes: usize,
    pub(crate) used_bytes: usize,
    pub(crate) peak_bytes: usize,
}

/// Reservations release automatically, including failed builds and old leases.
#[derive(Debug)]
pub(crate) struct CoreKeyReservation {
    budget: Arc<CoreKeyBudget>,
    bytes: usize,
}

impl CoreKeyBudget {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            limit,
            used: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        }
    }

    pub(crate) fn stats(&self) -> CoreKeyBudgetStats {
        CoreKeyBudgetStats {
            limit_bytes: self.limit,
            used_bytes: self.used.load(Ordering::Acquire),
            peak_bytes: self.peak.load(Ordering::Acquire),
        }
    }

    /// The coordinator reserves its page buffers and temporary changes here.
    pub(crate) fn reserve_temporary(
        self: &Arc<Self>,
        bytes: usize,
    ) -> Result<CoreKeyReservation, CoreKeyError> {
        self.reserve(bytes)?;
        Ok(CoreKeyReservation {
            budget: Arc::clone(self),
            bytes,
        })
    }

    fn reserve(&self, bytes: usize) -> Result<(), CoreKeyError> {
        let previous = self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes).filter(|total| *total <= self.limit)
            })
            .map_err(|_| CoreKeyError::Capacity)?;
        self.peak.fetch_max(previous + bytes, Ordering::AcqRel);
        Ok(())
    }
}

impl Drop for CoreKeyReservation {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
