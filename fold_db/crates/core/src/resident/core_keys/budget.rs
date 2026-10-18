//! A separate byte allowance, including build work and read-held old domains.

use super::{CoreKeyError, DEFAULT_SCHEMA_KEY_BYTES, SCHEMA_KEY_BYTES_ENV};
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

    pub(crate) fn from_env() -> Self {
        let limit = std::env::var(SCHEMA_KEY_BYTES_ENV)
            .ok()
            .and_then(|raw| raw.trim().parse().ok())
            .unwrap_or(DEFAULT_SCHEMA_KEY_BYTES);
        Self::new(limit)
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

impl CoreKeyReservation {
    pub(super) fn grow(&mut self, bytes: usize) -> Result<(), CoreKeyError> {
        let total = self
            .bytes
            .checked_add(bytes)
            .ok_or(CoreKeyError::Capacity)?;
        self.budget.reserve(bytes)?;
        self.bytes = total;
        Ok(())
    }

    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Drop for CoreKeyReservation {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
