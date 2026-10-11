//! Serialize account replacement consumers and admit their whole allocation peak.

use crate::memory_budget::{self, ProcessMemoryBudget};
use crate::sync::{SyncError, SyncResult};
use std::sync::OnceLock;
use tokio::sync::{Mutex, MutexGuard};

pub(crate) struct Admission {
    _guard: MutexGuard<'static, ()>,
}

impl Admission {
    pub(crate) async fn acquire(peak: u64) -> SyncResult<Self> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let admission = Self {
            _guard: LOCK.get_or_init(|| Mutex::new(())).lock().await,
        };
        admission.check(peak)?;
        Ok(admission)
    }

    pub(crate) fn check(&self, peak: u64) -> SyncResult<()> {
        let budget = ProcessMemoryBudget::from_env();
        let metric = std::env::var(memory_budget::GUARD_METRIC_ENV)
            .ok()
            .map(|value| value.trim().to_ascii_lowercase());
        let measured = match metric.as_deref() {
            Some("rss") => crate::sync::engine::upload_policy::sample_rss_bytes(),
            None | Some("footprint") => memory_budget::current_phys_footprint_bytes(),
            _ => None,
        }.ok_or_else(|| error("cloud replacement memory guard sample is unavailable; set a supported daemon guard metric before retry"))?;
        let standing = measured.max(budget.projected_total_bytes);
        let margin = (budget.rss_limit_bytes / 10).max(256 * 1024 * 1024);
        let total = checked_sum(&[standing, margin, peak])?;
        if total > budget.rss_limit_bytes {
            return Err(error("cloud replacement needs more free memory under the configured memory guard; retain the local source and retry"));
        }
        Ok(())
    }
}

/// Both outer and inner legacy compressed envelopes use the existing codec
/// ceiling. Metadata strings need not be proportional to file plaintext.
pub(crate) fn logical_row_bound(raw: u64) -> u64 {
    raw.max(crate::crypto::at_rest::at_rest_max_inflated_bytes())
}

/// Covers raw/opened rows, parsed strings, both envelope buffers, plaintext,
/// target encryption and storage append copies. No four-large-object batch.
pub(crate) fn row_peak(raw: u64) -> SyncResult<u64> {
    checked_sum(&[checked_scale(logical_row_bound(raw), 8)?, 4 * 1024 * 1024])
}

pub(crate) fn snapshot_peak(json_bound: u64, row_scratch: u64) -> SyncResult<u64> {
    checked_sum(&[checked_scale(json_bound, 8)?, row_scratch, 4 * 1024 * 1024])
}

pub(crate) fn checked_sum(values: &[u64]) -> SyncResult<u64> {
    values.iter().try_fold(0u64, |sum, value| {
        sum.checked_add(*value)
            .ok_or_else(|| error("cloud replacement allocation size overflows"))
    })
}

pub(crate) fn checked_scale(value: u64, factor: u64) -> SyncResult<u64> {
    value
        .checked_mul(factor)
        .ok_or_else(|| error("cloud replacement allocation size overflows"))
}

pub(crate) fn raw_lengths(
    admission: &Admission,
    retained_peak: u64,
    source: &dyn crate::storage::NamespacedStore,
    namespace: &str,
    keys: &[String],
) -> SyncResult<Vec<Option<laststore::PointValueSize>>> {
    let key_bytes = keys
        .iter()
        .try_fold(0u64, |sum, key| checked_sum(&[sum, key.len() as u64, 128]))?;
    admission.check(checked_sum(&[retained_peak, checked_scale(key_bytes, 8)?])?)?;
    let store = source
        .raw_last_store()
        .ok_or_else(|| error("cloud replacement needs exact LastStore value sizes"))?;
    crate::storage::laststore::raw_value_lengths(&store, namespace, keys, |peak| {
        let peak = checked_sum(&[retained_peak, peak])
            .map_err(|error| crate::storage::StorageError::BackendError(error.to_string()))?;
        admission
            .check(peak)
            .map_err(|error| crate::storage::StorageError::BackendError(error.to_string()))
    })
    .map_err(|_| error("cloud replacement cannot admit the exact value metadata under the memory guard; retain the source and retry"))
}

pub(crate) fn outer_bound(size: laststore::PointValueSize) -> u64 {
    if size.may_inflate {
        logical_row_bound(size.bytes)
    } else {
        size.bytes
    }
}

pub(crate) fn read_peak(size: laststore::PointValueSize) -> SyncResult<u64> {
    checked_sum(&[row_peak(size.bytes)?, size.open_peak])
}

/// Main-row copies do not parse a second encrypted file body. Only the
/// observed outer codec can inflate; deletion inverses retain outer rows.
pub(crate) fn body_peak(size: laststore::PointValueSize) -> SyncResult<u64> {
    checked_sum(&[
        checked_scale(outer_bound(size), 8)?,
        size.open_peak,
        4 * 1024 * 1024,
    ])
}

pub(crate) fn keys_peak(
    admission: &Admission,
    store: &dyn crate::storage::NamespacedStore,
    namespace: &str,
    keys: &[String],
) -> SyncResult<u64> {
    let sizes = raw_lengths(admission, 0, store, namespace, keys)?;
    let bound = sizes
        .iter()
        .flatten()
        .try_fold(0, |sum, size| checked_sum(&[sum, outer_bound(*size), 512]))?;
    checked_sum(&[
        checked_scale(bound, 16)?,
        sizes
            .iter()
            .flatten()
            .map(|size| size.open_peak)
            .max()
            .unwrap_or(0),
    ])
}

pub(crate) fn reserve_pair(
    admission: &Admission,
    source: &dyn crate::storage::NamespacedStore,
    destination: &dyn crate::storage::NamespacedStore,
    namespace: &str,
    keys: &[String],
) -> SyncResult<()> {
    let source = keys_peak(admission, source, namespace, keys)?;
    let destination = keys_peak(admission, destination, namespace, keys)?;
    admission.check(checked_sum(&[source, destination])?)
}

fn error(message: &str) -> SyncError {
    SyncError::Storage(message.into())
}
