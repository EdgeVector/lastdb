//! Timer-owned retry after a copy that cannot restore admission yet.
use super::*;
use std::time::Duration;

/// Longest pause between failed packs or unsuccessful soft-limit recovery.
pub(super) const MAX_BACKOFF_SECS: u64 = 60;

/// A published copy can retain concurrent grafts above the soft limit. Retry
/// without new growth; refused-write notifications cannot bypass this back-off.
pub(super) async fn after_pack(
    ledger: &PartsLedger,
    report: &NativeMaintenanceReport,
    failures: &mut u32,
) -> bool {
    if !ledger.over_soft_limit() {
        *failures = 0;
        return report.parts_after >= report.parts_before;
    }
    *failures = failures.saturating_add(1);
    let pause = (1u64 << (*failures).min(6)).min(MAX_BACKOFF_SECS);
    let error = StorageError::WriteResourceLimit {
        detail: format!(
            "compaction_behind: maintenance left {} storage parts at the soft limit; retry in {pause} seconds",
            ledger.current.load(Relaxed)
        ),
    };
    ledger.record_failure(&error);
    tracing::warn!(%error, "native storage maintenance needs another full copy");
    tokio::time::sleep(Duration::from_secs(pause)).await;
    false
}
