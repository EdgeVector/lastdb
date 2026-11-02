//! Process-memory capture and explicit native warm record relief.

use super::*;

/// Capture `vmmap -summary` once per crossing of the 12 GiB line.
pub fn maybe_capture_vmmap(home: &Path, footprint_bytes: u64) {
    static CROSSED: AtomicBool = AtomicBool::new(false);
    if footprint_bytes < fold_db::memory_budget::FOOTPRINT_VMMAP_CAPTURE_BYTES {
        CROSSED.store(false, Ordering::Release);
        return;
    }
    if CROSSED.swap(true, Ordering::AcqRel) {
        return;
    }
    let monitoring = home.join("monitoring");
    if let Err(error) = std::fs::create_dir_all(&monitoring) {
        tracing::warn!(
            target: "lastdbd::footprint",
            error = %error,
            "could not create monitoring dir for vmmap capture"
        );
        return;
    }
    let ts = fold_db::clock::unix_secs();
    let dest = monitoring.join(format!("footprint-{ts}.txt"));
    let pid = std::process::id().to_string();
    match Command::new("/usr/bin/vmmap")
        .args(["-summary", &pid])
        .output()
    {
        Ok(output) => {
            let mut body = output.stdout;
            if !output.stderr.is_empty() {
                body.extend_from_slice(b"\n--- stderr ---\n");
                body.extend_from_slice(&output.stderr);
            }
            if let Err(error) = std::fs::write(&dest, body) {
                tracing::warn!(
                    target: "lastdbd::footprint",
                    error = %error,
                    path = %dest.display(),
                    "vmmap capture write failed"
                );
            } else {
                tracing::warn!(
                    target: "lastdbd::footprint",
                    path = %dest.display(),
                    footprint_gib = footprint_bytes / GIB,
                    "captured vmmap -summary after footprint crossed 12 GiB"
                );
            }
        }
        Err(error) => {
            tracing::warn!(
                target: "lastdbd::footprint",
                error = %error,
                "vmmap -summary failed"
            );
        }
    }
}

/// Bound each pressure step so a tick makes useful progress without a large
/// lock section. The target refers only to warm record bytes.
pub(super) const FOOTPRINT_EVICT_MIN_STEP_BYTES: u64 = 64 * 1024 * 1024;
pub(super) const FOOTPRINT_EVICT_MAX_STEP_BYTES: u64 = 1024 * 1024 * 1024;

pub(super) fn footprint_evict_step_bytes(footprint_bytes: u64, target_bytes: u64) -> u64 {
    footprint_bytes.saturating_sub(target_bytes).clamp(
        FOOTPRINT_EVICT_MIN_STEP_BYTES,
        FOOTPRINT_EVICT_MAX_STEP_BYTES,
    )
}

/// Flush accepted writes, remove unused clean records, then release free pages.
pub async fn shed_memory(host: &Host) -> ShedReport {
    let timeout = std::time::Duration::from_secs(30);
    let flush = tokio::time::timeout(timeout, host.db.flush_accepted_mutations(timeout)).await;
    let (deferred_idle, mut error) = match flush {
        Ok(Ok(())) => (true, None),
        Ok(Err(error)) => (false, Some(format!("accepted write flush failed: {error}"))),
        Err(_) => (
            false,
            Some("accepted write flush exceeded 30 seconds".into()),
        ),
    };
    begin_shed_cooldown();
    let result = host
        .db
        .db_ops()
        .namespaced_store()
        .disk_pack_runtime()
        .ok_or_else(|| "native warm set is unavailable".to_string())
        .and_then(|runtime| {
            runtime
                .purge_clean_memory(0)
                .map_err(|error| error.to_string())
        });
    let (records_purged, warm_bytes_before, warm_bytes_after, target_met, native_memory) =
        match result {
            Ok(report) => (
                report.records_purged,
                report.bytes_before,
                report.bytes_after,
                report.target_met,
                Some(report.remaining),
            ),
            Err(failure) => {
                error = Some(error.map_or(failure.clone(), |prior| format!("{prior}; {failure}")));
                (0, 0, 0, false, None)
            }
        };
    let malloc_bytes_released = crate::allocator::purge();
    let phys_footprint_bytes = fold_db::memory_budget::current_phys_footprint_bytes();
    if let Some(bytes) = phys_footprint_bytes {
        maybe_capture_vmmap(&host.home, bytes);
    }
    ShedReport {
        deferred_idle,
        records_purged,
        warm_bytes_before,
        warm_bytes_after,
        target_met,
        native_memory,
        error,
        malloc_bytes_released,
        phys_footprint_bytes,
    }
}
