//! vmmap capture, stepwise eviction helpers and operator shed. Moved verbatim from `footprint.rs`.

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

/// Whether stepwise eviction may stop.
///
/// The footprint clause is "at or under `target_bytes`" **and** the implied
/// charge/measured multiplier under
/// [`fold_db::memory_budget::FOOTPRINT_EVICT_STOP_MULTIPLIER`]. Footprint
/// alone can sit on the target while uncounted per-handle overhead is still
/// climbing relative to the charged budget, so both must clear.
///
/// `suppress_floor` is the pressure drain: while footprint is still above the
/// 4 GiB stop, `resident_bytes <= floor_bytes` is not success. The 1 GiB floor
/// would otherwise end the pass with the set still pinned near the env.
///
/// Pure so the stop rule is testable against fabricated measurements without
/// a live `Host` or a real `phys_footprint` reader.
pub(super) fn footprint_evict_should_stop(
    footprint_bytes: u64,
    charged_bytes: u64,
    resident_bytes: u64,
    floor_bytes: u64,
    target_bytes: u64,
    suppress_floor: bool,
) -> bool {
    if !suppress_floor && resident_bytes <= floor_bytes {
        return true;
    }
    let multiplier = if charged_bytes > 0 {
        footprint_bytes as f64 / charged_bytes as f64
    } else {
        0.0
    };
    footprint_bytes <= target_bytes
        && multiplier < fold_db::memory_budget::FOOTPRINT_EVICT_STOP_MULTIPLIER
}

/// Warm bytes one eviction step should try to free. Targets the footprint
/// overshoot above `target_bytes`, clamped so a step is never a one-group
/// trickle (measured live: 1.8 KB to 4 MB freed per step against a 2-5 GiB
/// overshoot) and never a whole-set drop that holds a barrier for minutes.
pub(super) const FOOTPRINT_EVICT_MIN_STEP_BYTES: u64 = 64 * 1024 * 1024;
pub(super) const FOOTPRINT_EVICT_MAX_STEP_BYTES: u64 = 1024 * 1024 * 1024;

pub(super) fn footprint_evict_step_bytes(footprint_bytes: u64, target_bytes: u64) -> u64 {
    footprint_bytes.saturating_sub(target_bytes).clamp(
        FOOTPRINT_EVICT_MIN_STEP_BYTES,
        FOOTPRINT_EVICT_MAX_STEP_BYTES,
    )
}

/// Eviction target for one step.
///
/// Pressure may go under the floor. A clear host stays at or above the floor,
/// and does not aim above `clear_ceiling`. That ceiling is the budget
/// `next_effective_warm_bytes` just stored. A zero ceiling means the warm
/// budget is off, so the step target stands.
pub(super) fn footprint_evict_to_bytes(
    pressure_high: bool,
    resident_bytes: u64,
    step_bytes: u64,
    floor_bytes: u64,
    clear_ceiling: u64,
) -> u64 {
    if pressure_high {
        return resident_bytes.saturating_sub(step_bytes);
    }
    let stepped = resident_bytes.saturating_sub(step_bytes).max(floor_bytes);
    if clear_ceiling == 0 {
        stepped
    } else {
        stepped.min(clear_ceiling.max(floor_bytes))
    }
}

/// Budget stored for `fits` after one eviction step.
///
/// A miss reports `bytes_after` as the pre-step resident. That pin must not
/// replace a lower target. Pressure keeps `min(bytes_after, evict_to)` and
/// does not restore the floor. A clear host also stays under `clear_ceiling`
/// and at or above the floor.
pub(super) fn published_warm_budget_after_step(
    pressure_high: bool,
    bytes_after: u64,
    evict_to: u64,
    clear_ceiling: u64,
    floor_bytes: u64,
) -> u64 {
    let kept = bytes_after.min(evict_to).max(1);
    if pressure_high || clear_ceiling == 0 {
        if pressure_high {
            kept
        } else {
            kept.max(floor_bytes)
        }
    } else {
        kept.min(clear_ceiling.max(1)).max(floor_bytes)
    }
}

/// Drain deferred writes, trim the warm set to the floor, and release pages.
pub async fn shed_memory(host: &Host) -> ShedReport {
    let deferred_idle = host
        .db
        .mutation_manager()
        .wait_deferred_idle(std::time::Duration::from_secs(30))
        .await;
    let store = host.db.db_ops().namespaced_store();
    let budget = fold_db::memory_budget::process_memory_budget();
    let floor = fold_db::memory_budget::effective_warm_floor_bytes(budget.warm_bytes);
    begin_shed_cooldown();
    store.set_effective_warm_bytes(floor);
    let (groups_evicted, warm_bytes_before, warm_bytes_after) =
        match store.evict_warm_set_to_bytes(floor) {
            Ok(Some(report)) => (
                report.groups_evicted,
                report.bytes_before,
                report.bytes_after,
            ),
            Ok(None) => (0, 0, 0),
            Err(error) => {
                tracing::warn!(
                    target: "lastdbd::footprint",
                    error = %error,
                    "shed warm-set trim failed"
                );
                (0, 0, 0)
            }
        };
    let malloc_bytes_released = crate::allocator::purge();
    let phys_footprint_bytes = fold_db::memory_budget::current_phys_footprint_bytes();
    if let Some(bytes) = phys_footprint_bytes {
        maybe_capture_vmmap(&host.home, bytes);
    }
    ShedReport {
        deferred_idle,
        groups_evicted,
        warm_bytes_before,
        warm_bytes_after,
        effective_warm_bytes: floor,
        malloc_bytes_released,
        phys_footprint_bytes,
    }
}
