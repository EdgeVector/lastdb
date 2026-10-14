use super::*;
use fold_db::fold_db_core::AutomaticGcAtomsStatus;

/// A tick this many step intervals old (and at least five minutes) means the
/// cadence task is no longer running, whatever its last outcome said.
const GC_ATOMS_STALE_TICK_INTERVALS: u64 = 10;
const GC_ATOMS_STALE_TICK_FLOOR_SECS: u64 = 300;

/// `lastdb status` lines for the automatic `gc-atoms` cadence.
///
/// Always emits at least one line. A silent cadence and a cadence that never
/// started must not read the same.
pub fn gc_atoms_status_lines(snapshot: &StatusSnapshot) -> Vec<String> {
    match snapshot.gc_atoms.as_ref() {
        Some(status) => gc_atoms_lines_at(status, fold_db::clock::unix_secs()),
        None => {
            vec!["GC atoms: unsupported by daemon (no cadence status; older daemon)".to_string()]
        }
    }
}

/// Time-explicit form of [`gc_atoms_status_lines`], so the age arithmetic has
/// one clock.
pub fn gc_atoms_lines_at(status: &AutomaticGcAtomsStatus, now: u64) -> Vec<String> {
    let age = |at: u64| -> String {
        if at == 0 {
            "never".to_string()
        } else {
            format!("{} ago", format_age_secs(now.saturating_sub(at)))
        }
    };
    let engine = if status.sync_engine_attached {
        "attached"
    } else {
        "absent"
    };
    let in_step = if status.step_in_progress {
        format!(
            " in_step={}",
            format_age_secs(now.saturating_sub(status.step_started_at_unix_secs))
        )
    } else {
        String::new()
    };
    let bypass = if status.grace_bypass {
        " BYPASS_GRACE=ON(cap-armed pass deletes without grace or audit)"
    } else {
        ""
    };
    let mut head = format!(
        "GC atoms: loop={} cap={} step_interval={}s grace={}{bypass} sync_engine={engine} steps={} last_tick={} last_step={}{in_step} outcome={} skip_reason={}",
        gc_atoms_loop_state(status, now),
        format_bytes(status.max_unreferenced_bytes),
        status.step_interval_secs,
        format_age_secs(status.grace_window_secs),
        status.steps,
        age(status.last_tick_at_unix_secs),
        age(status.last_step_at_unix_secs),
        or_none(&status.last_outcome),
        status.last_skip_reason.as_deref().unwrap_or("none"),
    );
    head.push_str(&format!(
        " peer_apply={}",
        if status.peer_apply_state.is_empty() {
            "unread"
        } else {
            status.peer_apply_state.as_str()
        }
    ));
    if status.peer_interlock_skipped_steps > 0 {
        head.push_str(&format!(
            " peer_interlock_skips={}",
            status.peer_interlock_skipped_steps
        ));
    }
    if status.allow_after_peer_apply {
        head.push_str(
            " ALLOW_AFTER_PEER_APPLY=ON(atoms of lost multi-device writes can be deleted)",
        );
    }
    if status.last_progress_at_unix_secs != 0 {
        head.push_str(&format!(
            " last_progress={}",
            age(status.last_progress_at_unix_secs)
        ));
    }
    if let Some(reason) = status.last_stale_lap_restart_reason.as_deref() {
        head.push_str(&format!(
            " stale_lap_restarts={} (last: {reason})",
            status.stale_lap_restarts
        ));
    }
    if let Some(error) = status.last_error.as_deref() {
        head.push_str(&format!(
            " last_error={error:?} ({})",
            age(status.last_error_at_unix_secs)
        ));
    }
    vec![
        head,
        gc_atoms_lap_line(status, now, &age),
        gc_atoms_delete_line(status, &age),
        gc_atoms_pass_line(status),
    ]
}

fn or_none(text: &str) -> &str {
    if text.is_empty() {
        "none"
    } else {
        text
    }
}

fn gc_atoms_loop_state(status: &AutomaticGcAtomsStatus, now: u64) -> &'static str {
    if !status.running {
        return if status.max_unreferenced_bytes == 0 {
            "disabled"
        } else {
            "not-started"
        };
    }
    if status.last_tick_at_unix_secs == 0 {
        return "starting";
    }
    // A step in progress is the loop working: judge staleness from the later
    // of the last tick and the start of the running step.
    let last_alive = status
        .last_tick_at_unix_secs
        .max(status.step_started_at_unix_secs);
    let stale_after = status
        .step_interval_secs
        .saturating_mul(GC_ATOMS_STALE_TICK_INTERVALS)
        .max(GC_ATOMS_STALE_TICK_FLOOR_SECS);
    if now.saturating_sub(last_alive) > stale_after {
        "STALE"
    } else {
        "running"
    }
}

fn gc_atoms_lap_line(
    status: &AutomaticGcAtomsStatus,
    now: u64,
    age: &dyn Fn(u64) -> String,
) -> String {
    let measured = if status.last_lap_completed_at_unix_secs == 0 {
        "unreferenced=unmeasured (no lap has completed in this home)".to_string()
    } else {
        format!(
            "unreferenced={} in {} atoms of {} scanned (lap #{} finished {}, took {})",
            format_bytes(status.last_lap_unreferenced_bytes),
            status.last_lap_unreferenced_atoms,
            status.last_lap_atoms_scanned,
            status.last_lap_generation,
            age(status.last_lap_completed_at_unix_secs),
            format_age_secs(status.last_lap_duration_secs),
        )
    };
    let probe = match status.lap_phase.as_deref() {
        Some(phase) if phase != "complete" => format!(
            "probe lap #{} phase={phase} pages={} started={} slowest_call={}ms",
            status.lap_generation,
            status.lap_pages_completed,
            lap_started_age(status, now),
            status.slowest_probe_call_ms,
        ),
        _ if status.lap_generation == 0 => "probe has not started a lap".to_string(),
        _ => format!(
            "probe idle (last lap complete; slowest_call={}ms)",
            status.slowest_probe_call_ms
        ),
    };
    format!("  {measured}; {probe}")
}

fn lap_started_age(status: &AutomaticGcAtomsStatus, now: u64) -> String {
    let started = status
        .lap_started_at
        .as_deref()
        .and_then(|text| chrono::DateTime::parse_from_rfc3339(text).ok())
        .and_then(|time| u64::try_from(time.timestamp()).ok());
    started.map_or_else(
        || "unknown".to_string(),
        |at| format!("{} ago", format_age_secs(now.saturating_sub(at))),
    )
}

/// The delete pass line: what it rechecked and why it kept what it kept.
fn gc_atoms_pass_line(status: &AutomaticGcAtomsStatus) -> String {
    let kept = &status.last_pass_candidates_retained;
    format!(
        "  last_delete_pass: rechecked={} kept_by marker={} body_changed={} no_zero_epoch={} within_grace={} audit_not_clear={} reads_not_ready={} active_edge={} pending_hold={} retired_intent={} unreadable={}",
        status.last_pass_candidates_revalidated,
        kept.referenced_marker,
        kept.body_changed,
        kept.no_zero_epoch,
        kept.within_grace,
        kept.audit_not_clear,
        kept.reads_not_ready,
        kept.active_edge,
        kept.pending_hold,
        kept.retired_intent_root,
        kept.unreadable,
    )
}

fn gc_atoms_delete_line(status: &AutomaticGcAtomsStatus, age: &dyn Fn(u64) -> String) -> String {
    format!(
        "  slowest_delete_batch={}ms last_delete={} ({} atoms, {}) total_deleted={} atoms ({}); reap {}: examined={} deleted={} retained_before_grace={} retained_without_audit={} retained_audit_reachable={} retained_stale_audit={} retained_error={}; audit_deferred={} audit_failed={} audit_cut_short_steps={}",
        status.slowest_delete_batch_ms,
        age(status.last_delete_at_unix_secs),
        status.last_delete_atoms,
        format_bytes(status.last_delete_bytes_approx),
        status.total_atoms_deleted,
        format_bytes(status.total_bytes_deleted_approx),
        age(status.last_reap_at_unix_secs),
        status.last_reap_examined,
        status.last_reap_deleted,
        status.last_reap_retained_before_grace,
        status.last_reap_retained_without_audit,
        status.last_reap_retained_audit_reachable,
        status.last_reap_retained_stale_audit,
        status.last_reap_retained_error,
        status.last_audit_deferred,
        status.audit_failed,
        status.audit_cut_short_steps,
    )
}
