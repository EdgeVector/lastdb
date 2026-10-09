use super::BackupProgressSnapshot;

/// Format a short human progress line for `lastdb status` (no trailing newline).
pub fn format_backup_progress_line(snap: &BackupProgressSnapshot) -> Option<String> {
    // An enabled uploader with a live failure streak is the case an operator
    // most needs to see, and it must be reported BEFORE the progress-bar path.
    // Note this fires even when the chunk bar reads complete: all sealed chunks
    // can be present in cloud while every CAS publish fails, and that home is
    // not restorable at the cut it thinks it is. Printing "100%" there is the
    // user-visible face of the streak-laundering bug.
    if snap.enabled && snap.consecutive_failures > 0 {
        let detail = snap
            .last_error
            .as_deref()
            .map_or_else(String::new, |e| format!(": {e}"));
        return Some(format!(
            "Backup: FAILING — {} consecutive failed cycle(s){detail}",
            snap.consecutive_failures
        ));
    }
    // An uploader that has never completed a cycle has no chunk numbers to
    // render either, so it also precedes the progress bar.
    if snap.enabled && !snap.complete && !snap.show_progress {
        return Some("Backup: no cycle has completed yet (nothing confirmed in cloud)".to_string());
    }
    if !snap.show_progress {
        return None;
    }
    let pct = snap
        .percent
        .map_or_else(|| "?%".into(), |p| format!("{p:.1}%"));
    let mut parts = vec![format!(
        "Recut: {pct} ({}/{} sealed chunks in cloud) — in-flight photograph; not restore base until CAS",
        snap.chunks_present, snap.chunks_total
    )];
    if let Some(elapsed) = snap.elapsed_secs {
        parts.push(format!("elapsed {}", format_duration_secs(elapsed)));
    }
    match snap.eta_secs {
        Some(eta) => parts.push(format!("ETA ~{}", format_duration_secs(eta))),
        // "calculating…" is the right words for a drain that has not yet earned
        // an estimate, and exactly the wrong words for one that is going
        // backwards — it reads as "working on it" while the bar falls. When the
        // recent window is net-negative, say which way it is moving instead.
        None if snap.recent_erased > snap.recent_gained => {
            parts.push(format!(
                "LOSING GROUND — {} erased vs {} gained over the last {} cycle(s), \
                 none gained in {}",
                snap.recent_erased,
                snap.recent_gained,
                snap.net_progress_window_cycles,
                if snap.cycles_since_net_gain == 1 {
                    "1 cycle".to_string()
                } else {
                    format!("{} cycles", snap.cycles_since_net_gain)
                }
            ));
        }
        None => parts.push("ETA calculating…".into()),
    }
    Some(parts.join(", "))
}

fn format_duration_secs(secs: u64) -> String {
    if secs < 60 {
        return format!("{secs}s");
    }
    let m = secs / 60;
    let s = secs % 60;
    if m < 60 {
        return format!("{m}m{s:02}s");
    }
    let h = m / 60;
    let m = m % 60;
    format!("{h}h{m:02}m")
}
