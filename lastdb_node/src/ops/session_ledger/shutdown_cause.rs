//! OS shutdown cause capture. Moved verbatim from `session_ledger.rs`.

/// Whether [`record_start`](Ledger::record_start) should pay the multi-second
/// `pmset -g log` shutdown-cause capture. Only when there IS a previous
/// session and it ended unclean — that's the one case where "did the machine
/// die under us" is the question. Clean relaunches and first runs skip it so
/// the capture never taxes a normal launch.
pub(super) fn should_capture_shutdown_cause(
    had_previous_session: bool,
    prev_session_exit_recorded: bool,
) -> bool {
    had_previous_session && !prev_session_exit_recorded
}

/// Capture the OS "previous shutdown cause" at startup.
///
/// macOS records a numeric shutdown cause in its boot log; we read the most
/// recent `Previous shutdown cause: <N>` line via `pmset -g log` and translate
/// the well-known codes. A non-negative code (0, 5) is a clean/normal shutdown;
/// a negative code (e.g. -128 unknown, -60 SMC, -3 hard power loss) indicates an
/// unexpected/uncontrolled shutdown — exactly the "machine rebooted out from
/// under us" signal the 2026-06-19 incident lacked.
///
/// Best-effort and gated behind `cfg(target_os = "macos")`; other platforms
/// return `None`. We shell out to `pmset` (always present on macOS, no extra
/// privileges) rather than parsing NVRAM/ioreg, which needs root for the
/// equivalent field.
#[cfg(target_os = "macos")]
pub(super) fn capture_shutdown_cause() -> Option<String> {
    let output = std::process::Command::new("/usr/bin/pmset")
        .args(["-g", "log"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    // The log is chronological; the LAST "Previous shutdown cause" line is the
    // most recent boot's.
    let code: i64 = text
        .lines()
        .rev()
        .find_map(|line| {
            line.find("Previous shutdown cause:").map(|idx| {
                line[idx + "Previous shutdown cause:".len()..]
                    .split_whitespace()
                    .next()
                    .unwrap_or("")
                    .parse::<i64>()
            })
        })?
        .ok()?;
    Some(describe_shutdown_cause(code))
}

#[cfg(not(target_os = "macos"))]
pub(super) fn capture_shutdown_cause() -> Option<String> {
    // Best-effort, macOS-only: no portable "previous shutdown cause" equivalent
    // on Linux/Windows that doesn't need root or extra crates. Stored as None
    // with this note so a future port has a clear seam.
    None
}

/// Translate a macOS `Previous shutdown cause` code into a short human label,
/// keeping the raw code so an unrecognized value is still actionable. Codes are
/// the documented `gIOPMStatsResponseTimedOut`-adjacent values surfaced by
/// `pmset -g log` / the SMC; the common ones:
///   - `0`  : clean shutdown (loss of system power requested)
///   - `5`  : normal user-initiated shutdown
///   - `3`  : hard shutdown (power button held / forced)
///   - `-3` : hardware/power loss (e.g. battery died, power cut)
///   - `-60`: SMC-detected uncontrolled shutdown
///   - `-128`: unknown / no record (often a kernel panic or reset)
#[cfg(target_os = "macos")]
pub(super) fn describe_shutdown_cause(code: i64) -> String {
    let label = match code {
        0 => "clean shutdown",
        5 => "normal shutdown",
        3 => "forced shutdown (power button held)",
        -3 => "power loss / hardware shutdown",
        -60 => "uncontrolled shutdown (SMC)",
        -128 => "unknown (panic / reset)",
        _ => "unrecognized",
    };
    format!("{code} ({label})")
}
