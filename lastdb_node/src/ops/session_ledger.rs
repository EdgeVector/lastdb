//! Session / uptime ledger — "how long was the brain down, and did the last
//! session end cleanly?" answerable in one read.
//!
//! ## Why this exists
//!
//! Incident 2026-06-19: the `:9001` brain's last heartbeat was 04:32:37; it
//! returned as a new PID at 10:47:39 after an *unclean machine reboot at 06:17*
//! (no clean shutdown record) and a stale-lock takeover. Reconstructing the
//! 6h15m outage took a multi-step forensic scan of `observability.jsonl` plus
//! `pmset`/`last`/`sw_vers`. None of "previous session ended uncleanly", "down
//! for 6h15m", or "prior shutdown cause" was recorded by FoldDB itself. This
//! module makes the node record exactly that, on every start.
//!
//! ## What it records
//!
//! Two files under `$FOLDDB_HOME`:
//!
//! - `sessions.jsonl` — append-only ledger, one [`SessionRecord`] per line. The
//!   newest line is the current session. A graceful shutdown first records
//!   `exit: "shutdown_started"`, then promotes that disposition to `"clean"`
//!   after the drain and final flush complete. A record with no disposition was
//!   a session that died uncleanly (SIGKILL, panic, power loss, OS reboot).
//! - `current-session.json` — the live session's mutable heartbeat. Rewritten
//!   cheaply every ~60s ([`Ledger::heartbeat`]) so the *next* start can compute
//!   downtime from `last_heartbeat_ts` even when the prior process never got to
//!   write an `end_ts`. We keep heartbeats out of the append-only ledger so the
//!   ledger stays one-line-per-session and small.
//!
//! ## Lifecycle
//!
//! 1. [`Ledger::record_start`] at boot — reads the prior session, computes
//!    clean/unclean + downtime, captures the OS shutdown cause, appends the new
//!    record, and returns a [`Ledger`] handle plus a [`StartSummary`].
//! 2. [`Ledger::heartbeat`] every ~60s (a [`Ledger::spawn_heartbeat`] thread is
//!    the easy default) — rewrites `current-session.json` with a fresh
//!    `last_heartbeat_ts`.
//! 3. [`mark_clean_shutdown`] on graceful exit (Tauri tray Quit / SIGTERM) —
//!    stamps `end_ts` + `exit: "clean"` onto the current session's ledger line.
//!
//! Everything is best-effort: a node must never fail to start because the
//! ledger couldn't be written. Errors are surfaced to the caller (which logs a
//! WARN) but never propagated as a hard failure of boot.

use fold_db::clock::unix_secs;
use std::io::{BufRead, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

mod heartbeat;
pub use heartbeat::SessionHeartbeat;
#[path = "session_ledger/ledger.rs"]
mod ledger;
#[path = "session_ledger/marks.rs"]
mod marks;
#[path = "session_ledger/read.rs"]
mod read;
#[path = "session_ledger/receipt.rs"]
mod receipt;
#[path = "session_ledger/record.rs"]
mod record;
#[path = "session_ledger/restart.rs"]
mod restart;
#[path = "session_ledger/shutdown_cause.rs"]
mod shutdown_cause;

pub use ledger::*;
pub use marks::*;
pub use read::*;
pub use receipt::*;
pub use record::*;
use restart::*;
use shutdown_cause::*;

/// Exit disposition of a session, as recorded in the ledger.
pub const EXIT_CLEAN: &str = "clean";
/// A supervisor-requested shutdown reached the ledger before the async drain.
pub const EXIT_SHUTDOWN_STARTED: &str = "shutdown_started";
/// The process returned a fatal error from its own startup/serve path (store
/// wouldn't open, socket wouldn't bind, accept loop died). The exit was
/// controlled and self-diagnosed — the opposite of a native crash.
pub const EXIT_STARTUP_FAILED: &str = "startup_failed";

/// One-use supervisor annotation consumed by the next daemon start.
pub const RESTART_INTENT_FILE: &str = "restart-intent.json";

fn rewrite_ledger(home: &Path, records: &[SessionRecord]) -> std::io::Result<()> {
    std::fs::create_dir_all(home)?;
    let mut body = String::new();
    for r in records {
        let line = serde_json::to_string(r).map_err(std::io::Error::other)?;
        body.push_str(&line);
        body.push('\n');
    }
    let path = Ledger::ledger_path(home);
    let tmp = path.with_extension("jsonl.tmp");
    std::fs::write(&tmp, body)?;
    std::fs::rename(&tmp, &path)
}

/// Human-readable duration: "6h15m", "45s", "2d3h". Compact, for log lines and
/// the `folddb sessions` table.
pub fn format_duration(secs: u64) -> String {
    if secs == 0 {
        return "0s".to_string();
    }
    let days = secs / 86_400;
    let hours = (secs % 86_400) / 3_600;
    let mins = (secs % 3_600) / 60;
    let s = secs % 60;
    let mut out = String::new();
    if days > 0 {
        out.push_str(&format!("{days}d"));
    }
    if hours > 0 {
        out.push_str(&format!("{hours}h"));
    }
    if mins > 0 {
        out.push_str(&format!("{mins}m"));
    }
    // Only show seconds for sub-hour durations to keep long gaps compact.
    if s > 0 && days == 0 && hours == 0 {
        out.push_str(&format!("{s}s"));
    }
    if out.is_empty() {
        out.push_str("0s");
    }
    out
}
