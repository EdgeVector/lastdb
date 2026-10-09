//! On-panic crash reports for long-running node binaries.
//!
//! When the FoldDB node dies on a Rust panic, macOS leaves no fold-specific
//! crash report and the unified log rotates past the window — so an incident
//! after the fact has nothing to read but a gap in `observability.jsonl` (the
//! 2026-06-19 :9001 outage). This module installs a chained
//! [`std::panic::set_hook`] that drops a durable, human-readable report under
//! `$FOLDDB_HOME/crash-reports/<rfc3339-utc>.txt` on every panic, and surfaces
//! any unseen reports on the next start.
//!
//! ## What the hook writes
//!
//! - the panic message + source location,
//! - a force-captured [`std::backtrace::Backtrace`],
//! - the build version, selected HTTP port, and OS/arch,
//! - the last ~200 lines of the in-memory observability RING buffer.
//!
//! ## Panic-safety
//!
//! The hook runs on the panicking thread, possibly while another panic is
//! unwinding. It must not allocate heavily, must not re-panic, and must not
//! block. Every fallible step is best-effort: write errors are swallowed, the
//! RING tail is bounded, and the report directory is created lazily. The
//! default hook is always invoked first so console behaviour (and the Sentry
//! panic integration, where wired) is unchanged.
//!
//! ## Retention
//!
//! Reports are capped at [`MAX_CRASH_REPORTS`] newest files; older ones are
//! pruned on the next [`scan_and_warn_previous_crashes`] (i.e. at startup),
//! never inside the panic hook.

use std::backtrace::Backtrace;
use std::fs;
use std::io::Write;
use std::panic::PanicHookInfo;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::layers::ring::RingHandle;

/// Sub-directory under `$FOLDDB_HOME` where crash reports are written.
pub const CRASH_REPORTS_DIR: &str = "crash-reports";

/// How many lines of the observability RING buffer to embed in a report.
const RING_TAIL_LINES: usize = 200;

/// Maximum number of crash-report files to retain. Pruned at startup
/// (newest-first) — never inside the panic hook.
pub const MAX_CRASH_REPORTS: usize = 20;

/// Marker file recording the wall-clock of the most recent clean start, used
/// by [`scan_and_warn_previous_crashes`] to decide which reports are "new"
/// (written since the last time the operator saw a startup). Lives alongside
/// the reports so a single `FOLDDB_HOME` scopes both.
const LAST_START_MARKER: &str = ".last-clean-start";

/// Callback invoked (best-effort) right after a crash report is written to
/// disk, with the report's path. The observability crate stays free of any
/// telemetry dependency: a binary that wants to *ship* the report at crash
/// time (rather than on the next launch) injects the sender here. Runs on the
/// panicking thread inside the hook, so — like the hook itself — it must be
/// panic-safe and time-bounded; it must never re-panic.
pub type OnReportWritten = Arc<dyn Fn(&Path) + Send + Sync + 'static>;

/// Immutable context the panic hook needs to compose a report. Cheap to clone
/// (the [`RingHandle`] is `Arc`-backed); captured once at install time so the
/// hook closure owns everything it touches and never reaches back into
/// process-global state that might itself be mid-panic.
#[derive(Clone)]
pub struct CrashContext {
    /// Directory crash reports are written to (`$FOLDDB_HOME/crash-reports`).
    dir: PathBuf,
    /// Build version string (e.g. `env!("CARGO_PKG_VERSION")`).
    version: Arc<str>,
    /// Selected HTTP port, when known at install time.
    port: Option<u16>,
    /// Handle to the in-memory RING buffer, for the tail-of-log section.
    ring: Option<RingHandle>,
    /// Optional post-write hook — see [`OnReportWritten`]. `None` (the default)
    /// preserves the write-only behaviour every existing caller relies on.
    on_report_written: Option<OnReportWritten>,
}

impl CrashContext {
    /// Build a context rooted at `folddb_home/crash-reports`.
    pub fn new(
        folddb_home: impl AsRef<Path>,
        version: &str,
        port: Option<u16>,
        ring: Option<RingHandle>,
    ) -> Self {
        Self {
            dir: folddb_home.as_ref().join(CRASH_REPORTS_DIR),
            version: Arc::from(version),
            port,
            ring,
            on_report_written: None,
        }
    }

    /// Attach a callback run right after each report is written (with its
    /// path). Used by the desktop shell to send the report to Sentry *at crash
    /// time* instead of waiting for the next launch. Must be panic-safe and
    /// time-bounded (it runs inside the panic hook).
    #[must_use]
    pub fn with_on_report_written(mut self, cb: OnReportWritten) -> Self {
        self.on_report_written = Some(cb);
        self
    }

    /// The crash-reports directory this context writes to.
    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

/// Install the process-wide panic hook that writes a crash report on every
/// panic. Chains to (calls) the previously-installed hook first, so console
/// output and any registered panic integrations are preserved.
///
/// Idempotent in spirit: calling twice simply re-chains, but binaries should
/// call it exactly once, as early as the RING handle is available.
pub fn install_crash_hook(ctx: CrashContext) {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        // Default hook first: preserve console backtrace + Sentry capture.
        previous(info);
        // Best-effort report write. Never propagate an error out of the hook.
        if let Ok(path) = write_crash_report(&ctx, info) {
            // Optional crash-time delivery. Runs on the panicking thread; the
            // callback owns its own panic-safety + time bound (a shipped build
            // sends the report to Sentry here, bounded by a flush timeout, so
            // a crash is reported the instant it happens rather than on the
            // next launch). Under `panic = "abort"` this hook is the last code
            // that runs, so a bounded send here still completes before exit.
            if let Some(cb) = &ctx.on_report_written {
                cb(&path);
            }
        }
    }));
}

/// Compose and write a single crash report. Returns the path written on
/// success. Best-effort: the caller (the panic hook) discards the result.
fn write_crash_report(ctx: &CrashContext, info: &PanicHookInfo<'_>) -> std::io::Result<PathBuf> {
    fs::create_dir_all(&ctx.dir)?;

    let now = SystemTime::now();
    let epoch_ms = now
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64);
    let rfc3339 = format_rfc3339_utc(epoch_ms);
    // `:` is legal on the filesystems we target but awkward to type; use a
    // filename-safe variant while keeping the human RFC3339 form inside.
    let stem = rfc3339.replace(':', "-");
    // Two panics in the same millisecond (a panicking thread pool unwinding
    // several workers at once) would otherwise collide on the same filename
    // and clobber each other. Append a numeric uniquifier when the base path
    // already exists so every panic leaves its own report.
    let mut path = ctx.dir.join(format!("{stem}.txt"));
    let mut n = 1;
    while path.exists() {
        path = ctx.dir.join(format!("{stem}-{n}.txt"));
        n += 1;
    }

    let payload = compose_report(ctx, info, &rfc3339);

    // Truncate-create so a re-run can't append onto a partial file.
    let mut file = fs::File::create(&path)?;
    file.write_all(payload.as_bytes())?;
    let _ = file.flush();
    Ok(path)
}

/// Build the report body. Pure (no IO beyond the RING read) and panic-safe so
/// it can be unit-tested directly.
fn compose_report(ctx: &CrashContext, info: &PanicHookInfo<'_>, rfc3339: &str) -> String {
    let mut out = String::with_capacity(4096);

    out.push_str("FoldDB crash report\n");
    out.push_str("===================\n");
    out.push_str(&format!("time:    {rfc3339}\n"));
    out.push_str(&format!("version: {}\n", ctx.version));
    out.push_str(&format!(
        "port:    {}\n",
        ctx.port
            .map_or_else(|| "unknown".to_string(), |p| p.to_string())
    ));
    out.push_str(&format!("os:      {}\n", std::env::consts::OS));
    out.push_str(&format!("arch:    {}\n", std::env::consts::ARCH));
    out.push('\n');

    // Panic message + location.
    out.push_str("panic\n-----\n");
    out.push_str(&format!("message:  {}\n", panic_message(info)));
    match info.location() {
        Some(loc) => out.push_str(&format!(
            "location: {}:{}:{}\n",
            loc.file(),
            loc.line(),
            loc.column()
        )),
        None => out.push_str("location: <unknown>\n"),
    }
    out.push('\n');

    // Force-capture a backtrace regardless of RUST_BACKTRACE — a crash report
    // with no backtrace is the exact gap this feature exists to close.
    out.push_str("backtrace\n---------\n");
    let bt = Backtrace::force_capture();
    out.push_str(&bt.to_string());
    if !out.ends_with('\n') {
        out.push('\n');
    }
    out.push('\n');

    // Tail of the in-memory event log.
    out.push_str(&format!(
        "observability ring (last {RING_TAIL_LINES} events)\n"
    ));
    out.push_str("------------------------------------------\n");
    match &ctx.ring {
        Some(ring) => {
            let lines = ring.tail_text(RING_TAIL_LINES);
            if lines.is_empty() {
                out.push_str("<ring buffer empty>\n");
            } else {
                for line in lines {
                    out.push_str(&line);
                    out.push('\n');
                }
            }
        }
        None => out.push_str("<ring buffer unavailable>\n"),
    }

    out
}

/// Extract the panic payload's string form. Handles the common `&str` /
/// `String` payloads; falls back to a placeholder for other types.
fn panic_message(info: &PanicHookInfo<'_>) -> String {
    let payload = info.payload();
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

/// Scan the crash-reports directory at startup, emit one `WARN` per report
/// written since the last clean start, then prune old reports and refresh the
/// clean-start marker.
///
/// Returns the paths of the reports that were surfaced (newest → oldest),
/// primarily for tests. Best-effort: a missing/unreadable directory yields an
/// empty result without error.
pub fn scan_and_warn_previous_crashes(folddb_home: impl AsRef<Path>) -> Vec<PathBuf> {
    let dir = folddb_home.as_ref().join(CRASH_REPORTS_DIR);
    let marker = dir.join(LAST_START_MARKER);

    let last_seen_ms = read_marker_ms(&marker);

    let mut reports = list_reports(&dir);
    // Newest first for the WARN output + retention walk.
    reports.sort_by_key(|r| std::cmp::Reverse(r.modified_ms));

    let mut surfaced = Vec::new();
    for report in &reports {
        if report.modified_ms > last_seen_ms {
            tracing::warn!(
                target: "observability::crash",
                path = %report.path.display(),
                "previous session left a crash report at {}",
                report.path.display()
            );
            surfaced.push(report.path.clone());
        }
    }

    prune_reports(&reports);
    write_marker_now(&marker);

    surfaced
}

/// A crash report file with its modification time (epoch ms) for ordering.
struct ReportFile {
    path: PathBuf,
    modified_ms: i64,
}

/// List `*.txt` reports in `dir` (ignores the marker + any non-report files).
fn list_reports(dir: &Path) -> Vec<ReportFile> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter_map(|entry| {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("txt") {
                return None;
            }
            let modified_ms = entry
                .metadata()
                .ok()
                .and_then(|m| m.modified().ok())
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map_or(0, |d| d.as_millis() as i64);
            Some(ReportFile { path, modified_ms })
        })
        .collect()
}

/// Delete all but the [`MAX_CRASH_REPORTS`] newest reports. `reports` must be
/// sorted newest-first.
fn prune_reports(reports: &[ReportFile]) {
    for report in reports.iter().skip(MAX_CRASH_REPORTS) {
        let _ = fs::remove_file(&report.path);
    }
}

/// Read the clean-start marker's recorded epoch-ms. Absent/unreadable → 0
/// (treat every existing report as new), which is the safe-loud default.
fn read_marker_ms(marker: &Path) -> i64 {
    fs::read_to_string(marker)
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0)
}

/// Stamp the clean-start marker with the current epoch-ms. Best-effort.
fn write_marker_now(marker: &Path) {
    if let Some(parent) = marker.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64);
    let _ = fs::write(marker, now_ms.to_string());
}

// ---------------------------------------------------------------------------
// CLI support helpers (`folddb crash-reports …`)
// ---------------------------------------------------------------------------

/// Resolve the crash-reports directory under a given `$FOLDDB_HOME`.
pub fn crash_reports_dir(folddb_home: impl AsRef<Path>) -> PathBuf {
    folddb_home.as_ref().join(CRASH_REPORTS_DIR)
}

/// List crash-report paths newest → oldest. Empty when the directory is
/// absent. Drives `folddb crash-reports list`.
pub fn list_crash_reports(folddb_home: impl AsRef<Path>) -> Vec<PathBuf> {
    let dir = crash_reports_dir(folddb_home);
    let mut reports = list_reports(&dir);
    reports.sort_by_key(|r| std::cmp::Reverse(r.modified_ms));
    reports.into_iter().map(|r| r.path).collect()
}

/// Resolve a single report path by its timestamp stem (the filename without
/// the `.txt` extension). Accepts either the on-disk filename-safe form
/// (`2026-06-19T04-32-00.000Z`) or the human RFC3339 form
/// (`2026-06-19T04:32:00.000Z`); the latter is normalised. Returns the path
/// only when it exists. Drives `folddb crash-reports show <ts>`.
pub fn resolve_crash_report(folddb_home: impl AsRef<Path>, ts: &str) -> Option<PathBuf> {
    let stem = ts.trim().trim_end_matches(".txt").replace(':', "-");
    let path = crash_reports_dir(folddb_home).join(format!("{stem}.txt"));
    path.exists().then_some(path)
}

// ---------------------------------------------------------------------------
// RFC3339 UTC formatting (dependency-free)
// ---------------------------------------------------------------------------

/// Format an epoch-millis timestamp as `YYYY-MM-DDTHH:MM:SS.mmmZ` (RFC3339,
/// UTC). Self-contained civil-from-days conversion so the observability crate
/// does not take on a `chrono`/`time` dependency for a single format call.
/// Panic-free: a negative or absurd input clamps rather than overflows.
fn format_rfc3339_utc(epoch_ms: i64) -> String {
    let epoch_ms = epoch_ms.max(0);
    let secs = epoch_ms / 1000;
    let millis = (epoch_ms % 1000) as u32;

    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let hour = rem / 3600;
    let minute = (rem % 3600) / 60;
    let second = rem % 60;

    let (year, month, day) = civil_from_days(days);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}Z")
}

/// Convert a count of days since the Unix epoch (1970-01-01) to a civil
/// `(year, month, day)`. Howard Hinnant's well-known `civil_from_days`
/// algorithm; integer-only and panic-free.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    let year = if m <= 2 { y + 1 } else { y };
    (year, m, d)
}
