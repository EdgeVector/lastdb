//! FMT layer — JSON formatter with format-time PII redaction.
//!
//! Defense-in-depth alongside the [`crate::redact!`] / [`crate::redact_id!`]
//! macros: even if a call site forgets to wrap a sensitive value, this
//! formatter still scrubs the value at write time when the field name
//! matches the deny-list.
//!
//! Output shape follows the OpenTelemetry log data model — one JSON object
//! per line with `time_unix_nano`, `severity_text`, `severity_number`,
//! `body`, `target`, optional `span`, and an `attributes` object holding
//! the event's structured fields.

use std::collections::HashSet;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Map, Value};
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_appender::non_blocking::{NonBlocking, WorkerGuard};
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::registry::LookupSpan;

use crate::ObsError;

// ---------------------------------------------------------------------------
// File rotation / retention
// ---------------------------------------------------------------------------

/// How many days of rotated log segments to keep. With daily rotation this is
/// also the rough day-count of history the offline `folddb logs --since` reader
/// can reach. Before this bound existed `observability.jsonl` grew unbounded —
/// it had reached ~287 MB / ~10 days during the 2026-06-19 incident, which is
/// what forced a hand-rolled 160 MB scan to find the outage gap.
pub const OBS_LOG_RETENTION_DAYS: usize = 7;

/// Default filename stem the node log file uses (`observability`). The rolling
/// appender writes daily segments named `<stem>.<YYYY-MM-DD>.<ext>` in the same
/// directory, so a path of `…/observability.jsonl` rolls into
/// `…/observability.2026-06-19.jsonl`, `…/observability.2026-06-20.jsonl`, …
/// The offline reader globs `<stem>.*.<ext>` (plus a legacy un-suffixed
/// `<stem>.<ext>`) so `--since` keeps working across a roll.
const DEFAULT_LOG_STEM: &str = "observability";
const DEFAULT_LOG_EXT: &str = "jsonl";

/// Enumerate the on-disk log segments for a canonical log path, **newest
/// first**, so an offline reader can walk back through rotated history.
///
/// For `…/observability.jsonl` this returns every `…/observability.*.jsonl`
/// daily segment the rolling appender wrote, plus a legacy un-rotated
/// `…/observability.jsonl` if one still exists (nodes that ran before this
/// rotation change appended to that single file). Segments are ordered by the
/// date encoded in their filename, falling back to filesystem mtime, so the
/// newest day sorts first — the reader stops early once it has walked past the
/// requested `--since` bound.
///
/// Returns an empty vec when the directory can't be read; callers treat that
/// the same as "no log file yet".
pub fn rotated_log_segments(canonical_path: &Path) -> Vec<PathBuf> {
    let (dir, prefix, suffix) = split_log_path(canonical_path);
    let dot_prefix = format!("{prefix}.");
    let dot_suffix = format!(".{suffix}");
    // The legacy single-file name (`observability.jsonl`) — pre-rotation nodes
    // wrote here. It matches `<prefix>.<suffix>` exactly (no date segment).
    let legacy_name = format!("{prefix}.{suffix}");

    let Ok(read_dir) = std::fs::read_dir(&dir) else {
        return Vec::new();
    };

    let mut segments: Vec<(Option<String>, std::time::SystemTime, PathBuf)> = Vec::new();
    for entry in read_dir.flatten() {
        let Ok(meta) = entry.metadata() else { continue };
        if !meta.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };

        // Rotated segment: `<prefix>.<date>.<suffix>` (and not the legacy name).
        let is_rotated = name.starts_with(&dot_prefix)
            && name.ends_with(&dot_suffix)
            && name != legacy_name
            && name.len() > dot_prefix.len() + dot_suffix.len();
        let is_legacy = name == legacy_name;
        if !is_rotated && !is_legacy {
            continue;
        }

        // Sort key: the embedded date string (`YYYY-MM-DD`) sorts
        // lexicographically == chronologically. The legacy file has no date —
        // treat it as the oldest so live segments win, then fall back to mtime.
        let date_key = if is_rotated {
            name.strip_prefix(&dot_prefix)
                .and_then(|s| s.strip_suffix(&dot_suffix))
                .map(str::to_string)
        } else {
            None
        };
        let mtime = meta.modified().unwrap_or(std::time::UNIX_EPOCH);
        segments.push((date_key, mtime, entry.path()));
    }

    // Newest first: by date string desc, then mtime desc. `None` (legacy) sorts
    // last so the dated live/recent segments are visited before the un-dated
    // historical blob.
    use std::cmp::Ordering;
    segments.sort_by(|a, b| match (&a.0, &b.0) {
        // Both dated → newer date first (desc), tie-break newer mtime first.
        (Some(x), Some(y)) => y.cmp(x).then(b.1.cmp(&a.1)),
        // A dated segment outranks the un-dated legacy blob.
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        // Neither dated → newer mtime first.
        (None, None) => b.1.cmp(&a.1),
    });

    segments.into_iter().map(|(_, _, p)| p).collect()
}

/// The newest on-disk log segment for a canonical log path — the file the
/// appender is *currently* writing to, or `None` if no segment exists yet.
///
/// The rolling appender never writes the canonical `…/observability.jsonl`
/// itself; it writes dated `…/observability.<YYYY-MM-DD>.jsonl` segments. So a
/// user-facing `tail -f …/observability.jsonl` always fails. This returns the
/// real, current filename to advertise instead. It is derived from
/// [`rotated_log_segments`] (newest-first) rather than recomputing the date
/// ourselves, so it can never drift from the appender's naming — including the
/// UTC-vs-local date boundary, since `tracing_appender` stamps segments in UTC.
pub fn current_log_segment(canonical_path: &Path) -> Option<PathBuf> {
    rotated_log_segments(canonical_path).into_iter().next()
}

/// A shell glob that matches every on-disk segment for a canonical log path
/// (`…/observability.*.jsonl`). Safe to hand to `tail -f` or `cat` so a
/// copy-pasted command keeps working across a daily roll. Pairs with
/// [`current_log_segment`] for the "advertise the real path" use case.
pub fn log_segment_glob(canonical_path: &Path) -> PathBuf {
    let (dir, prefix, suffix) = split_log_path(canonical_path);
    dir.join(format!("{prefix}.*.{suffix}"))
}

/// Split a canonical log-file path (e.g. `…/observability.jsonl`) into the
/// `(directory, filename_prefix, filename_suffix)` the rolling appender needs.
///
/// - `…/observability.jsonl` → (`…`, `"observability"`, `"jsonl"`)
/// - `…/folddb.log`          → (`…`, `"folddb"`, `"log"`)
/// - `…/observability`       → (`…`, `"observability"`, `"jsonl"`) (no extension)
/// - a bare filename with no parent → (`.`, …)
///
/// A path whose file stem is empty (`…/.jsonl`) or absent falls back to the
/// default stem so we never hand the appender an empty prefix.
fn split_log_path(path: &Path) -> (PathBuf, String, String) {
    let dir = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf);
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .filter(|s| !s.is_empty())
        .unwrap_or(DEFAULT_LOG_STEM)
        .to_string();
    let ext = path
        .extension()
        .and_then(|s| s.to_str())
        .filter(|s| !s.is_empty())
        .unwrap_or(DEFAULT_LOG_EXT)
        .to_string();
    (dir, stem, ext)
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Where the FMT layer writes formatted JSON events.
#[derive(Debug, Clone)]
pub enum FmtTarget {
    /// Append-write to a regular file. Created if absent.
    File(PathBuf),
    /// Process stdout. Suitable for Lambda / docker-style log capture.
    Stdout,
    /// Process stderr. Suitable for CLIs that reserve stdout for output.
    Stderr,
}

/// Holds the [`tracing_appender`] worker thread alive so its background
/// flush keeps draining the queue. **Must be retained for the lifetime of
/// the binary** — dropping the guard stops the worker mid-flush and any
/// log lines still in the channel are lost.
#[must_use = "FmtGuard must be held for the lifetime of the binary or log lines may be dropped"]
pub struct FmtGuard {
    _worker: WorkerGuard,
}

/// Open the target sink and wrap it in [`tracing_appender::non_blocking`].
///
/// Used by the multi-layer init helpers in [`crate::init`], which build the
/// fmt layer inline so the layer's `Subscriber` type parameter is inferred
/// at the composition site.
///
/// The [`FmtTarget::File`] path is **daily-rotated with bounded retention**
/// (see [`OBS_LOG_RETENTION_DAYS`]): the canonical path `…/observability.jsonl`
/// becomes a directory of `…/observability.<YYYY-MM-DD>.jsonl` segments, and the
/// appender prunes everything older than the retention window on startup and on
/// each roll. This caps the firehose that grew unbounded to ~287 MB before this
/// change. The offline `folddb logs` reader globs the segment set so `--since`
/// still reaches across rotations.
pub(crate) fn build_fmt_writer(target: FmtTarget) -> Result<(NonBlocking, FmtGuard), ObsError> {
    let writer: Box<dyn io::Write + Send + 'static> = match target {
        FmtTarget::File(path) => Box::new(build_rolling_appender(&path)?),
        FmtTarget::Stdout => Box::new(io::stdout()),
        FmtTarget::Stderr => Box::new(io::stderr()),
    };
    let (non_blocking, worker) = tracing_appender::non_blocking(writer);
    Ok((non_blocking, FmtGuard { _worker: worker }))
}

/// Build the daily-rotating, retention-bounded file appender for a canonical
/// log path. Factored out of [`build_fmt_writer`] so it is unit-testable
/// without standing up the whole `non_blocking` worker.
fn build_rolling_appender(path: &Path) -> Result<RollingFileAppender, ObsError> {
    let (dir, prefix, suffix) = split_log_path(path);
    // Ensure the log directory exists; `default_node_log_path` already creates
    // it for the FOLDDB_HOME / ~/.folddb branches, but the OBS_FILE_PATH branch
    // hands us the caller's path verbatim, and the rolling appender errors if
    // its directory is missing.
    std::fs::create_dir_all(&dir)?;
    // Retire any pre-rotation legacy monolith so it ages out under retention.
    retire_legacy_log(&dir, &prefix, &suffix);
    RollingFileAppender::builder()
        .rotation(Rotation::DAILY)
        .filename_prefix(prefix)
        .filename_suffix(suffix)
        .max_log_files(OBS_LOG_RETENTION_DAYS)
        .build(&dir)
        .map_err(|e| ObsError::Io(io::Error::other(e.to_string())))
}

/// Rename a pre-rotation legacy `<prefix>.<suffix>` monolith (e.g.
/// `observability.jsonl`) into a dated segment so the rolling appender's
/// retention window can prune it.
///
/// Nodes that ran before the rolling appender landed appended to a single
/// un-dated `observability.jsonl`. `tracing_appender`'s `max_log_files` only
/// counts the **dated** `<prefix>.<date>.<suffix>` segments it manages, so that
/// legacy blob is invisible to retention and grows / sits **forever** — it was
/// observed at 330 MB on 2026-06-20 even though daily rotation was already
/// shipping (a small `observability.2026-06-21.jsonl` sat right beside it). The
/// shipped app keeps the monolith open and unbounded.
///
/// We don't delete user data; we just give the blob a dated name so it falls
/// under the same 7-day retention as every rolled segment and ages out
/// naturally. The date used is the file's last-modified day (its newest data),
/// so it prunes ~`OBS_LOG_RETENTION_DAYS` after the last line was written. We
/// pick a `<date>.legacy` suffix component collision-free against the
/// appender's own `<date>` segments — concretely we rename to
/// `<prefix>.<YYYY-MM-DD>-legacy.<suffix>`, which still matches the offline
/// reader's `<prefix>.*.<suffix>` glob (so `folddb logs --since` keeps reading
/// it) and still matches `max_log_files`' prune glob (so retention prunes it),
/// while never colliding with a real `<prefix>.<YYYY-MM-DD>.<suffix>` segment.
///
/// Best-effort: any IO failure (missing file, permission, a target that already
/// exists) is swallowed — a failed retirement must never block observability
/// init. The worst case is the status quo (the blob stays), not a boot failure.
fn retire_legacy_log(dir: &Path, prefix: &str, suffix: &str) {
    let legacy = dir.join(format!("{prefix}.{suffix}"));
    let Ok(meta) = std::fs::metadata(&legacy) else {
        return; // no legacy monolith — nothing to retire
    };
    if !meta.is_file() {
        return;
    }
    let date = legacy_retire_date(meta.modified().unwrap_or(SystemTime::UNIX_EPOCH));
    let mut target = dir.join(format!("{prefix}.{date}-legacy.{suffix}"));
    // Avoid clobbering an existing retirement (e.g. a previous boot already
    // retired a blob on the same day): disambiguate with a counter.
    let mut n = 1;
    while target.exists() {
        target = dir.join(format!("{prefix}.{date}-legacy-{n}.{suffix}"));
        n += 1;
        if n > 1000 {
            return; // pathological dir — give up rather than spin
        }
    }
    let _ = std::fs::rename(&legacy, &target);
}

/// Format a `SystemTime` as a `YYYY-MM-DD` UTC date string for the legacy-log
/// retirement name. Done with a tiny civil-from-days computation rather than
/// pulling in `chrono`/`time` (the observability crate stays dependency-lean,
/// and `tracing_appender` already stamps its own dates in UTC).
fn legacy_retire_date(modified: SystemTime) -> String {
    let secs = modified
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let days = (secs / 86_400) as i64;
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}")
}

/// Convert a count of days since the Unix epoch (1970-01-01) to a
/// `(year, month, day)` Gregorian/proleptic civil date. Howard Hinnant's
/// well-known branch-free `civil_from_days` algorithm; valid for the entire
/// range we care about. Self-contained so the crate avoids a date-library dep.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

// ---------------------------------------------------------------------------
// Deny-list — field names whose values are scrubbed at format time.
// ---------------------------------------------------------------------------

/// Compile-time deny-list. The dotted variants (`auth.token`, `api.key`)
/// match the canonical attribute style from [`crate::attrs`]; the
/// underscore variants match common ad-hoc field names.
const STATIC_DENY_LIST: &[&str] = &[
    "auth_token",
    "auth.token",
    "password",
    "api_key",
    "api.key",
    "secret",
    "email",
    "phone",
    "ssn",
];

const REDACTED_PLACEHOLDER: &str = "<redacted>";

const OBS_REDACT_EXTRA_ENV: &str = "OBS_REDACT_EXTRA";

#[derive(Clone, Debug)]
pub(crate) struct DenyList {
    set: HashSet<String>,
}

impl DenyList {
    /// Static list plus comma-separated names from the `OBS_REDACT_EXTRA`
    /// env var. Reads the env var fresh on every call — the layer
    /// constructor calls this once at startup, so the snapshot is taken
    /// when the binary boots.
    pub(crate) fn from_env() -> Self {
        let raw = std::env::var(OBS_REDACT_EXTRA_ENV).unwrap_or_default();
        let extras: Vec<&str> = raw
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect();
        Self::with_extras(&extras)
    }

    pub(crate) fn with_extras(extras: &[&str]) -> Self {
        let mut set: HashSet<String> = STATIC_DENY_LIST.iter().map(|s| (*s).to_string()).collect();
        for extra in extras {
            set.insert((*extra).to_string());
        }
        Self { set }
    }

    pub(crate) fn contains(&self, name: &str) -> bool {
        self.set.contains(name)
    }
}

// ---------------------------------------------------------------------------
// RedactingFormat — custom FormatEvent impl
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct RedactingFormat {
    deny_list: DenyList,
    service_name: Option<String>,
}

impl RedactingFormat {
    pub(crate) fn from_env() -> Self {
        Self {
            deny_list: DenyList::from_env(),
            service_name: None,
        }
    }

    /// Like [`Self::from_env`] but stamps every formatted line with the OTel
    /// resource attribute `service.name = <name>`. Used by [`crate::init_node`]
    /// so a binary's file output is self-identifying.
    pub(crate) fn from_env_with_service(service_name: &str) -> Self {
        Self {
            deny_list: DenyList::from_env(),
            service_name: Some(service_name.to_string()),
        }
    }
}

impl<S, N> FormatEvent<S, N> for RedactingFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let metadata = event.metadata();
        let level = *metadata.level();

        let mut visitor = JsonFieldVisitor::new(&self.deny_list);
        event.record(&mut visitor);
        let JsonFieldVisitor {
            body, attributes, ..
        } = visitor;

        let mut obj = Map::new();
        obj.insert(
            "time_unix_nano".into(),
            Value::String(now_unix_nanos().to_string()),
        );
        obj.insert("severity_text".into(), Value::String(level.to_string()));
        obj.insert(
            "severity_number".into(),
            Value::from(severity_number(level)),
        );
        obj.insert("body".into(), Value::String(body.unwrap_or_default()));
        obj.insert(
            "target".into(),
            Value::String(metadata.target().to_string()),
        );
        if let Some(name) = self.service_name.as_deref() {
            obj.insert("service.name".into(), Value::String(name.to_string()));
        }
        if let Some(span) = ctx.lookup_current() {
            obj.insert("span".into(), Value::String(span.name().to_string()));
        }
        if !attributes.is_empty() {
            obj.insert("attributes".into(), Value::Object(attributes));
        }

        let line = serde_json::to_string(&Value::Object(obj)).map_err(|_| fmt::Error)?;
        writeln!(writer, "{line}")
    }
}

fn severity_number(level: Level) -> u32 {
    // Map tracing levels to the OTel SeverityNumber enum.
    match level {
        Level::TRACE => 1,
        Level::DEBUG => 5,
        Level::INFO => 9,
        Level::WARN => 13,
        Level::ERROR => 17,
    }
}

fn now_unix_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos())
}

// ---------------------------------------------------------------------------
// Visitor — walks event fields, applying the deny-list per field name.
// ---------------------------------------------------------------------------

struct JsonFieldVisitor<'a> {
    deny_list: &'a DenyList,
    body: Option<String>,
    attributes: Map<String, Value>,
}

impl<'a> JsonFieldVisitor<'a> {
    fn new(deny_list: &'a DenyList) -> Self {
        Self {
            deny_list,
            body: None,
            attributes: Map::new(),
        }
    }

    fn record_field(&mut self, name: &str, value: Value) {
        // tracing routes the macro's bare-string message to the special
        // field named `message`; promote it to the OTel `body` slot.
        //
        // NOTE — this return is *before* the deny-list check below, and that is
        // not an oversight that can be fixed here: by the time the layer sees
        // it, everything a macro interpolated into its format string has already
        // been rendered into this one opaque string. There is no field name left
        // to match on, and scrubbing the body wholesale would blank every log
        // line in the process.
        //
        // The consequence is worth stating plainly, because it is the opposite
        // of what "we have a redaction layer" suggests: **the deny-list protects
        // structured fields only.** A secret passed as its own named field is
        // redacted; the same secret interpolated into the format string is not,
        // and neither is anything reachable through a `{:?}` on a struct that
        // happens to hold user data. The CI lint
        // (`scripts/lints/lint-redaction.sh`) has the same shape and the same
        // blind spot — it matches a `name = value` pair, so it cannot see a
        // positional argument either.
        //
        // So the call site is the only place this can be got right: redact into
        // the format argument (`redact!` / `redact_id!`, or a purpose-built
        // log-safe view like `HashRangeFilter::redacted`). Pinned by
        // `message_body_is_not_scrubbed_by_the_deny_list`.
        if name == "message" {
            self.body = Some(match value {
                Value::String(s) => s,
                other => other.to_string(),
            });
            return;
        }
        let final_value = if self.deny_list.contains(name) {
            Value::String(REDACTED_PLACEHOLDER.into())
        } else {
            value
        };
        self.attributes.insert(name.to_string(), final_value);
    }
}

impl<'a> Visit for JsonFieldVisitor<'a> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.record_field(field.name(), Value::String(value.into()));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.record_field(field.name(), Value::Number(value.into()));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.record_field(field.name(), Value::Number(value.into()));
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        let num = serde_json::Number::from_f64(value).map_or(Value::Null, Value::Number);
        self.record_field(field.name(), num);
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.record_field(field.name(), Value::Bool(value));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.record_field(field.name(), Value::String(format!("{value:?}")));
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
