//! Opt-in, numeric diagnostics for a synchronous native commit on an isolated node.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::io::Write;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::OnceLock;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

thread_local! {
    static ACTIVE: RefCell<Option<Report>> = const { RefCell::new(None) };
    static DEPTH: Cell<usize> = const { Cell::new(0) };
}

/// Numeric phase totals. Child flush totals overlap their enclosing disk phases.
#[derive(Default)]
pub struct Report {
    /// Capture start on the local UTC clock, in microseconds.
    pub started_us: u64,
    /// Capture end on the local UTC clock, in microseconds.
    pub ended_us: u64,
    /// Complete synchronous capture time from the monotonic clock.
    pub total_us: u64,
    /// Union of outermost phase intervals, never a sum of nested phases.
    pub covered_us: u64,
    /// Capture time outside every named phase, including diagnostic overhead.
    pub unassigned_us: u64,
    /// Phase wall times; nested child phases must not be added to their parents.
    pub phases_us: BTreeMap<&'static str, u64>,
    /// Numeric work counts for this capture only.
    pub counts: BTreeMap<&'static str, u64>,
}

/// Diagnostics remain off unless explicitly selected for an isolated node.
pub fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("LASTDB_SNAPSHOT_TIMING").as_deref() == Ok("1")
            && std::env::var("LASTDB_ISOLATED_COPY").as_deref() == Ok("1")
    })
}

/// Emit one complete numeric event under a fixed snapshot or request marker.
/// Callers construct the event only inside their enabled capture or clock guard.
/// Serialization precedes one locked stderr write; output errors remain best effort.
pub fn emit(prefix: &'static str, event: &serde_json::Value) {
    if !enabled()
        || !matches!(
            prefix,
            "LASTDB_SNAPSHOT_TIMING" | "LASTDB_WRITE_REQUEST_TIMING"
        )
    {
        return;
    }
    let mut line = Vec::new();
    line.extend_from_slice(prefix.as_bytes());
    line.push(b' ');
    if serde_json::to_writer(&mut line, event).is_err() {
        return;
    }
    line.push(b'\n');
    let _ = std::io::stderr().lock().write_all(&line);
}

/// Local UTC clock for correlation with a client request interval.
pub fn unix_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            duration.as_micros().try_into().unwrap_or(u64::MAX)
        })
}

struct Capture;

impl Drop for Capture {
    fn drop(&mut self) {
        ACTIVE.with(|active| *active.borrow_mut() = None);
        DEPTH.with(|depth| depth.set(0));
    }
}

/// Capture only this thread's work; concurrent compaction cannot enter its totals.
pub fn measure<T>(action: impl FnOnce() -> T) -> (T, Option<Report>) {
    if !enabled() || ACTIVE.with(|active| active.borrow().is_some()) {
        return (action(), None);
    }
    let started = Instant::now();
    ACTIVE.with(|active| {
        *active.borrow_mut() = Some(Report {
            started_us: unix_us(),
            ..Report::default()
        });
    });
    let capture = Capture;
    let (result, reads) = crate::read_receipt::observe_disk_reads(action);
    note_reads(reads);
    let report = ACTIVE.with(|active| {
        active.borrow_mut().take().map(|mut report| {
            report.total_us = started.elapsed().as_micros().try_into().unwrap_or(u64::MAX);
            report.unassigned_us = report.total_us.saturating_sub(report.covered_us);
            report.ended_us = unix_us();
            report
        })
    });
    drop(capture);
    (result, report)
}

fn note_reads(reads: crate::read_receipt::DiskReadReceipt) {
    for (name, value) in [
        ("read_file_opens", reads.file_opens),
        ("read_operations", reads.read_operations),
        ("read_metadata_operations", reads.metadata_operations),
        ("read_payload_bytes", reads.payload_bytes),
        ("read_format_bytes", reads.format_probe_bytes),
        ("read_open_us", reads.open_nanos / 1000),
        ("read_metadata_us", reads.metadata_nanos / 1000),
        ("read_io_us", reads.read_nanos / 1000),
    ] {
        count(name, value);
    }
}

/// Synchronous phase guard. It cannot move to a different thread.
pub struct Stage {
    name: &'static str,
    started: Option<Instant>,
    outermost: bool,
    thread: PhantomData<Rc<()>>,
}

/// Measure a named synchronous phase only inside an active capture.
pub fn stage(name: &'static str) -> Stage {
    let active = enabled() && ACTIVE.with(|active| active.borrow().is_some());
    let outermost = active
        && DEPTH.with(|depth| {
            let current = depth.get();
            depth.set(current + 1);
            current == 0
        });
    Stage {
        name,
        started: active.then(Instant::now),
        outermost,
        thread: PhantomData,
    }
}

/// Measure one synchronous action without extending the guard beyond it.
pub fn during<T>(name: &'static str, action: impl FnOnce() -> T) -> T {
    let _stage = stage(name);
    action()
}

impl Drop for Stage {
    fn drop(&mut self) {
        if let Some(started) = self.started {
            let micros = started.elapsed().as_micros().try_into().unwrap_or(u64::MAX);
            DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
            ACTIVE.with(|active| {
                if let Some(report) = active.borrow_mut().as_mut() {
                    *report.phases_us.entry(self.name).or_default() += micros;
                    if self.outermost {
                        report.covered_us = report.covered_us.saturating_add(micros);
                    }
                }
            });
        }
    }
}

/// Add numeric work to the active capture, if present.
pub fn count(name: &'static str, value: u64) {
    if enabled() {
        ACTIVE.with(|active| {
            if let Some(report) = active.borrow_mut().as_mut() {
                *report.counts.entry(name).or_default() += value;
            }
        });
    }
}
