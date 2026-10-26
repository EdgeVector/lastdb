//! Opt-in, numeric diagnostics for a synchronous native commit on an isolated node.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::OnceLock;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

thread_local! {
    static ACTIVE: RefCell<Option<Report>> = const { RefCell::new(None) };
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
    }
}

/// Capture only this thread's work; concurrent compaction cannot enter its totals.
pub fn measure<T>(action: impl FnOnce() -> T) -> (T, Option<Report>) {
    capture(action, crate::disk_pack::page_usage::initial_counts)
}

/// A joined private-file worker contributes work, not another snapshot header.
#[cfg(target_os = "linux")]
pub(crate) fn measure_child<T>(inherit: bool, action: impl FnOnce() -> T) -> (T, Option<Report>) {
    if !inherit {
        return (action(), None);
    }
    capture(action, BTreeMap::new)
}

#[cfg(target_os = "linux")]
pub(crate) fn capturing() -> bool {
    enabled() && ACTIVE.with(|active| active.borrow().is_some())
}

/// Merge a completed worker only after its join, without replacing wall time.
#[cfg(target_os = "linux")]
pub(crate) fn merge_child(child: Option<Report>) {
    if let Some(child) = child {
        ACTIVE.with(|active| {
            if let Some(parent) = active.borrow_mut().as_mut() {
                for (name, value) in child.phases_us {
                    *parent.phases_us.entry(name).or_default() += value;
                }
                for (name, value) in child.counts {
                    *parent.counts.entry(name).or_default() += value;
                }
            }
        });
    }
}

fn capture<T>(
    action: impl FnOnce() -> T,
    counts: impl FnOnce() -> BTreeMap<&'static str, u64>,
) -> (T, Option<Report>) {
    if !enabled() || ACTIVE.with(|active| active.borrow().is_some()) {
        return (action(), None);
    }
    let started = Instant::now();
    ACTIVE.with(|active| {
        *active.borrow_mut() = Some(Report {
            started_us: unix_us(),
            counts: counts(),
            ..Report::default()
        });
    });
    let capture = Capture;
    let (result, reads) = crate::read_receipt::observe_disk_reads(action);
    note_reads(reads);
    let report = ACTIVE.with(|active| {
        active.borrow_mut().take().map(|mut report| {
            report.total_us = started.elapsed().as_micros().try_into().unwrap_or(u64::MAX);
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
    thread: PhantomData<Rc<()>>,
}

/// Measure a named synchronous phase only inside an active capture.
pub fn stage(name: &'static str) -> Stage {
    Stage {
        name,
        started: (enabled() && ACTIVE.with(|active| active.borrow().is_some())).then(Instant::now),
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
            ACTIVE.with(|active| {
                if let Some(report) = active.borrow_mut().as_mut() {
                    *report.phases_us.entry(self.name).or_default() += micros;
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
