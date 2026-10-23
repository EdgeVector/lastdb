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
    let result = action();
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
