//! What the planner reports: totals, and the atoms whose counter disagreed.
//!
//! The planner never hides a counter fault. It keeps the body, marks the
//! counter `suspect`, and records the event here. `lastdb status` shows these
//! totals, and an operator repairs the counter offline.

use serde::Serialize;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// How many recent fault events the report keeps.
const RECENT_EVENTS: usize = 32;

/// One planner event that the report counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CounterEvent {
    /// An atom gained its first reference in a scope. Body and counter created.
    Created,
    /// The count reached zero. Body and counter deleted.
    Deleted,
    /// A removal asked for more references than the counter held.
    Underflow,
    /// A body existed and its counter did not.
    CounterAbsent,
    /// A removal named an atom with neither counter nor body.
    Dangling,
}

impl CounterEvent {
    fn is_fault(self) -> bool {
        !matches!(self, Self::Created | Self::Deleted)
    }

    fn name(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Deleted => "deleted",
            Self::Underflow => "underflow",
            Self::CounterAbsent => "counter_absent",
            Self::Dangling => "dangling_removal",
        }
    }
}

/// Totals since the node started, and the recent faults.
#[derive(Debug, Default)]
pub(crate) struct CounterEvents {
    created: AtomicU64,
    deleted: AtomicU64,
    underflows: AtomicU64,
    counters_absent: AtomicU64,
    dangling_removals: AtomicU64,
    recent_faults: Mutex<VecDeque<String>>,
}

impl CounterEvents {
    pub(super) fn note(&self, atom: &str, event: &CounterEvent) {
        let total = match event {
            CounterEvent::Created => &self.created,
            CounterEvent::Deleted => &self.deleted,
            CounterEvent::Underflow => &self.underflows,
            CounterEvent::CounterAbsent => &self.counters_absent,
            CounterEvent::Dangling => &self.dangling_removals,
        };
        total.fetch_add(1, Ordering::Relaxed);
        if !event.is_fault() {
            return;
        }
        tracing::error!(atom = %atom, event = event.name(), "atom counter fault; the body stays");
        let mut recent = self
            .recent_faults
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if recent.len() == RECENT_EVENTS {
            recent.pop_front();
        }
        recent.push_back(format!("{}:{atom}", event.name()));
    }

    /// A copy of the totals for status output.
    pub(crate) fn snapshot(&self) -> AtomCounterReport {
        AtomCounterReport {
            bodies_created: self.created.load(Ordering::Relaxed),
            bodies_deleted: self.deleted.load(Ordering::Relaxed),
            underflows: self.underflows.load(Ordering::Relaxed),
            counters_absent: self.counters_absent.load(Ordering::Relaxed),
            dangling_removals: self.dangling_removals.load(Ordering::Relaxed),
            suspect_marked: self.underflows.load(Ordering::Relaxed)
                + self.counters_absent.load(Ordering::Relaxed),
            recent_faults: self
                .recent_faults
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .cloned()
                .collect(),
            verify_refs: None,
        }
    }
}

/// The planner report in `lastdb status`.
#[derive(Debug, Clone, Default, Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct AtomCounterReport {
    pub bodies_created: u64,
    pub bodies_deleted: u64,
    pub underflows: u64,
    pub counters_absent: u64,
    pub dangling_removals: u64,
    /// Counters this process marked `suspect`: `underflows` plus `counters_absent`.
    /// It counts events since the daemon started, not the atoms that are
    /// `suspect` now; the last verify run (`verify_refs.last.suspect_atoms`) and
    /// `lastdb db inventory` count those.
    #[serde(default)]
    pub suspect_marked: u64,
    pub recent_faults: Vec<String>,
    /// The verifier: the last complete run and a run in progress. Memory only;
    /// it is empty after a restart. `None` when the daemon predates the field.
    #[serde(default)]
    pub verify_refs: Option<super::VerifySummary>,
}
