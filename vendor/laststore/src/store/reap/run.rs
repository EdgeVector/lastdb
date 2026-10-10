//! Run a plan: guards, count pass, gate, apply pass.

use super::apply::CollectionApplied;
use super::count::CollectionCount;
use super::guard::{
    lock_store, refuse_unless_data_dir, refuse_unless_stopped_plain, resolve_store_root,
};
use super::plan::ReapPlan;
use super::ReapError;
use crate::options::{LastStoreOptions, PackagingMode};
use crate::store::LastStore;
use std::path::Path;

/// How to run a plan.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReapOptions {
    /// Write. Without it the run counts and changes no byte of the store.
    /// With it, the run creates `maintenance.lock` before the count pass, and
    /// the file stays also when the gate fails.
    pub execute: bool,
    /// Accept a count that is lower than `expect_keys`. A rerun after an
    /// earlier run needs it.
    pub already_applied_ok: bool,
}

/// Something the run did. The caller shows it while the run goes on.
pub enum ReapEvent<'a> {
    /// The count pass finished one collection.
    Counted(&'a CollectionCount),
    /// The apply pass rewrote one group.
    GroupRewritten {
        /// Collection name.
        collection: &'a str,
        /// Shard number.
        shard: u16,
        /// Hash group, or `None` on a segment-log store.
        group: Option<u32>,
        /// Keys dropped from the group.
        dropped_keys: u64,
        /// Segment bytes of the group before the rewrite.
        bytes_before: u64,
        /// Segment bytes of the group after the rewrite.
        bytes_after: u64,
    },
    /// The apply pass finished one collection.
    Applied(&'a CollectionApplied),
}

/// The result of a run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReapOutcome {
    /// Count pass result of each collection.
    pub counts: Vec<CollectionCount>,
    /// Apply pass result of each collection. Empty when nothing was written.
    pub applied: Vec<CollectionApplied>,
    /// True when the run was allowed to write.
    pub executed: bool,
    /// True when the plan matched no key at all.
    pub already_applied: bool,
    /// Why the counts fail the gate. Empty when they pass.
    pub gate_problems: Vec<String>,
}

impl ReapOutcome {
    /// Keys the plan matched, or keys dropped when the run wrote.
    pub fn total_keys(&self) -> u64 {
        self.counts.iter().map(|count| count.matched_keys).sum()
    }

    /// Key and value bytes the plan matched.
    pub fn total_bytes(&self) -> u64 {
        self.counts.iter().map(|count| count.matched_bytes).sum()
    }

    /// Segment bytes of the rewritten groups before the rewrite.
    pub fn bytes_before(&self) -> u64 {
        self.applied.iter().map(|done| done.bytes_before).sum()
    }

    /// Segment bytes of the rewritten groups after the rewrite.
    pub fn bytes_after(&self) -> u64 {
        self.applied.iter().map(|done| done.bytes_after).sum()
    }
}

impl LastStore {
    /// Run a plan on this store.
    ///
    /// The count pass runs on every collection of the plan first. With
    /// `execute`, the gate must pass for all of them before the apply pass
    /// writes the first byte. A failed gate returns
    /// [`ReapError::GateMismatch`] and writes nothing. Without `execute`, a
    /// failed gate is listed in [`ReapOutcome::gate_problems`].
    pub fn reap(
        &self,
        plan: &ReapPlan,
        options: &ReapOptions,
        on_event: &mut dyn FnMut(ReapEvent<'_>),
    ) -> Result<ReapOutcome, ReapError> {
        if options.execute {
            self.require_writable()
                .map_err(|error| ReapError::Refused(error.to_string()))?;
        }
        if self.opts.packaging != PackagingMode::Plain || self.opts.data_key.is_some() {
            return Err(ReapError::Refused(
                "the store is not plain packaging".to_string(),
            ));
        }
        plan.recheck_allow_list()?;
        let mut counts = Vec::with_capacity(plan.collections.len());
        for rules in &plan.collections {
            let mut count = self.reap_count_collection(rules)?;
            count.ok = count.gate_problem(options.already_applied_ok).is_none();
            on_event(ReapEvent::Counted(&count));
            counts.push(count);
        }
        let gate_problems: Vec<String> = counts
            .iter()
            .filter_map(|count| count.gate_problem(options.already_applied_ok))
            .collect();
        let already_applied = counts.iter().all(|count| count.matched_keys == 0);
        let mut outcome = ReapOutcome {
            counts,
            applied: Vec::new(),
            executed: options.execute,
            already_applied,
            gate_problems,
        };
        if !options.execute {
            return Ok(outcome);
        }
        if !outcome.gate_problems.is_empty() {
            return Err(ReapError::GateMismatch(outcome.gate_problems.join("; ")));
        }
        for (rules, count) in plan.collections.iter().zip(&outcome.counts) {
            let done = self.reap_apply_collection(rules, count, on_event)?;
            on_event(ReapEvent::Applied(&done));
            outcome.applied.push(done);
        }
        Ok(outcome)
    }
}

/// Run a plan on a stopped store home.
///
/// This loads the plan, checks that no daemon runs, takes the maintenance
/// lock, opens the store, and runs the plan. `home` is the store root or its
/// parent. A run with `execute` creates `maintenance.lock` before the count
/// pass. A run without `execute` creates no file.
pub fn reap_home(
    home: &Path,
    plan_dir: &Path,
    collections: Option<&[String]>,
    options: &ReapOptions,
    on_event: &mut dyn FnMut(ReapEvent<'_>),
) -> Result<ReapOutcome, ReapError> {
    let plan = ReapPlan::load(plan_dir, collections)?;
    let root = resolve_store_root(home)?;
    refuse_unless_stopped_plain(&root)?;
    refuse_unless_data_dir(&root)?;
    let _lock = lock_store(&root, options.execute)?;
    let store = if options.execute {
        LastStore::open(&root)?
    } else {
        LastStore::open_read_only(&root, LastStoreOptions::default())
            .map_err(|error| ReapError::Refused(error.to_string()))?
    };
    store.reap(&plan, options, on_event)
}
