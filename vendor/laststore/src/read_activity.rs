//! Exact read-shape activity inside one synchronous storage call.
//! Embedders carry the returned counts across their async/blocking boundary.

use std::cell::Cell;

/// Read-shape events caused by an observed storage call, not concurrent work.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadActivity {
    /// Product reads rejected before group access.
    pub partition_read_rejections: u64,
    /// Explicit startup/admin physical passes.
    pub all_group_walks: u64,
}

impl ReadActivity {
    fn add(self, other: Self) -> Self {
        Self {
            partition_read_rejections: self
                .partition_read_rejections
                .saturating_add(other.partition_read_rejections),
            all_group_walks: self.all_group_walks.saturating_add(other.all_group_walks),
        }
    }
}

thread_local! {
    static ACTIVITY: Cell<Option<ReadActivity>> = const { Cell::new(None) };
}

/// Observe synchronous storage work. Nested calls contribute once to their
/// parent, and unwind restores the previous scope. No key bytes are retained.
pub fn observe_read_activity<T>(work: impl FnOnce() -> T) -> (T, ReadActivity) {
    struct Scope(Option<ReadActivity>);
    impl Drop for Scope {
        fn drop(&mut self) {
            ACTIVITY.with(|cell| {
                let observed = cell.get().unwrap_or_default();
                cell.set(self.0.map(|parent| parent.add(observed)));
            });
        }
    }
    let scope = Scope(ACTIVITY.with(|cell| cell.replace(Some(ReadActivity::default()))));
    let result = work();
    let observed = ACTIVITY.with(|cell| cell.get().unwrap_or_default());
    drop(scope);
    (result, observed)
}

pub(crate) fn rejection() {
    ACTIVITY.with(|cell| {
        if let Some(mut activity) = cell.get() {
            activity.partition_read_rejections =
                activity.partition_read_rejections.saturating_add(1);
            cell.set(Some(activity));
        }
    });
}

pub(crate) fn all_groups() {
    ACTIVITY.with(|cell| {
        if let Some(mut activity) = cell.get() {
            activity.all_group_walks = activity.all_group_walks.saturating_add(1);
            cell.set(Some(activity));
        }
    });
}
