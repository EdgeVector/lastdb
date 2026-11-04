//! Counts of work issued by one request.

/// One countable unit of request work — "how much", where [`RequestPhase`] is
/// "how long".
///
/// Deliberately small. A counter earns a place here only when a wall-clock
/// phase already exists for the same work AND that phase is known to move
/// more from node state than from the code being evaluated. Every variant is
/// counted at the site that issues the work, so it is exact for the request
/// rather than a store-wide gauge sampled around it, which would charge it
/// for concurrent callers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestCounter {
    /// Logical resident commits this request published — one per caller batch
    /// that reached the apply gates, not one per mutation or per schema.
    ///
    /// The number the one-resident-commit design is defined by. A Brain write
    /// carrying a primary record plus eight exact projections is CORRECT at 1
    /// and is the old serial repair path at 9, and no wall-clock phase can
    /// tell those apart: both spend their time in the same buckets, and the
    /// serial shape is often the *faster* one per commit while being nine
    /// times the work. Read with [`Self::ResidentOperations`].
    ///
    /// A count rather than a phase on purpose — see
    /// [`crate::fold_db_core::mutation_manager::receipt`] for why the resident
    /// stage CLOCKS cannot join [`RequestPhaseTotals`] without corrupting the
    /// `unattributed` remainder, and why the counts can.
    ResidentCommits,
    /// Complete creates, replacements and erasures carried by those commits.
    ResidentOperations,
}

/// Accumulated per-request work counts.
///
/// Zero means "not reported", exactly as for [`RequestPhaseTotals`]: consumers
/// treat an all-zero set as absent, so recording sites need no "was anything
/// counted" side channel. `Copy` for the same reason the phase set is — the
/// telemetry ring clones whole samples repeatedly.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RequestCounts {
    pub resident_commits: u64,
    pub resident_operations: u64,
}

impl RequestCounts {
    /// True when nothing was counted, so surfaces can omit the set entirely
    /// and stay byte-identical to pre-counter output.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}
