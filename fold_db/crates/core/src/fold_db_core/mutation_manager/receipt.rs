//! What one logical resident commit returns to its caller.
//!
//! A batch write used to answer with `Vec<String>` — the mutation ids it
//! applied. That answers "which items", and nothing else. It cannot say which
//! revision the caller may now read back, how many operations of each class the
//! commit contained, or whether the acknowledgment claims disk.
//!
//! [`ResidentCommitReceipt`] is that answer. The Brain write plan
//! (`design-brain-one-resident-commit-write` §9) sends one batch carrying the
//! primary record and every exact projection, and needs the reply to name **one
//! committed revision** for all of them plus the per-class operation counts. A
//! caller that gets ids back has to re-read to learn anything else, and a
//! re-read after a write is precisely the round trip the one-resident-commit
//! design exists to remove.
//!
//! # Why the stage clocks live here and not in `RequestPhaseTotals`
//!
//! [`ResidentCommitStages`] carries the two stage times a native write
//! measures — preparation and acknowledgment. They are NOT added to
//! [`crate::request_phases::RequestPhaseTotals`], and that is deliberate rather
//! than an omission.
//!
//! The phase set is a *partition* of the request's wall clock: `apply` is
//! computed as the residual of the batch's named write buckets, and
//! `PhaseTimings::within_wall_us` sums the set to derive the `unattributed=`
//! remainder every operator surface prints. These stages overlap phases that
//! already exist — preparation lies inside the `apply_us` residual, and the
//! acknowledgment spans the whole request. Adding them as phase buckets would
//! double-count those microseconds, drive the residual to zero and then
//! underflow it, and corrupt `unattributed` on every request the node serves —
//! not just resident commits.
//!
//! So the stages ride the receipt, where they describe one commit to the caller
//! that made it, and the request-scoped instrument is a pair of *counts*
//! ([`crate::request_phases::RequestCounter::ResidentCommits`] and
//! [`crate::request_phases::RequestCounter::ResidentOperations`]). Counts carry
//! no wall-clock invariant, so they can name "one logical resident commit and
//! its operation count" without disturbing anything the phase set means.

/// Whether the acknowledgment claims disk.
///
/// The default write path acknowledges after the resident commit and lets the
/// per-schema FIFO lane write the envelope afterwards, so the honest answer is
/// [`Self::Queued`]. A caller that waited for the local persist token gets
/// [`Self::Durable`]. Design law §4.8: the response states the durability state
/// separately rather than letting `success` imply it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResidentDurability {
    /// The envelope is on the persist lane; disk has not been waited for.
    Queued,
    /// The local persist token completed before this receipt was returned.
    Durable,
}

impl ResidentDurability {
    /// Wire spelling, stable for the `/api/mutation` and `/api/mutations/batch`
    /// payloads.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Durable => "durable",
        }
    }
}

/// Per-class operation counts for one resident commit.
///
/// Counts cover complete records accepted in the native transaction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResidentCommitOperations {
    /// `MutationType::Create` rows applied.
    pub created: u64,
    /// `MutationType::Replace` rows applied.
    pub updated: u64,
    /// `MutationType::Delete` and `MutationType::Purge` rows applied.
    pub deleted: u64,
}

impl ResidentCommitOperations {
    /// Every complete operation accepted by this commit.
    ///
    /// This is the number reported as
    /// [`crate::request_phases::RequestCounter::ResidentOperations`] — the
    /// denominator that makes "one logical resident commit" readable. One
    /// commit carrying 9 operations and nine commits carrying one apiece are
    /// the two shapes the Brain write plan is trying to tell apart, and the
    /// commit count alone cannot.
    #[must_use]
    pub fn total(&self) -> u64 {
        self.created
            .saturating_add(self.updated)
            .saturating_add(self.deleted)
    }

    /// Fold `other` into this set. Hard-erasure peels and the create/update
    /// pipeline each produce a partial set for the same logical batch.
    pub fn merge(&mut self, other: Self) {
        self.created = self.created.saturating_add(other.created);
        self.updated = self.updated.saturating_add(other.updated);
        self.deleted = self.deleted.saturating_add(other.deleted);
    }
}

/// Stage times for one resident commit, in microseconds.
///
/// See the module docs for why these are on the receipt rather than in the
/// request phase set. Zero preparation means "not measured on this path" — a
/// an empty batch prepares nothing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResidentCommitStages {
    /// Record preparation for every schema in the batch.
    pub prepare_us: u64,
    /// Whole-batch wall clock to the acknowledgment, including finalize.
    pub ack_us: u64,
}

/// One logical resident commit's answer to its caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResidentCommitReceipt {
    /// One server-generated identity per accepted input, in caller order.
    pub mutation_ids: Vec<String>,
    /// Complete-record revisions, indexed by the server-generated mutation ID.
    pub record_revisions: std::collections::BTreeMap<String, String>,
    /// The one committed revision for the primary record and every exact
    /// projection in this batch.
    ///
    /// This is the batch's author-clock logical counter: `prepare_mutation_
    /// author_clocks` allocates exactly one value per caller batch and stamps
    /// every local mutation with it, so it is a genuine per-commit monotone
    /// number rather than a max over independent per-slot counters (which is
    /// what a slot revision would give — a value that is not comparable across
    /// the primary and its projections and therefore cannot be "one committed
    /// revision" at all).
    ///
    /// `None` only for an empty batch, which allocates no author-clock counter.
    pub revision: Option<u64>,
    /// Per-class operation counts.
    pub operations: ResidentCommitOperations,
    /// Whether the acknowledgment claims disk.
    pub durability: ResidentDurability,
    /// Stage times for this commit.
    pub stages: ResidentCommitStages,
}

impl ResidentCommitReceipt {
    /// An empty commit: no mutation reached the pipeline.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            mutation_ids: Vec::new(),
            record_revisions: std::collections::BTreeMap::new(),
            revision: None,
            operations: ResidentCommitOperations::default(),
            durability: ResidentDurability::Queued,
            stages: ResidentCommitStages::default(),
        }
    }
}
