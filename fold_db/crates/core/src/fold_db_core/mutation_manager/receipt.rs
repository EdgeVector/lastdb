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
//! [`ResidentCommitStages`] carries the four stage times the design names in
//! §11 — preparation, gate wait, publish, acknowledgment. They are NOT added to
//! [`crate::request_phases::RequestPhaseTotals`], and that is deliberate rather
//! than an omission.
//!
//! The phase set is a *partition* of the request's wall clock: `apply` is
//! computed as the residual of every other named bucket, and
//! `PhaseTimings::within_wall_us` sums the set to derive the `unattributed=`
//! remainder every operator surface prints. These four stages overlap phases
//! that already exist — gate wait IS `field_key_gate_us`, publish IS
//! `apply_memory_us`, and preparation spans `restore_field_keys_us`
//! and `schema_load_us`. Adding them as phase buckets would
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
/// Sized by mutation verb rather than by schema, because the caller that sent
/// the batch knows which of its rows were the primary record and which were
/// exact projections, and wants to check that the node applied the shape it
/// planned. `no_op` is the idempotency-duplicate count: work the batch asked
/// for that the node correctly did not repeat. It is reported rather than
/// hidden so a client cannot read a short `created` as a lost write.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResidentCommitOperations {
    /// `MutationType::Create` rows applied.
    pub created: u64,
    /// `MutationType::Update` rows applied.
    pub updated: u64,
    /// `MutationType::Delete` and `MutationType::Purge` rows applied.
    pub deleted: u64,
    /// Rows the idempotency filter recognised as already applied.
    pub no_op: u64,
}

impl ResidentCommitOperations {
    /// Every operation the commit accounted for, applied or recognised.
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
            .saturating_add(self.no_op)
    }

    /// Fold `other` into this set. Hard-erasure peels and the create/update
    /// pipeline each produce a partial set for the same logical batch.
    pub fn merge(&mut self, other: Self) {
        self.created = self.created.saturating_add(other.created);
        self.updated = self.updated.saturating_add(other.updated);
        self.deleted = self.deleted.saturating_add(other.deleted);
        self.no_op = self.no_op.saturating_add(other.no_op);
    }
}

/// Stage times for one resident commit, in microseconds.
///
/// See the module docs for why these are on the receipt rather than in the
/// request phase set. Zero means "not measured on this path" — a batch that
/// was entirely idempotency duplicates never reaches the gates and reports zero
/// gate wait and zero publish, which is the true answer rather than a gap.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResidentCommitStages {
    /// Delta preparation: restore, record build, and prepared-revision capture
    /// for every schema in the batch, all of it outside the apply gates.
    pub prepare_us: u64,
    /// Time blocked acquiring the sorted apply gates, summed across the
    /// revision-mismatch retries.
    pub gate_wait_us: u64,
    /// The memory-only publish performed while the gates are held. This is the
    /// span the p99 bar in the terminal proof measures.
    pub publish_us: u64,
    /// Whole-batch wall clock to the acknowledgment, including finalize.
    pub ack_us: u64,
}

/// One logical resident commit's answer to its caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResidentCommitReceipt {
    /// One accepted input mutation identity per caller input, in caller order.
    /// Idempotent repeats retain their submitted identity. This list does not
    /// claim which identity currently owns a tip or which erase removed bytes.
    pub mutation_ids: Vec<String>,
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
    /// `None` when the batch stamped no local clock: a pure replay of imported
    /// mutations keeps the authoring device's clock and mints none of its own.
    pub revision: Option<u64>,
    /// Per-class operation counts.
    pub operations: ResidentCommitOperations,
    /// Whether the acknowledgment claims disk.
    pub durability: ResidentDurability,
    /// Stage times for this commit.
    pub stages: ResidentCommitStages,
    /// Legacy fanout slots, retained for receipt compatibility. Native writes
    /// leave this empty; their durability belongs to the native commit queue.
    pub touched_group_ids: Vec<crate::durable_flush::TouchedGroupId>,
}

impl ResidentCommitReceipt {
    /// An empty commit: no mutation reached the pipeline.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            mutation_ids: Vec::new(),
            revision: None,
            operations: ResidentCommitOperations::default(),
            durability: ResidentDurability::Queued,
            stages: ResidentCommitStages::default(),
            touched_group_ids: Vec::new(),
        }
    }
}
