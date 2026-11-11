//! What one logical resident commit returns to its caller.

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
