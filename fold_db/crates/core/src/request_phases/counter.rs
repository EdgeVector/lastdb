//! Counts of work issued by one request.

/// One countable unit of request work — "how much", where [`RequestPhase`] is
/// "how long".
///
/// Deliberately small. A counter earns a place here only when a wall-clock
/// phase already exists for the same work AND that phase is known to move
/// more from node state than from the code being evaluated. Every variant is
/// counted at the site that issues the work, so it is exact for the request
/// rather than a store-wide gauge sampled around it (contrast
/// `OpSample::cold_shard_loads`, which is store-wide and therefore charged
/// for concurrent callers).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestCounter {
    /// Field molecules this request handed to the durable store.
    ///
    /// The denominator `persist_molecules_us` never had. One mutation touches
    /// one molecule PER FIELD, so this is the schema's modified-field count,
    /// not a constant: measured on the primary 2026-08-17, 55 of 195
    /// mutations stored 24 apiece (BoardCards' 24 fields) against a mean of
    /// 11.98 and a median of 7. Without it, `persist_molecules_us / count` is
    /// a per-request figure whose unit of work is unknown, and two windows
    /// with different field mixes are not comparable.
    MoleculesPersisted,
    /// Awaited durable molecule-store operations this request issued.
    ///
    /// Counted at each call boundary in the molecule persist path — one per
    /// shared batch commit, one per single-molecule commit, and one more for
    /// the delete half of a full rewrite. This is the number the batch path
    /// exists to reduce: with every molecule eligible it is 1 regardless of
    /// field count, and with none eligible it is at least
    /// [`Self::MoleculesPersisted`].
    ///
    /// Read as a RATIO against `MoleculesPersisted`: `24/24` is the
    /// per-molecule path, `24/1` is the batch path, and anything between
    /// names how many molecules fell out of the batch's eligibility window
    /// (absent header, or a full order snapshot rather than an append tail).
    /// That ratio is what makes the restructuring verifiable on a live node
    /// without a wall-clock A/B the node's own variance would swamp.
    MoleculeStoreCommits,
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
    /// Operations carried by those commits: created + updated + deleted +
    /// recognised-duplicate rows.
    ///
    /// The denominator for [`Self::ResidentCommits`]. `9/1` is one resident
    /// commit carrying a primary record and eight projections; `9/9` is nine
    /// separate commits doing the same work, which is the shape the design
    /// replaces. Idempotency duplicates are counted here because a batch that
    /// was entirely duplicates still cost the node an idempotency scan, and
    /// omitting them would report that request as having issued no work at all.
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
    pub molecules_persisted: u64,
    pub molecule_store_commits: u64,
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
