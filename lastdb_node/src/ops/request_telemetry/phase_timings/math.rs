use super::*;

impl PhaseTimings {
    /// `true` when no phase was reported — the state every surface treats
    /// as absent rather than as a measurement of zero.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.as_array().iter().all(|v| *v == 0)
    }

    /// Flat fold over all [`PHASE_COUNT`] fields, INCLUDING pre-service waits
    /// AND `hydrate_us`, which its four `hydrate_*` children partition. On a
    /// post-0.23.4 query row that parent is therefore counted twice, so this
    /// sum is ~2x the real phase cost.
    ///
    /// It is the WRONG number to rank keys by — use [`Self::disjoint_us`] —
    /// and the WRONG number to subtract from `duration_ms` — use
    /// [`Self::within_wall_us`]. It is kept because the wire vocabulary is
    /// a flat 50-field array and a caller that genuinely wants the raw fold
    /// (a serialization round-trip check, for one) should not have to
    /// reimplement it.
    #[must_use]
    pub fn total_us(&self) -> u64 {
        self.as_array()
            .iter()
            .fold(0u64, |acc, v| acc.saturating_add(*v))
    }

    /// Phase time counted ONCE — [`Self::total_us`] less every parent phase
    /// whose children reported. This is the caller-observed cost and the
    /// right number to rank keys by.
    ///
    /// `hydrate_us` is the node's one declared PARENT: it is charged over the
    /// whole materialization span while `hydrate_atoms` / `hydrate_format` /
    /// `hydrate_sort` / `hydrate_filter` partition that same span exactly
    /// (an invariant of the phase recorder). Every other phase surface
    /// in this node treats its buckets as disjoint — `apply` and `persist`
    /// are residuals for precisely this reason — so summing the parent beside
    /// its own parts inflated every query row while every mutation row was
    /// ranked honestly.
    ///
    /// Measured on the primary 2026-08-18: all ten rendered `Request phases`
    /// rows were mis-ordered, query rows inflated 1.98-2.00x, and a
    /// `lastgit query` key printed at #3 belonged at #7 — below four separate
    /// mutation keys that `Top by total time`, which ranks by real
    /// `duration_ms`, placed above it. One command's output contradicted
    /// itself and whichever table was read first was the one acted on.
    ///
    /// The subtraction is GUARDED on the children, which handles both eras
    /// with no schema change: a rollup row written before the 0.23.4 carve
    /// carries `hydrate` with zero children and stays whole; a row written
    /// after it carries the children and drops the parent. The parent itself
    /// is never rewritten — `handlers.rs` keeps it so rollup series written
    /// before the carve stay comparable, and that argument is sound.
    #[must_use]
    pub fn disjoint_us(&self) -> u64 {
        self.total_us().saturating_sub(self.double_counted_us())
    }

    /// Phase time that [`Self::total_us`] counts twice: the sum of every
    /// parent phase whose children reported. Zero on mutation rows, on
    /// pre-carve rollup rows, and on any query that reported `hydrate`
    /// without a sub-step.
    #[must_use]
    pub(super) fn double_counted_us(&self) -> u64 {
        let hydrate_children = self
            .hydrate_atoms_us
            .saturating_add(self.hydrate_format_us)
            .saturating_add(self.hydrate_sort_us)
            .saturating_add(self.hydrate_filter_us);
        if hydrate_children == 0 {
            0
        } else {
            self.hydrate_us
        }
    }

    /// Phase time that falls INSIDE the request's `duration_ms` interval and
    /// is counted ONCE — [`Self::disjoint_us`] less the phases measured before
    /// the wall clock starts. Today that pre-service set is `queue_wait_us`
    /// alone.
    ///
    /// This is the only sound left-hand side for a residual. `duration_ms`
    /// starts after the UDS worker dequeues the job, so subtracting a sum
    /// that contains `queue_wait_us` yields a residual too negative by
    /// exactly the queue wait — and `residual_detail` renders any negative
    /// residual as `over=`, whose documented meaning is over-attribution by
    /// a BATCHED caller. A perfectly instrumented, perfectly unbatched query
    /// on a contended node was therefore libelled as over-attributed, with
    /// the size of the libel set by how contended the node was.
    ///
    /// Measured on the primary 2026-08-17, `client=kanban kind=query` on
    /// the live board schema: `hydrate` + `annotate` summed to 591.89 s
    /// against a 591.79 s wall clock — closed to 0.02% — and the row still
    /// printed `over=359228ms (60.7%)`, which was `queue_wait` and nothing
    /// else. Four of the ten rendered rows read `over=` for that reason.
    ///
    /// Built on `disjoint_us`, not `total_us`, for the same reason one step
    /// down: `hydrate_us` lies inside the wall clock but so do the four
    /// children that partition it, and counting the span twice made the
    /// residual too negative by a whole `hydrate`. With the queue-wait fix
    /// shipped and this one not, the primary printed `over=` at 96-100% on
    /// every one of the ten rendered rows (2026-09-07) — the same libel as
    /// before, now sourced from the double-count instead of the queue wait.
    #[must_use]
    pub fn within_wall_us(&self) -> u64 {
        self.disjoint_us().saturating_sub(self.queue_wait_us)
    }

    /// Stable `(name, value)` pairs in [`Self::PHASE_NAMES`] order.
    #[must_use]
    pub fn named_us(&self) -> [(&'static str, u64); PHASE_COUNT] {
        let values = self.as_array();
        std::array::from_fn(|i| (Self::PHASE_NAMES[i], values[i]))
    }

    /// Fold one request's phases into an aggregate: field-wise saturating
    /// SUM. Aggregates must stay delta-able, so no max/last semantics here.
    pub fn accumulate(&mut self, sample: Self) {
        let mut values = self.as_array();
        for (acc, v) in values.iter_mut().zip(sample.as_array()) {
            *acc = acc.saturating_add(v);
        }
        *self = Self::from_array(values);
    }

    /// Field-wise saturating subtraction — the interval delta the rollup
    /// writer persists between two consecutive cumulative snapshots.
    #[must_use]
    pub fn saturating_sub(&self, prev: Self) -> Self {
        let mut values = self.as_array();
        for (v, p) in values.iter_mut().zip(prev.as_array()) {
            *v = v.saturating_sub(p);
        }
        Self::from_array(values)
    }

    /// Rendered breakdown like ` phases[apply=3400us persist=90us]`. Zero
    /// phases are omitted and an empty set renders as the empty string, so
    /// lines for requests that reported nothing stay byte-identical to what
    /// operators already read.
    #[must_use]
    pub fn detail(&self) -> String {
        let parts: Vec<String> = self
            .named_us()
            .iter()
            .filter(|(_, v)| *v > 0)
            .map(|(name, v)| format!("{name}={v}us"))
            .collect();
        if parts.is_empty() {
            return String::new();
        }
        format!(" phases[{}]", parts.join(" "))
    }
}
