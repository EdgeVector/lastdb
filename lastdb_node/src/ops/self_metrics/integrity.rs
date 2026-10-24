use super::*;

gauge_health! {
    /// Read-path integrity counters — the cost of degrading gracefully.
    ///
    /// A tip pointing at an unresolvable atom used to fail the WHOLE hash-key
    /// partition with a 400 (79,519 healthy rows were unreachable behind 23 bad
    /// ones on the primary). The fix was to skip the broken row and serve the
    /// rest, which is right — but it turns a loud failure into a query that
    /// returns `200` with silently fewer rows than the data claims.
    ///
    /// So the graceful path has to increment something an operator can read.
    /// Otherwise "resilient" and "silently wrong" are the same observation, and
    /// the only remaining evidence is a `WARN` line in a log that also carries
    /// every mutation at `INFO`.
    /// Wire-transparent integrity gauges (bare u64 / null on the wire).
    ///
    /// Edges are typed as [`Unit::Edges`] so a renderer cannot print them as
    /// row(s) (historical mislabel #1). Missing fields deserialize to
    /// [`Availability::Unavailable`], never measured zero (#3).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct IntegrityHealth {
        /// Node-lifetime count of query rows dropped because the tip's atom did
        /// not resolve. Non-zero means reads are returning short — see the
        /// daemon log's `Skipping unresolved atom ref` warnings for the keys.
        ///
        /// This is **exposure**, not damage size: it counts drop *events*, so one
        /// broken row on a partition a hot client polls will run into the
        /// thousands on its own. Read it with `unresolved_atom_distinct`.
        pub unresolved_atom_skips: (Unit::Events, GAUGE_PROCESS_LIFETIME),
        /// Distinct `(atom_uuid, key)` edges behind those events — one per broken
        /// **field** tip, not one per row.
        ///
        /// A record has many fields and each carries its own tip -> atom edge, so
        /// one unreadable row with five dangling field tips is five edges. Read
        /// `unresolved_atom_rows` for how many rows are affected.
        pub unresolved_atom_distinct: (Unit::Edges, GAUGE_PROCESS_LIFETIME),
        /// Distinct keys among those edges — how many row reads come back short.
        ///
        /// Unavailable means the daemon does not report this field. Rendering that
        /// as `0` would make an older daemon look like it measured "zero affected
        /// rows" while it only measured dangling field-tip edges.
        pub unresolved_atom_rows: (Unit::Rows, GAUGE_PROCESS_LIFETIME),
    }
    flags {
        /// True when the node stopped retaining new identities at the cap, so both
        /// distinct figures are floors rather than totals.
        pub unresolved_distinct_capped,
    }
}

impl IntegrityHealth {
    pub fn measured(skips: u64, edges: u64, rows: Option<u64>, capped: bool) -> Self {
        Self {
            unresolved_atom_skips: gauge_measured(skips, Unit::Events, GAUGE_PROCESS_LIFETIME),
            unresolved_atom_distinct: gauge_measured(edges, Unit::Edges, GAUGE_PROCESS_LIFETIME),
            unresolved_atom_rows: gauge_from_wire(rows, Unit::Rows, GAUGE_PROCESS_LIFETIME),
            unresolved_distinct_capped: capped,
        }
    }
}
