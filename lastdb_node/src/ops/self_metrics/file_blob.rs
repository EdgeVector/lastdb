use super::*;

gauge_health! {
    /// File-blob durability gauges retained in the status response.
    /// The retired remote-fetch path supplies no current counters. Defaults
    /// stay unavailable rather than claiming measured zero loss.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct FileBlobHealth {
        /// Proven-absent fetch events for blobs this node recorded as uploaded —
        /// loss *discoveries*, not retries.
        ///
        /// The fetch that proves absence also clears the memo, so a client
        /// hammering one lost blob books it once. Exceeding
        /// [`Self::absent_with_memo_distinct`] therefore means a blob was
        /// re-uploaded and lost again, which is worse than the same count spread
        /// over distinct blobs.
        pub absent_with_memo: (Unit::Events, GAUGE_PROCESS_LIFETIME),
        /// Distinct blobs behind those events — how many objects are actually gone.
        pub absent_with_memo_distinct: (Unit::Named("blob(s)"), GAUGE_PROCESS_LIFETIME),
        /// Proven-absent fetch events with no memo held: an ordinary miss for bytes
        /// this node does not claim to have uploaded.
        ///
        /// Carried so the durability figure has a denominator. Absent it, a reader
        /// cannot tell a node that lost blobs from a node that is merely being
        /// asked for blobs it never had. Note it also absorbs re-fetches of blobs
        /// already reported above, because that first report cleared their memo —
        /// a denominator, not a diagnosis.
        pub absent_without_memo: (Unit::Events, GAUGE_PROCESS_LIFETIME),
    }
    flags {
        /// True when the node stopped retaining new identities at the cap, so
        /// [`Self::absent_with_memo_distinct`] is a floor rather than a total.
        pub absent_distinct_capped,
    }
}
