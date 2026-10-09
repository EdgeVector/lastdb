//! Disk-usage, warm-set and read-cost reports returned by storage backends.

/// Cheap, metadata-only disk usage for a physical collection.
///
/// Two independent measurements, both without reading a segment:
///
/// - `allocated_bytes` vs `apparent_bytes` is filesystem block allocation
///   past record length — slack from open-segment reservation and block
///   rounding. A delete does not change it.
/// - `live_bytes` / `dead_bytes` is the store's own record-byte residue: how
///   many of the bytes inside the segments still belong to a live id versus
///   how many are superseded puts, deleted puts, and delete markers. This is
///   what a delete leaves behind and what only a compaction returns. `None`
///   on a backend that cannot measure it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CollectionDiskUsage {
    pub allocated_bytes: u64,
    pub apparent_bytes: u64,
    /// Record bytes live ids still address, over the groups that could answer.
    pub live_bytes: Option<u64>,
    /// Dead record bytes, over the groups that could answer.
    pub dead_bytes: Option<u64>,
    /// On-disk bytes the residue probe could not classify (cold groups with
    /// no sidecar residue, and bytes appended after a sidecar was taken).
    /// Never counted as dead.
    pub residue_unknown_bytes: u64,
}

impl CollectionDiskUsage {
    /// Filesystem slack: bytes allocated past record length.
    #[must_use]
    pub fn slack_bytes(&self) -> u64 {
        self.allocated_bytes.saturating_sub(self.apparent_bytes)
    }

    /// Dead record bytes as basis points of measured record bytes.
    #[must_use]
    pub fn dead_bps(&self) -> u64 {
        let (Some(live), Some(dead)) = (self.live_bytes, self.dead_bytes) else {
            return 0;
        };
        let measured = live.saturating_add(dead);
        if measured == 0 {
            return 0;
        }
        ((u128::from(dead) * 10_000) / u128::from(measured)) as u64
    }

    /// On-disk bytes a rewrite of this collection is expected to return.
    ///
    /// The filesystem slack comes back whole. The dead-record share is
    /// applied to the apparent bytes the residue probe could classify
    /// (apparent minus the unknown remainder), because record bytes and disk
    /// bytes differ by whatever the at-rest layer does to a record — the
    /// ratio survives that, an absolute record count does not.
    #[must_use]
    pub fn reclaimable_estimate_bytes(&self) -> u64 {
        let measured_disk = self
            .apparent_bytes
            .saturating_sub(self.residue_unknown_bytes);
        let dead_share =
            ((u128::from(measured_disk) * u128::from(self.dead_bps())) / 10_000) as u64;
        self.slack_bytes().saturating_add(dead_share)
    }

    /// Expected reclaim as basis points of allocation.
    #[must_use]
    pub fn reclaimable_bps(&self) -> u64 {
        if self.allocated_bytes == 0 {
            return 0;
        }
        ((u128::from(self.reclaimable_estimate_bytes()) * 10_000)
            / u128::from(self.allocated_bytes)) as u64
    }
}

/// In-flight cold-load charge and LRU eviction counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WarmSetAdmissionStats {
    /// Cold loads currently parsing a group that is not yet published.
    pub in_flight_cold_load_count: u64,
    /// Estimated on-disk bytes of those in-flight loads.
    pub in_flight_cold_load_bytes: u64,
    /// Groups removed from the warm set since open.
    pub eviction_events: u64,
    /// Effective warm-set byte budget currently enforced by LRU eviction.
    pub effective_warm_bytes: u64,
}

/// Result of one LRU eviction pass against an explicit resident-byte target.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WarmSetEvictionReport {
    /// Groups actually dropped from the warm set.
    pub groups_evicted: u64,
    /// Resident bytes before the pass.
    pub bytes_before: u64,
    /// Resident bytes after the pass.
    pub bytes_after: u64,
}

/// What a read actually costs on this node, as opposed to how long it took.
///
/// Cold shard loads (segment parse + `BTreeMap` rebuild) are the unit of cost
/// for the hash-group read path: a query that is slow because it loaded 625
/// groups is a different problem from one that is slow under lock contention,
/// and wall time alone cannot tell them apart. Warm residency vs budget says
/// whether the warm set is sized to hold the working set, which was previously
/// guessed from process RSS.
///
/// The logical resident path does not add a byte term here. `warm_resident_bytes`
/// and `key_cache_bytes` stay the LastStore flag-off counters. There is no
/// `resident_bytes` field for the logical set.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReadCostStats {
    /// Cold hash-group shard loads since the store was opened (monotonic).
    pub cold_shard_loads: u64,
    /// Hash-group handles currently retained in memory.
    pub warm_resident_groups: u64,
    /// Approximate bytes held by those handles.
    pub warm_resident_bytes: u64,
    /// Configured warm-set byte budget. `0` means eviction is disabled.
    pub warm_budget_bytes: u64,
    /// Configured append-descriptor cap. `0` means the handle cap is off.
    ///
    /// Read this against `open_append_handles`, **not** against
    /// `warm_resident_groups`. Reported because the byte budget cannot stand in
    /// for it: on 2026-07-30 the primary held 8,167 group descriptors against an
    /// 8,192-fd limit with resident bytes at 2.75 of 4 GiB, and its data plane
    /// stopped accepting connections while every other field here read healthy.
    pub warm_budget_handles: u64,
    /// Open append descriptors across all resident hash groups.
    ///
    /// A group opens its append handle on its first spill, so this sits below
    /// `warm_resident_groups` — 5,048 against 13,818 in steady state on the
    /// primary, measured 2026-07-30 — and confusing the two is not hypothetical.
    /// Between 09:23Z and 10:11Z that day the primary reported 4,915 resident
    /// groups against a 4,915 cap, which reads as a warm set at its descriptor
    /// ceiling; it was holding 2, because the eviction the cap was driving closed
    /// the files as fast as it made them. The cap throttled the warm set to 10%
    /// of its byte budget and cost 4.7M cold loads in 43 minutes. This field is
    /// the one that would have shown it.
    pub open_append_handles: u64,
    /// Multi-op LastStore transactions that restored prior values after a
    /// mid-batch error. Zero on a healthy node.
    pub torn_transaction_rollbacks: u64,
    /// Transactions whose restore or rollback flush failed.
    /// A nonzero value means a failed transaction may have left a mixed row.
    pub torn_transaction_rollback_failures: u64,
    /// Committed transactions whose post-commit cache refresh failed.
    pub transaction_residency_refresh_failures: u64,
    /// Keys-only group resolutions that found the group already warm.
    ///
    /// This and the three fields below are the id tiers under the warm body
    /// set. They exist because the body tier was the only instrumented one:
    /// `cold_shard_loads` and `eviction_events` have reported since fold #906
    /// and the two cheap tiers below them reported nothing, so a read-thrash
    /// diagnosis could only reach for the warm byte budget — the most
    /// expensive knob, because it is charged 1:1 into the process memory
    /// budget and multiplied into the projection against the guard ceiling.
    ///
    /// The four sum to the number of keys-only resolutions.
    pub id_tier_resident: u64,
    /// Keys-only resolutions served by the in-memory key-index cache.
    pub id_tier_key_cache_hits: u64,
    /// Keys-only resolutions served by the on-disk id sidecar.
    pub id_tier_sidecar_hits: u64,
    /// Keys-only resolutions that missed both id tiers and scanned.
    pub id_tier_live_scans: u64,
    /// Groups whose ids are retained in the key-index cache right now.
    pub key_cache_groups: u64,
    /// Charged bytes of those retained id lists.
    pub key_cache_bytes: u64,
    /// Configured key-index cache budget. `0` means the cache is disabled.
    pub key_cache_budget_bytes: u64,
}
