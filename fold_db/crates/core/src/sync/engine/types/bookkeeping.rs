use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

pub(crate) const MAX_PRESIGN_BATCH: usize = 1000;
pub(crate) const PERSONAL_LOG_INDEX_SNAPSHOT: &str = "log_index.enc";
pub(crate) const SYNC_OUTBOX_NAMESPACE: &str = "sync_outbox";
pub(crate) const SYNC_OUTBOX_ENTRY_PREFIX: &str = "entry:";
pub(crate) const CLOUD_SYNC_BACKLOG_THRESHOLDS: [u8; 4] = [50, 75, 90, 95];
pub(crate) const CLOUD_SYNC_BACKLOG_MIN_FAILURES: u64 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CloudSyncFailureKey {
    pub(crate) sync_target: String,
    pub(crate) failure_class: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CloudSyncAlertEmissionKey {
    pub(crate) sync_target: String,
    pub(crate) failure_class: String,
    pub(crate) threshold_percent: u8,
}

#[derive(Debug, Clone)]
pub(crate) struct CloudSyncFailureStats {
    pub(crate) count: u64,
    pub(crate) last_error: String,
}

#[derive(Debug, Default)]
pub(crate) struct CloudSyncBacklogAlertState {
    pub(crate) failures: HashMap<CloudSyncFailureKey, CloudSyncFailureStats>,
    pub(crate) emitted: HashSet<CloudSyncAlertEmissionKey>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct CloudSyncBacklogSnapshot {
    pub(crate) pending_count: usize,
    pub(crate) max_pending: usize,
    pub(crate) threshold_percent: u8,
    pub(crate) oldest_pending_age_secs: Option<u64>,
    pub(crate) last_success_age_secs: Option<u64>,
}

/// In-memory bookkeeping for the durable outbox so the hot write path and
/// `status()` never scan or deserialize the whole outbox to answer "how many
/// entries are pending?".
///
/// The outbox namespace (`sync_outbox`) is mutated **only** by this engine
/// (`persist_outbox_entry` / `forget_recorded_op` / `remove_outbox_entries`),
/// so an in-process counter maintained at exactly those points is authoritative
/// for the engine's lifetime. `count` is seeded once, on first access, from a
/// single key scan (the sole `O(backlog)` outbox read left on the hot paths —
/// it happens at most once per process, never per write) and is re-derived
/// accurately on the next process start.
///
/// We deliberately keep this in memory rather than persisting a counter row in
/// the store: a separate counter row could not be updated atomically with the
/// entry write (there is no cross-key transaction here), so a crash between the
/// two writes would desync it permanently with no self-healing. An in-memory
/// counter is exactly correct within a process and re-derives itself on restart.
#[derive(Debug, Default)]
pub(crate) struct OutboxMeta {
    /// Number of entries currently in the durable outbox. `None` until the
    /// first scan seeds it.
    pub(crate) count: Option<usize>,
    /// Highest entry seq ever persisted. Used to advance `self.seq` past any
    /// persisted entry on restart so a regressed wall clock can never mint a
    /// seq that collides with an existing outbox key.
    pub(crate) max_seq: u64,
    /// Target configuration generation whose durable writer HWMs seeded
    /// `SyncEngine::seq`. A target reconfiguration increments the engine
    /// generation, so the next mint performs bounded point reads again.
    /// `None` forces the initial process-start seed even when generation zero
    /// is the first configured target set.
    pub(crate) seeded_target_generation: Option<u64>,
    /// Whether this process validated the durable allocation floor against all
    /// existing pin-row keys. A restart resets this flag, which makes a newer
    /// binary detect rows appended by an older binary that did not update the
    /// `appended_f:global` point row during a rollback interval.
    pub(crate) validated_pin_log_allocation_floor: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct PersonalLogIndex {
    pub(crate) version: u8,
    pub(crate) seqs: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct UnsealFailureLogKey {
    pub(crate) target_label: String,
    pub(crate) seq: u64,
    pub(crate) reason: String,
}

impl PersonalLogIndex {
    pub(crate) fn from_seqs<I>(seqs: I) -> Self
    where
        I: IntoIterator<Item = u64>,
    {
        let mut seqs: Vec<u64> = seqs.into_iter().collect();
        seqs.sort_unstable();
        seqs.dedup();
        Self { version: 1, seqs }
    }

    pub(crate) fn seqs_after(&self, cursor: u64) -> Vec<u64> {
        self.seqs
            .iter()
            .copied()
            .filter(|seq| *seq > cursor)
            .collect()
    }

    pub(crate) fn append<I>(&mut self, seqs: I)
    where
        I: IntoIterator<Item = u64>,
    {
        self.seqs.extend(seqs);
        self.seqs.sort_unstable();
        self.seqs.dedup();
    }
}
