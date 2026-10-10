use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WarmAdmission {
    Point,
    Scan,
}

/// Who admitted a molecule-tip group. Not stored on [`ShardKey`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AdmitClass {
    /// No schema owner. Stays on the historical LRU path.
    Unspecified,
    /// Factory, null, or any owner other than `lastgit`.
    Interactive,
    /// `lastgit`, plus maintenance that opts in. Unpublished when it does not fit.
    Background,
}

/// Class and atom-plane bit for one admit. Scan uses `atom` and ignores `class`.
#[derive(Clone, Copy)]
pub(super) struct WarmTouch {
    pub(super) class: AdmitClass,
    pub(super) atom: bool,
}

impl WarmTouch {
    pub(super) fn unspecified() -> Self {
        Self {
            class: AdmitClass::Unspecified,
            atom: false,
        }
    }
}

/// What one resident group costs the warm budget, split by what it would take
/// to get the bytes back.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct Residency {
    /// Everything the group is charged.
    pub(super) total: u64,
    /// The part of `total` that is a copy of bytes already on disk, and so can
    /// be released without costing a cold load.
    pub(super) trimmable: u64,
}

/// Loader pin table: one handle per group plus the bytes that pin currently
/// charges. `loader_pin_bytes` is the sum of `charged_bytes`.
#[derive(Default)]
pub(super) struct PinTable {
    pub(super) handles: HashMap<ShardKey, ShardHandle>,
    pub(super) charged_bytes: HashMap<ShardKey, u64>,
}

pub(super) fn apply_loader_pin_byte_delta(total: &AtomicU64, old: u64, new: u64) {
    if new >= old {
        total.fetch_add(new - old, Ordering::Relaxed);
    } else {
        total.fetch_sub(old - new, Ordering::Relaxed);
    }
}

/// Progress of one attempt to make warm-set room for a charge.
#[derive(Default)]
pub(super) struct WarmRoom {
    pub(super) trimmed_own: bool,
    pub(super) passes: u32,
    pub(super) exhausted: bool,
}

impl WarmRoom {
    /// Eviction passes before a point charge is published regardless. Each
    /// pass either reaches its limit or finds nothing left to evict; more
    /// passes are only needed when concurrent admissions keep taking the room.
    const MAX_PASSES: u32 = 8;

    /// Give up the charged group's own reproducible bytes, once, before
    /// anything else is trimmed or evicted for it. Returns the new residency
    /// when there was something to trim.
    pub(super) fn trim_own_caches(
        &mut self,
        handle: &ShardHandle,
        residency: Residency,
    ) -> Option<Residency> {
        if self.trimmed_own || residency.trimmable == 0 {
            return None;
        }
        self.trimmed_own = true;
        let mut shard = handle.lock().expect("poison");
        trim_shard_read_caches(&mut shard);
        Some(estimate_shard_residency_locked(&shard))
    }

    pub(super) fn record(&mut self, reached: bool) {
        self.passes += 1;
        if !reached || self.passes >= Self::MAX_PASSES {
            self.exhausted = true;
        }
    }
}

/// What the warm-set lock holder decided about one admission.
pub(super) enum Admit {
    /// The caller gets this handle; it is published, or leased when a scan
    /// group did not fit.
    Done(ShardHandle),
    /// A scan lease already holds this group's handle; admit that one instead.
    Adopt(ShardHandle),
    /// Other groups must get down to this many resident bytes first; `None`
    /// when the charge alone is larger than the whole budget.
    MakeRoom(Option<u64>),
}
