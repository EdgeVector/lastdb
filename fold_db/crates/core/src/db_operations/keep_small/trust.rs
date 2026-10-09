//! Trust state: incomplete / stale / hydrate-missed markers that stop a report from reading as exact.

use super::*;

impl KeepSmallMeters {
    /// Current persisted trust payload.
    #[must_use]
    pub fn trust(&self) -> MeterTrustPayload {
        self.trust.lock().map_or_else(
            |_| MeterTrustPayload::legacy_incomplete(),
            |trust| trust.clone(),
        )
    }

    /// Mark every affected domain incomplete before a failed load or repair.
    pub fn mark_incomplete(&self, cause: &str) {
        if let Ok(mut trust) = self.trust.lock() {
            trust.mark_incomplete(cause);
        }
        self.molecule_counters_complete
            .store(false, Ordering::Relaxed);
    }

    /// Record the exact empty-home exception for a missing snapshot.
    pub fn mark_empty_home_absent(&self) {
        if let Ok(mut trust) = self.trust.lock() {
            *trust = MeterTrustPayload::absent();
        }
        self.hydrate_missed.store(false, Ordering::Relaxed);
        self.stale_after_unclean_stop
            .store(false, Ordering::Relaxed);
        self.molecule_counters_complete
            .store(true, Ordering::Relaxed);
    }

    /// The global trust state, independent of the process exit flag.
    #[must_use]
    pub fn global_trust_state(&self) -> MeterTrustState {
        self.trust
            .lock()
            .map_or(MeterTrustState::Incomplete, |trust| trust.global.state)
    }

    /// True when the global, schema, and molecule domains have exact evidence.
    #[must_use]
    pub fn all_counter_domains_complete(&self) -> bool {
        let Ok(trust) = self.trust.lock() else {
            return false;
        };
        if trust.global.state == MeterTrustState::Absent
            && trust.molecules.state == MeterTrustState::Absent
            && trust.schemas.is_empty()
            && self.molecule_counters_complete.load(Ordering::Relaxed)
        {
            return true;
        }
        trust.global.state.is_complete()
            && trust.molecules.state.is_complete()
            && trust
                .schemas
                .values()
                .all(|domain| domain.state.is_complete())
            && self.molecule_counters_complete.load(Ordering::Relaxed)
    }

    /// Record that the hydrated snapshot predates writes the store took
    /// before an unclean stop. See [`Self::stale_after_unclean_stop`].
    pub fn mark_stale_after_unclean_stop(&self) {
        self.stale_after_unclean_stop.store(true, Ordering::Relaxed);
        self.mark_incomplete("stale_after_unclean_stop");
    }

    /// True when boot hydrated a snapshot that the store had already moved
    /// past: the totals and counters are a hint from the last debounce tick,
    /// not a measurement of the home as it is.
    #[must_use]
    pub fn stale_after_unclean_stop(&self) -> bool {
        self.stale_after_unclean_stop.load(Ordering::Relaxed)
    }

    /// Why [`Self::molecule_counters_complete`] is false, as a stable wire
    /// token for the storage reports, or `None` when it is true.
    #[must_use]
    pub fn incomplete_reason(&self) -> Option<&'static str> {
        if self.hydrate_missed() {
            Some("hydrate_missed")
        } else if self.stale_after_unclean_stop() {
            Some("stale_after_unclean_stop")
        } else if !self.molecule_counters_complete.load(Ordering::Relaxed) {
            Some("molecule_counters_not_bootstrapped")
        } else {
            None
        }
    }

    /// Persisted global trust cause for machine-readable status surfaces.
    #[must_use]
    pub fn trust_incomplete_cause(&self) -> Option<String> {
        self.trust.lock().ok().and_then(|trust| {
            (trust.global.state == MeterTrustState::Incomplete)
                .then(|| trust.global.cause.clone())
                .flatten()
        })
    }

    /// Record that boot found no durable snapshot to hydrate from.
    ///
    /// See [`Self::hydrate_missed`] for why this is sticky.
    pub fn mark_hydrate_missed(&self) {
        self.hydrate_missed.store(true, Ordering::Relaxed);
        self.mark_incomplete("hydrate_missed");
    }

    /// True when boot looked for the durable snapshot and did not find it, so
    /// the totals cover only what this process has written.
    #[must_use]
    pub fn hydrate_missed(&self) -> bool {
        self.hydrate_missed.load(Ordering::Relaxed)
    }
}
