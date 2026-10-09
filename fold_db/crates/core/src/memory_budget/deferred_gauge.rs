//! Deferred-persist byte gauge and reservations.

use super::*;

/// Live accounting for acked-but-unpersisted deferred work, against the byte
/// cap the process budget granted.
///
/// The count cap (`LASTDB_RESIDENT_MAX_DEFERRED`) bounds how many tasks may be
/// in flight; this bounds how much **memory** they hold. Those are different
/// resources: 512 tasks each holding a 512 KiB atom batch is 256 MiB, and the
/// same 512 tasks holding one small record each is under a megabyte. The
/// 2026-07-29 balloon was the first shape passing a cap written for the second.
#[derive(Debug)]
pub struct DeferredPersistGauge {
    in_flight_bytes: AtomicU64,
    in_flight_count: AtomicUsize,
    cap_bytes: AtomicU64,
    cap_count: usize,
    /// The cap the window reopens to after a latch recovers. Set once at
    /// construction; a latch stores 0 into `cap_bytes`, not here.
    armed_cap_bytes: u64,
    projection_diverged: AtomicBool,
    footprint_over_limit: AtomicBool,
    runtime_degraded: AtomicBool,
    /// Monotonic millis of the first consecutive sample under the recovery
    /// line while latched; 0 when no recovery hold is running.
    recovery_since_millis: AtomicU64,
}

impl DeferredPersistGauge {
    #[must_use]
    pub const fn new(cap_bytes: u64) -> Self {
        Self::new_with_count(cap_bytes, usize::MAX)
    }

    #[must_use]
    pub const fn new_with_count(cap_bytes: u64, cap_count: usize) -> Self {
        Self {
            in_flight_bytes: AtomicU64::new(0),
            in_flight_count: AtomicUsize::new(0),
            cap_bytes: AtomicU64::new(cap_bytes),
            cap_count,
            armed_cap_bytes: cap_bytes,
            projection_diverged: AtomicBool::new(false),
            footprint_over_limit: AtomicBool::new(false),
            runtime_degraded: AtomicBool::new(false),
            recovery_since_millis: AtomicU64::new(0),
        }
    }

    /// Gauge sized from the process budget with a task-count cap.
    #[must_use]
    pub fn from_process_budget_with_count(cap_count: usize) -> Self {
        Self::new_with_count(process_memory_budget().deferred_cap_bytes, cap_count)
    }

    #[must_use]
    pub fn cap_bytes(&self) -> u64 {
        self.cap_bytes.load(Ordering::Acquire)
    }

    #[must_use]
    pub const fn cap_count(&self) -> usize {
        self.cap_count
    }

    #[must_use]
    pub fn in_flight_bytes(&self) -> u64 {
        self.in_flight_bytes.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn in_flight_count(&self) -> usize {
        self.in_flight_count.load(Ordering::Acquire)
    }

    /// Why `try_reserve` refused, for callers that must report it.
    ///
    /// `try_reserve` answers `false` for two states that mean opposite things
    /// to an operator, and it cannot distinguish them from its return value
    /// alone. Callers used to log both as "defer cap reached", so a window
    /// that was switched off announced itself, once per write batch, as a
    /// window under pressure.
    #[must_use]
    pub fn refusal_kind(&self) -> DeferRefusal {
        if self.cap_bytes() == 0 || self.cap_count == 0 {
            DeferRefusal::Disabled
        } else {
            DeferRefusal::WindowFull
        }
    }

    /// Admit `bytes` of deferred work if it fits under the cap.
    ///
    /// Returns `false` when it does not — the caller then persists inline,
    /// which is always correct and always makes progress. A batch larger than
    /// the whole cap therefore never defers, by design: there is no starvation
    /// because the inline path needs no admission.
    #[must_use]
    pub fn try_reserve(&self, bytes: u64) -> bool {
        let cap_bytes = self.cap_bytes();
        if cap_bytes == 0 || self.cap_count == 0 {
            return false;
        }
        if self
            .in_flight_count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current
                    .checked_add(1)
                    .filter(|next| *next <= self.cap_count)
            })
            .is_err()
        {
            return false;
        }
        if self
            .in_flight_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                let next = current.saturating_add(bytes);
                (next <= cap_bytes).then_some(next)
            })
            .is_ok()
        {
            return true;
        }
        self.release_count();
        false
    }

    /// Compare a live physical-footprint sample with the boot projection and
    /// enforce the promised fail-safe.
    ///
    /// **Near-limit trips keep the derived defer window** when the boot budget
    /// still fits: a 1–2 MiB excursion over a tight ceiling (Sentry RUST-3M /
    /// 12290 vs 12288 MiB) must not permanently force every mode=write mutation
    /// onto the inline path. The over-limit edge still raises
    /// `over_limit_alarm_now` so operators see the trip.
    ///
    /// **The alarm is one per episode.** It fires on the crossing, then stays
    /// quiet until the footprint falls below
    /// [`footprint_over_limit_clear_threshold`]. A footprint parked at the
    /// ceiling otherwise re-alarms on every dip and re-cross, which counts
    /// status polls rather than incidents.
    ///
    /// **Hard latch (cap → 0)** only when:
    /// - the boot budget itself cannot fit under the guard (`!budget.fits` or
    ///   derived/explicit deferred cap is already zero), or
    /// - measured footprint overruns the configured limit by at least
    ///   [`FOOTPRINT_HARD_OVERRUN_FRACTION`] (floored at
    ///   [`FOOTPRINT_HARD_OVERRUN_MIN_BYTES`]) — a real runaway, not jitter.
    ///
    /// **The latch recovers.** Once the footprint has stayed under
    /// [`footprint_latch_recovery_line`] for
    /// [`FOOTPRINT_LATCH_RECOVERY_HOLD_SECS`], the window reopens to the cap it
    /// was built with and `runtime_recovered_now` fires once. A sample over the
    /// line restarts the hold; a new runaway latches again on its own sample.
    /// A budget that never had headroom never recovers: its armed cap is zero.
    ///
    /// Already-admitted tasks always drain normally after a hard latch; new
    /// batches take the inline durable path.
    #[must_use]
    pub fn observe_footprint(
        &self,
        measured_footprint_bytes: u64,
        budget: &ProcessMemoryBudget,
    ) -> RuntimeMemoryBudgetObservation {
        self.observe_footprint_at(measured_footprint_bytes, budget, monotonic_millis())
    }

    /// [`Self::observe_footprint`] with an explicit monotonic clock, so the
    /// recovery hold is testable without sleeping.
    #[must_use]
    pub fn observe_footprint_at(
        &self,
        measured_footprint_bytes: u64,
        budget: &ProcessMemoryBudget,
        now_millis: u64,
    ) -> RuntimeMemoryBudgetObservation {
        let projection_tolerance_bytes = scale_bytes(
            budget.projected_total_bytes,
            FOOTPRINT_PROJECTION_TOLERANCE_FRACTION,
        );
        let projection_ceiling = budget
            .projected_total_bytes
            .saturating_add(projection_tolerance_bytes);
        let projection_diverged = measured_footprint_bytes > projection_ceiling;
        let footprint_over_limit = measured_footprint_bytes > budget.rss_limit_bytes;
        let hard_overrun = footprint_hard_overrun(measured_footprint_bytes, budget.rss_limit_bytes);
        // Projected budget still has a non-zero defer window → near-limit trips
        // stay soft. No-headroom / cannot-fit configs keep the old hard latch.
        let projected_has_headroom = budget.fits && budget.deferred_cap_bytes > 0;
        let should_hard_latch = footprint_over_limit && (!projected_has_headroom || hard_overrun);

        let was_diverged = self
            .projection_diverged
            .swap(projection_diverged, Ordering::AcqRel);
        // Hysteresis on the alarm edge only: an episode stays open until the
        // footprint falls a clear band under the limit. The raw
        // `footprint_over_limit` fact and the hard-latch decision below still
        // read the current sample, so a runaway latches on the sample that
        // proves it.
        let was_over_limit = self.footprint_over_limit.load(Ordering::Acquire);
        let alarm_episode_open = if was_over_limit {
            measured_footprint_bytes > footprint_over_limit_clear_threshold(budget.rss_limit_bytes)
        } else {
            footprint_over_limit
        };
        self.footprint_over_limit
            .store(alarm_episode_open, Ordering::Release);

        let mut runtime_recovered_now = false;
        if should_hard_latch {
            self.runtime_degraded.store(true, Ordering::Release);
            self.cap_bytes.store(0, Ordering::Release);
            self.recovery_since_millis.store(0, Ordering::Release);
        } else if self.runtime_degraded.load(Ordering::Acquire)
            && self.armed_cap_bytes > 0
            && projected_has_headroom
            && measured_footprint_bytes <= footprint_latch_recovery_line(budget)
        {
            // 0 means "no hold running", so the first sample of a hold is
            // stamped at least 1 even on a clock that reads zero.
            let now = now_millis.max(1);
            let since = self.recovery_since_millis.load(Ordering::Acquire);
            if since == 0 {
                self.recovery_since_millis.store(now, Ordering::Release);
            } else if now.saturating_sub(since) >= FOOTPRINT_LATCH_RECOVERY_HOLD_SECS * 1000 {
                self.cap_bytes
                    .store(self.armed_cap_bytes, Ordering::Release);
                self.runtime_degraded.store(false, Ordering::Release);
                self.recovery_since_millis.store(0, Ordering::Release);
                runtime_recovered_now = true;
            }
        } else {
            self.recovery_since_millis.store(0, Ordering::Release);
        }

        RuntimeMemoryBudgetObservation {
            measured_footprint_bytes,
            projected_total_bytes: budget.projected_total_bytes,
            projection_tolerance_bytes,
            projection_diverged,
            footprint_over_limit,
            runtime_degraded: self.runtime_degraded.load(Ordering::Acquire),
            effective_deferred_cap_bytes: self.cap_bytes(),
            projection_warning_now: projection_diverged && !was_diverged,
            over_limit_alarm_now: alarm_episode_open && !was_over_limit,
            runtime_recovered_now,
        }
    }

    /// Return `bytes` to the cap. Saturates at zero rather than wrapping: an
    /// unbalanced release must not hand out `u64::MAX` of phantom capacity.
    pub fn release(&self, bytes: u64) {
        let _ = self
            .in_flight_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(bytes))
            });
        self.release_count();
    }

    fn release_count(&self) {
        let _ = self
            .in_flight_count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(1))
            });
    }
}

/// Reservation held by an in-flight deferred task, released on drop.
///
/// Drop-based release is what makes the accounting leak-proof: every early
/// return in the deferred task — and a panic — gives the bytes back, so a
/// failed persist cannot permanently shrink the defer window.
#[derive(Debug)]
pub struct DeferReservation {
    gauge: std::sync::Arc<DeferredPersistGauge>,
    bytes: u64,
}

impl DeferReservation {
    #[must_use]
    pub fn new(gauge: std::sync::Arc<DeferredPersistGauge>, bytes: u64) -> Self {
        Self { gauge, bytes }
    }

    #[must_use]
    pub const fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl Drop for DeferReservation {
    fn drop(&mut self) {
        self.gauge.release(self.bytes);
    }
}
