use super::{BackupProgressSnapshot, BackupProgressTracker, LastCycle};
use crate::clock::unix_secs;

impl BackupProgressTracker {
    pub fn snapshot(&self) -> BackupProgressSnapshot {
        if !self.enabled {
            return Self::disabled_snapshot();
        }

        let Some(last) = &self.last else {
            return self.unsampled_snapshot();
        };

        let remaining = last.chunks_total.saturating_sub(last.chunks_present);
        // Mirrors `observe()`'s definition. A cycle that enumerated nothing and
        // walked nothing genuinely has nothing outstanding — distinct from
        // "no cycle has ever run", which is the `last == None` branch above.
        // Mirrors `observe()`, including the unbackable clause: see there for
        // why a fully-drained cut with manifest-only refs is not complete.
        let cut_is_terminal = last.source_missing > 0 || last.unbackable_manifest_chunks > 0;
        let complete = !cut_is_terminal
            && ((last.chunks_total > 0 && remaining == 0)
                || (last.chunks_total == 0
                    && last.uploaded == 0
                    && last.already_present_walked == 0));
        let percent = if last.chunks_total == 0 {
            None
        } else {
            Some(
                ((last.chunks_present as f64) / (last.chunks_total as f64) * 1000.0).round() / 10.0,
            )
        };

        let elapsed_secs = self.session_started.map(|t| t.elapsed().as_secs());

        // An ETA is a promise about when this finishes. The EWMA is fed gross
        // put bytes and cannot see erasure, so on a churning home it happily
        // quoted "~56m" through a window in which the backup moved BACKWARDS.
        // Refuse to quote one unless the session is actually gaining ground.
        let (recent_gained, recent_erased) = self.recent_totals();
        let net_progressing = recent_gained > recent_erased;
        let eta_secs = if complete || cut_is_terminal || (recent_erased > 0 && !net_progressing) {
            None
        } else {
            self.estimate_eta_secs(last, remaining)
        };

        BackupProgressSnapshot {
            enabled: true,
            complete,
            percent,
            chunks_total: last.chunks_total,
            chunks_present: last.chunks_present,
            chunks_remaining: remaining,
            bytes_remaining: last.bytes_remaining,
            elapsed_secs: if complete { None } else { elapsed_secs },
            eta_secs,
            ewma_upload_bps: self.ewma_bps,
            ewma_link_bps: self.ewma_link_bps,
            ewma_overhead_secs: self.ewma_overhead_secs,
            last_cycle_uploaded: last.uploaded,
            last_cycle_already_present: last.already_present_walked,
            last_cycle_bytes_uploaded: last.bytes_uploaded,
            last_cycle_failed: last.failed,
            chunks_source_missing: last.source_missing,
            chunks_unbackable_manifest: last.unbackable_manifest_chunks,
            cas_counter: last.cas_counter,
            // An unbroken failure streak outranks whatever the last drain
            // sample was doing. Without this, a home whose CAS fails every
            // cycle still reports the drain's phase (`chunks_only`), which is
            // true about the drain and misleading about the backup.
            phase: if self.consecutive_failures > 0 {
                "failing".into()
            } else {
                last.phase.clone()
            },
            // A terminal cut gets no progress bar. `complete` is false here,
            // so without the guard the CLI would start a bar that can only
            // ever sit at 100% — a worse lie than the one this fix removed.
            show_progress: !complete && !cut_is_terminal && last.chunks_total > 0,
            last_success_unix: self.last_success_unix,
            last_success_age_secs: self
                .last_success_unix
                .map(|t| unix_secs().saturating_sub(t)),
            consecutive_failures: self.consecutive_failures,
            last_error: self.last_error.clone(),
            chunks_gained: self.session_gained,
            chunks_erased: self.session_erased,
            net_progressing,
            recent_gained,
            recent_erased,
            net_progress_window_cycles: self.recent_deltas.len() as u64,
            cycles_since_net_gain: self.cycles_since_net_gain,
            target_generation: last.target_generation,
            last_publish_unix: self.last_publish_unix,
            last_publish_age_secs: self
                .last_publish_unix
                .map(|t| unix_secs().saturating_sub(t)),
        }
    }

    fn disabled_snapshot() -> BackupProgressSnapshot {
        BackupProgressSnapshot {
            enabled: false,
            complete: true,
            percent: None,
            chunks_total: 0,
            chunks_present: 0,
            chunks_remaining: 0,
            bytes_remaining: None,
            elapsed_secs: None,
            eta_secs: None,
            ewma_upload_bps: None,
            ewma_link_bps: None,
            ewma_overhead_secs: None,
            last_cycle_uploaded: 0,
            last_cycle_already_present: 0,
            last_cycle_bytes_uploaded: 0,
            last_cycle_failed: 0,
            chunks_source_missing: 0,
            chunks_unbackable_manifest: 0,
            cas_counter: None,
            phase: "disabled".into(),
            show_progress: false,
            last_success_unix: None,
            last_success_age_secs: None,
            consecutive_failures: 0,
            last_error: None,
            chunks_gained: 0,
            chunks_erased: 0,
            net_progressing: false,
            recent_gained: 0,
            recent_erased: 0,
            net_progress_window_cycles: 0,
            cycles_since_net_gain: 0,
            target_generation: None,
            last_publish_unix: None,
            last_publish_age_secs: None,
        }
    }

    /// Snapshot for a tracker that has produced no cycle sample in this process.
    fn unsampled_snapshot(&self) -> BackupProgressSnapshot {
        // No cycle has produced a sample in this process. A durable
        // full-manifest commit still proves the home has a restore base;
        // without that proof this remains the loud "never completed"
        // state that caught the original silent durability gap.

        BackupProgressSnapshot {
            enabled: true,
            // An outstanding abandon outranks the restored commit: the base
            // it names is exactly the one that proved unpublishable.
            complete: self.has_published && !self.sealed_base_abandoned,
            percent: None,
            chunks_total: 0,
            chunks_present: 0,
            chunks_remaining: 0,
            bytes_remaining: None,
            elapsed_secs: None,
            eta_secs: None,
            ewma_upload_bps: self.ewma_bps,
            ewma_link_bps: self.ewma_link_bps,
            ewma_overhead_secs: self.ewma_overhead_secs,
            last_cycle_uploaded: 0,
            last_cycle_already_present: 0,
            last_cycle_bytes_uploaded: 0,
            last_cycle_failed: 0,
            chunks_source_missing: 0,
            chunks_unbackable_manifest: 0,
            cas_counter: None,
            phase: if self.consecutive_failures > 0 || self.sealed_base_abandoned {
                "failing".into()
            } else if self.has_published {
                "published".into()
            } else {
                "never_completed".into()
            },
            show_progress: false,
            last_success_unix: self.last_success_unix,
            last_success_age_secs: self
                .last_success_unix
                .map(|t| unix_secs().saturating_sub(t)),
            consecutive_failures: self.consecutive_failures,
            last_error: self.last_error.clone(),
            chunks_gained: self.session_gained,
            chunks_erased: self.session_erased,
            net_progressing: false,
            recent_gained: 0,
            recent_erased: 0,
            net_progress_window_cycles: 0,
            cycles_since_net_gain: self.cycles_since_net_gain,
            target_generation: self.session_generation,
            last_publish_unix: self.last_publish_unix,
            last_publish_age_secs: self
                .last_publish_unix
                .map(|t| unix_secs().saturating_sub(t)),
        }
    }

    /// Seconds until the backup finishes, or `None` when no honest number
    /// exists yet.
    ///
    /// # Why this is not `bytes_remaining / bytes_per_second`
    ///
    /// That was the old estimator, and on a large home it was wrong by more
    /// than an order of magnitude in the pessimistic direction. Measured on the
    /// primary 2026-08-01 at 92.9% complete: it quoted **86 hours** where the
    /// drain was converging in about **6**.
    ///
    /// The cause is a unit mismatch, not a slow link. A drain cycle is bounded
    /// by a per-cycle *chunk count* budget (`LASTDB_BACKUP_UPLOAD_TARGET_PER_CYCLE`,
    /// 16 on that host), and it walks every already-present candidate to find
    /// them. So one cycle was ~60s: ~55s walking 15k present chunks and ~5s
    /// actually transferring 16. Dividing bytes by the whole cycle yields a
    /// "rate" that is really *recent average chunk size ÷ cycle time* — a
    /// number about the chunks just drained, not about the link. Projecting it
    /// onto a backlog whose chunks were 60x larger overstated the remainder
    /// wildly. Three consecutive live samples showed it tracking chunk size
    /// exactly: when the drained chunks doubled in size the quoted ETA fell
    /// from 86h to 66h, with no change in link speed.
    ///
    /// It also *degraded as the backup approached completion*: the walk grows
    /// with `chunks_present`, so bytes-per-cycle-second decays monotonically and
    /// the ETA rises while progress is strictly forward. An estimate that gets
    /// worse the closer you get is not a rounding error, it is the wrong model.
    ///
    /// # The model used instead
    ///
    /// Both real constraints are charged, in the units they are actually
    /// denominated in:
    ///
    /// ```text
    /// eta = bytes_remaining / link_bps          // bandwidth-bound part
    ///     + cycles_remaining * overhead_secs    // count-bound part (the walk)
    /// ```
    ///
    /// They are summed rather than maxed because within a cycle they are
    /// sequential: every cycle pays its walk and *then* transfers.
    fn estimate_eta_secs(&self, last: &LastCycle, remaining: u64) -> Option<u64> {
        if remaining == 0 {
            return None;
        }

        // Bytes still to move. Prefer the exact sum the drain computed; fall
        // back to the last cycle's average chunk size when it is unavailable.
        let bytes_remaining = match last.bytes_remaining {
            Some(bytes) if bytes > 0 => bytes as f64,
            _ if last.uploaded > 0 && last.bytes_uploaded > 0 => {
                (last.bytes_uploaded as f64 / last.uploaded as f64) * remaining as f64
            }
            _ => return None,
        };

        let link_bps = self.ewma_link_bps.filter(|b| *b > 1.0)?;

        // A cycle that moved nothing gives no basis for bounding how many more
        // cycles this takes. Quoting the bandwidth-bound floor alone would be a
        // knowingly-optimistic promise about durability, so quote nothing —
        // same ethic as the erasure guard above.
        if last.uploaded == 0 {
            return None;
        }
        let cycles_remaining = remaining.div_ceil(last.uploaded) as f64;
        let overhead_secs = self.ewma_overhead_secs.unwrap_or(0.0).max(0.0);

        let secs = bytes_remaining / link_bps + cycles_remaining * overhead_secs;
        Some(secs.ceil() as u64)
    }

    /// Gained/erased summed over the recent window that decides
    /// `net_progressing`.
    fn recent_totals(&self) -> (u64, u64) {
        self.recent_deltas.iter().fold((0, 0), |(g, e), d| {
            (g.saturating_add(d.gained), e.saturating_add(d.erased))
        })
    }
}
