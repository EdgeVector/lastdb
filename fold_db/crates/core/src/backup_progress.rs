//! Sealed-chunk cloud backup progress for operator status.
//!
//! Tracks incomplete LastStore backup sessions so `lastdb status` / `/api/status`
//! can show percent complete, elapsed time, and ETA while not fully caught up.
//! When fully caught up, progress is suppressed (no noisy "100%" spam).

use crate::clock::unix_secs;
use serde::Serialize;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Smoothing factor for EWMA of upload bytes/sec (0 < α ≤ 1).
const EWMA_ALPHA: f64 = 0.35;

/// How many recent cycles decide whether the backup is gaining ground.
///
/// `net_progressing` used to compare the whole session's gained against its
/// erased. That answers "has this session gained ground since it started",
/// which is the wrong question the moment a session changes regime: a run that
/// banks a large gain while the link is healthy keeps reporting progress
/// through an arbitrarily long stretch of pure loss, because the old surplus
/// never expires.
///
/// Measured on the primary 2026-08-05T23:12Z, one cut (generation 502), no
/// uploads possible because the cloud account was over quota: `chunks_present`
/// fell 10,449 -> 10,303 in 120 seconds — 146 erased, 0 gained, and every
/// sample in that window reported `net_progressing: true`. The session had
/// banked 4,640 gains in its healthy hours, so the flag could not have gone
/// false until ~9,000 further chunks were erased, by which point the cut is
/// gone. The bar fell 90.1% -> 88.9% while the line read "ETA calculating…".
///
/// A bounded window is what makes the flag answer "is it gaining ground NOW".
/// Sixteen cycles is long enough that one reseal burst inside an otherwise
/// healthy drain does not flip it, and short enough that a regime change
/// surfaces in minutes rather than never.
const NET_PROGRESS_WINDOW_CYCLES: usize = 16;

/// Snapshot exposed to status consumers (JSON + CLI).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct BackupProgressSnapshot {
    /// True when a LastStore backup uploader is active for this home.
    pub enabled: bool,
    /// True when sealed backup-eligible work is fully present in cloud.
    pub complete: bool,
    /// Percent of sealed backup-eligible chunks already in cloud (0–100).
    /// `None` when total is unknown / zero (no candidates yet).
    pub percent: Option<f64>,
    pub chunks_total: u64,
    pub chunks_present: u64,
    pub chunks_remaining: u64,
    /// Estimated remaining payload bytes (from candidate sizes when known).
    pub bytes_remaining: Option<u64>,
    /// Seconds since the current incomplete session started.
    pub elapsed_secs: Option<u64>,
    /// Estimated seconds to finish at EWMA rate.
    pub eta_secs: Option<u64>,
    /// EWMA of recent upload throughput (bytes per second of *whole cycle*).
    ///
    /// Retained for continuity, but note this is not the link rate: on a large
    /// home most of a cycle is the walk over already-present candidates, so
    /// this number is dominated by walk time and by how big the recently
    /// drained chunks happened to be. Use `ewma_link_bps` to reason about the
    /// link; this one is only meaningful as "bytes per cycle-second".
    pub ewma_upload_bps: Option<f64>,
    /// EWMA of the true link rate: bytes per second *of transfer*. This is the
    /// rate `eta_secs` charges remaining bytes at.
    pub ewma_link_bps: Option<f64>,
    /// EWMA of per-cycle overhead in seconds (cycle wall time minus transfer
    /// wall time) — the walk. `eta_secs` charges this once per remaining cycle.
    pub ewma_overhead_secs: Option<f64>,
    pub last_cycle_uploaded: u64,
    pub last_cycle_already_present: u64,
    pub last_cycle_bytes_uploaded: u64,
    /// Chunks whose presign/upload/confirm failed in the last drain cycle.
    /// Distinct from `consecutive_failures` (whole-cycle publish failures):
    /// this is partial PUT loss that used to leave zero operator signal.
    #[serde(default)]
    pub last_cycle_failed: u64,
    /// Chunks of the current cut that have no local sealed file, so the cut can
    /// never reach 100% present and can never publish.
    ///
    /// Non-zero is terminal for this generation: it means the home is not
    /// getting more restorable no matter how long the drain runs, and the cut
    /// has to be replaced. Distinguishes "the backup is slow" from "the backup
    /// is over", which `chunks_remaining` alone cannot.
    #[serde(default)]
    pub chunks_source_missing: u64,
    /// Chunks NAMED BY THE CUT'S MANIFEST that have no local candidate to
    /// upload them from — carried forward from an earlier manifest and since
    /// resealed away. Terminal for this generation exactly like
    /// `chunks_source_missing`, and reached by the opposite route.
    ///
    /// The two populations are disjoint by construction: `cut_backup_manifest`
    /// walks the store UNION the previous manifest's `atom_chunks`, while
    /// candidate enumeration walks the store alone, so the manifest universe is
    /// strictly larger. A candidate whose frozen file vanished lands in
    /// `chunks_source_missing`; a manifest-only ref lands here. Both make the
    /// cut unable to reach 100% present, and only the first used to have a
    /// number — the second was reachable only by parsing English out of
    /// `last_error`, while `complete`, `percent` and `chunks_remaining` all
    /// argued the cut had finished.
    #[serde(default)]
    pub chunks_unbackable_manifest: u64,
    pub cas_counter: Option<u64>,
    pub phase: String,
    /// Whether the human CLI should print a progress line (incomplete only).
    pub show_progress: bool,
    /// Unix seconds of the last cycle that completed without error, if any in
    /// this process. `None` means the uploader has never gotten a cycle
    /// through — which is NOT the same as "nothing to do".
    pub last_success_unix: Option<u64>,
    /// Seconds since `last_success_unix`. `None` when no cycle has succeeded.
    pub last_success_age_secs: Option<u64>,
    /// Consecutive failed publish cycles since the last success. Non-zero means
    /// backup is actively failing even though local reads/writes are fine.
    pub consecutive_failures: u32,
    /// Redacted text of the most recent cycle failure, if any.
    pub last_error: Option<String>,
    /// Chunks that entered the present set since this session started.
    ///
    /// Reported next to `chunks_erased` because `chunks_present` alone cannot
    /// distinguish "uploading nothing" from "uploading hard and losing it to
    /// churn". On the 2026-07-31 primary those were 234 gained against 253
    /// erased in 14 minutes, while the bar sat at ~56% quoting "ETA ~56m".
    pub chunks_gained: u64,
    /// Chunks that LEFT the present set since this session started (sealed-chunk
    /// reseal rotating a sha out of the candidate set). Non-zero means uploaded
    /// bytes are being invalidated; a run with `chunks_erased >= chunks_gained`
    /// is not converging, whatever the percentage says.
    pub chunks_erased: u64,
    /// False when the backup is not currently gaining ground, judged over the
    /// last `net_progress_window_cycles` cycles rather than the whole session.
    /// An ETA is only meaningful when this is true, so `eta_secs` is `None`
    /// otherwise.
    ///
    /// Deliberately NOT `chunks_gained > chunks_erased`: those are session
    /// totals, and a session that banked a large early gain keeps satisfying
    /// that comparison through hours of pure loss. See
    /// `NET_PROGRESS_WINDOW_CYCLES`.
    pub net_progressing: bool,
    /// Chunks gained within the recent window that decides `net_progressing`.
    #[serde(default)]
    pub recent_gained: u64,
    /// Chunks erased within the recent window that decides `net_progressing`.
    /// `recent_erased > recent_gained` is the live "losing ground" condition;
    /// the session totals cannot express it.
    #[serde(default)]
    pub recent_erased: u64,
    /// How many cycles that window spans, so a reader can size the two above.
    #[serde(default)]
    pub net_progress_window_cycles: u64,
    /// Cycles observed since the last one that gained ground. Zero while
    /// gaining. This is the plainest form of the question an operator asks —
    /// "how long has it been going backwards" — and unlike the window totals it
    /// keeps counting past the window's edge.
    #[serde(default)]
    pub cycles_since_net_gain: u64,
    /// `manifest.counter` of the cut currently being drained, when there is one.
    pub target_generation: Option<u64>,
    /// Unix seconds of the last successful CAS — the last time a backup actually
    /// became restorable. Distinct from `last_success_unix`, which only means a
    /// drain cycle round-tripped; conflating the two is what let a 10-day-old
    /// backup read green.
    pub last_publish_unix: Option<u64>,
    /// Seconds since `last_publish_unix`. `None` when nothing ever landed.
    pub last_publish_age_secs: Option<u64>,
}

/// One publish/drain cycle sample for the tracker.
///
/// Not `Copy`: `failure_error` carries the cause of a failed cycle, and an
/// owned `String` is what lets that be the real error text rather than a code.
/// Every construction site hands the sample straight to `observe()` and none
/// reuses it afterwards, so a move is all that was ever needed.
#[derive(Debug, Clone, PartialEq)]
pub struct BackupCycleSample {
    /// Total sealed backup-eligible candidates this cycle (full enumerate).
    pub chunks_total: u64,
    /// How many of those are already known present in cloud after the cycle.
    pub chunks_present: u64,
    pub uploaded: u64,
    pub already_present_walked: u64,
    pub bytes_uploaded: u64,
    /// Candidates whose presign/upload/confirm failed this cycle. Surfaced so a
    /// "10% of PUTs fail every cycle" incident is visible in status and logs.
    pub failed: u64,
    /// First error text from this cycle's PUT fan-out, if any candidate failed.
    ///
    /// Carried because `failed` alone says a cycle lost work but not why, and
    /// the drain deliberately does not abort on the first error. Without the
    /// text, a cycle in which *every* attempted PUT was rejected has no way to
    /// populate `last_error`, and the operator sees a failure count with no
    /// cause beside it.
    pub failure_error: Option<String>,
    /// The failed PUT fan-out contained the typed cloud quota rejection.
    ///
    /// Kept separate from `failure_error` so recovery bookkeeping does not
    /// depend on parsing provider text. A later clean PUT cycle may clear this
    /// specific drain failure without laundering an unrelated CAS failure.
    pub quota_exceeded: bool,
    /// Candidates of this cut whose local sealed file is gone. Non-zero means
    /// the cut can never reach 100% present, so no amount of further draining
    /// makes the home restorable at this generation.
    pub source_missing: u64,
    /// Manifest-only chunks of this cut with no local candidate, read off the
    /// held `BackupPublishTarget`. Maintained at cut time and after every
    /// unbackable retirement, so a cycle that abandons its cut still reports
    /// the count that made it terminal.
    pub unbackable_manifest_chunks: u64,
    /// Sum of `chunk.bytes` for candidates not yet present (if known).
    pub bytes_remaining: Option<u64>,
    pub cycle_duration: Duration,
    /// Of `cycle_duration`, the wall time spent moving bytes (the PUT fan-out).
    /// The remainder is per-cycle overhead: the walk over already-present
    /// candidates, which recurs every cycle and does not shrink with the
    /// backlog. The ETA needs these separated — see `snapshot()`.
    pub transfer_secs: f64,
    pub cas_counter: Option<u64>,
    pub phase: &'static str,
    /// `manifest.counter` of the cut this cycle drained. A change here is a
    /// legitimate new denominator (a fresh cut), so the gained/erased ledger
    /// resets; a `chunks_present` drop WITHOUT a change is churn erasing work.
    pub target_generation: Option<u64>,
}

/// Mutable tracker retained by the sync engine.
#[derive(Debug, Default)]
pub struct BackupProgressTracker {
    enabled: bool,
    session_started: Option<Instant>,
    session_started_unix: Option<u64>,
    ewma_bps: Option<f64>,
    /// EWMA of the true link rate: bytes uploaded per second *of transfer*.
    /// Distinct from `ewma_bps`, which is per second of whole cycle.
    ewma_link_bps: Option<f64>,
    /// EWMA of per-cycle overhead (cycle wall time minus transfer wall time) —
    /// the walk over already-present candidates. Paid once per cycle regardless
    /// of how many bytes the cycle moves.
    ewma_overhead_secs: Option<f64>,
    last: Option<LastCycle>,
    last_success_unix: Option<u64>,
    consecutive_failures: u32,
    last_error: Option<String>,
    /// The visible failure streak was most recently raised by a typed quota
    /// rejection in the drain. A clean PUT cycle proves that condition ended.
    quota_failure_active: bool,
    /// Gained/erased ledger for the current cut, so the operator surface can
    /// show a run that is losing ground as losing ground.
    session_generation: Option<u64>,
    session_gained: u64,
    session_erased: u64,
    /// Per-cycle deltas for the most recent cycles, oldest first, capped at
    /// `NET_PROGRESS_WINDOW_CYCLES`. This is what `net_progressing` reads; the
    /// session totals above stay as the gross ledger an operator compares
    /// against, which is what card
    /// `lastdb-backup-progress-signal-honest-gross-uploaded-vs-erased` asked
    /// for and which remains correct as a lifetime figure.
    recent_deltas: VecDeque<CycleDelta>,
    /// Cycles observed since one last gained ground. Unbounded on purpose: the
    /// window saturates, this does not.
    cycles_since_net_gain: u64,
    /// Whether a full backup manifest has ever been CAS-committed for this
    /// home. Unlike the progress samples above, this fact survives a process
    /// restart via the LastStore high-water marker.
    has_published: bool,
    last_publish_unix: Option<u64>,
    /// Whether the sealed-home base was abandoned as unpublishable and no
    /// commit has landed since. Like `has_published`, this survives a process
    /// restart via the LastStore high-water marker — it has to, because the
    /// abandon produces no further progress samples to infer it from.
    sealed_base_abandoned: bool,
}

/// One cycle's movement in the present set. Exactly one side is non-zero — a
/// cycle either gained chunks or lost them, never both, because the sample
/// carries a single `chunks_present` reading.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
struct CycleDelta {
    gained: u64,
    erased: u64,
}

#[derive(Debug, Clone)]
struct LastCycle {
    chunks_total: u64,
    chunks_present: u64,
    uploaded: u64,
    already_present_walked: u64,
    bytes_uploaded: u64,
    failed: u64,
    source_missing: u64,
    unbackable_manifest_chunks: u64,
    bytes_remaining: Option<u64>,
    cas_counter: Option<u64>,
    phase: String,
    target_generation: Option<u64>,
}

impl BackupProgressTracker {
    pub fn new_enabled() -> Self {
        Self {
            enabled: true,
            ..Self::default()
        }
    }

    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
        if !enabled {
            self.clear_session();
            self.last = None;
            self.ewma_bps = None;
            self.last_success_unix = None;
            self.consecutive_failures = 0;
            self.last_error = None;
            self.quota_failure_active = false;
        }
    }

    /// Record that a cut actually landed in cloud (successful CAS).
    ///
    /// This is the only event that makes a home restorable, and it is
    /// deliberately separate from `observe()`: a drain cycle round-tripping is
    /// not durability, and reporting it as if it were is what kept a 10-day-old
    /// backup looking healthy.
    ///
    /// It is also the ONLY event that may clear the failure streak. A publish
    /// that round-trips is the single piece of evidence that the thing which
    /// was failing has stopped failing; a drain sample is evidence about
    /// chunk presence and nothing else. See `observe()`.
    pub fn observe_publish_landed(&mut self) {
        if !self.enabled {
            return;
        }
        self.has_published = true;
        self.last_publish_unix = Some(unix_secs());
        self.consecutive_failures = 0;
        self.last_error = None;
        self.quota_failure_active = false;
        // A landed cut is a publishable base by definition.
        self.sealed_base_abandoned = false;
    }

    /// Restore a previously committed full-manifest publish from LastStore's
    /// durable high-water marker.
    ///
    /// This deliberately accepts only manifest-commit evidence. Mutation-log
    /// segment confirmation is not a snapshot base and must never call this.
    #[cfg(any(feature = "cloud-sync", test))]
    pub(crate) fn restore_published_manifest(&mut self, committed_at_unix: Option<u64>) {
        if !self.enabled {
            return;
        }
        self.has_published = true;
        self.last_publish_unix = committed_at_unix;
        self.last_success_unix = committed_at_unix;
    }

    /// Restore an outstanding sealed-base abandon from the durable marker.
    ///
    /// Companion to [`Self::restore_published_manifest`], and the reason that
    /// call alone was not enough: the marker's commit timestamp is positive
    /// evidence only. A home whose held cut was later abandoned as
    /// unpublishable still carries an old, real commit, so a restarted process
    /// restored `has_published` and reported `complete: true` for a sealed base
    /// that can no longer be published. The abandon has to be restored beside
    /// it, or the degraded state does not survive a restart.
    #[cfg(any(feature = "cloud-sync", test))]
    pub(crate) fn restore_sealed_base_abandoned(&mut self, reason: String) {
        if !self.enabled {
            return;
        }
        self.sealed_base_abandoned = true;
        self.last_error = Some(reason);
    }

    /// Whether any full-manifest cut has ever landed in cloud (successful CAS),
    /// including a commit restored from the LastStore high-water marker.
    pub fn has_ever_published(&self) -> bool {
        self.has_published
    }

    /// Observe one publish cycle that failed outright (no sample produced).
    ///
    /// Without this, a uploader whose every cycle errors leaves `last == None`
    /// forever, and `snapshot()` reported `complete: true` — the silent
    /// durability gap: backup stalled for hours while status stayed green.
    pub fn observe_failure(&mut self, error: &str) {
        if !self.enabled {
            return;
        }
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        self.last_error = Some(error.to_string());
        self.quota_failure_active = false;
    }

    /// Observe one drain/publish cycle. Pure bookkeeping; never blocks I/O.
    ///
    /// A sample means the drain half of a cycle round-tripped. It deliberately
    /// does NOT clear `consecutive_failures` / `last_error`: the uploader takes
    /// its progress sample BEFORE the CAS publish it precedes, so treating a
    /// sample as proof of a healthy publish let every drain launder the CAS
    /// failure that followed it. Measured on the primary 2026-08-03: a backup
    /// whose CAS had failed every cycle for 18+ hours read `fails=0` in 23 of
    /// 24 samples, because each drain wiped the streak within one sample
    /// interval. Only `observe_publish_landed()` clears it.
    ///
    /// It does, however, *raise* the streak when the drain itself lost every
    /// unit of work it attempted — see the comment on `drain_lost_everything`.
    pub fn observe(&mut self, sample: BackupCycleSample) {
        if !self.enabled {
            return;
        }

        // A cycle that attempted PUTs and completed NONE of them is a failed
        // cycle, however many chunks a previous session had already landed.
        //
        // Measured on the primary 2026-08-05: the cloud account was 34 MB over
        // its 50 GiB quota, so every one of the 16 PUTs per cycle was rejected
        // HTTP 429 — ~8,000 rejections per 25 minutes, for hours. Status read
        // `phase=draining`, `consecutive_failures=0`, `last_error=null`,
        // `last_success_age_secs=9`. Every field an operator or monitor would
        // consult said healthy while 868 chunks / 2.85 GB of user data could
        // not leave the machine.
        //
        // The failure count was not missing — `failed` was carried into the
        // snapshot and published all along. It was simply consulted by nothing.
        // A quantity that no health derivation reads cannot raise an alarm.
        //
        // `uploaded == 0 && failed == 0` is NOT this case: that is the steady
        // state of a fully backed-up home, where every candidate is already
        // present and there is genuinely no work. Only an attempt that lost
        // every unit of its work counts against freshness.
        let drain_lost_everything = sample.uploaded == 0 && sample.failed > 0;

        // A cut carrying chunks with no local source is a *terminal* condition,
        // not a slow one: those chunks can never become present, and the cut
        // CASes only at 100%. It must therefore not be allowed to refresh cycle
        // freshness — otherwise the drain that skips them (correctly, so it can
        // reach the rest) round-trips cleanly forever and every field an
        // operator reads says healthy while the home stays unrestorable.
        //
        // This is the same laundering `drain_lost_everything` closed for failed
        // PUTs, arriving by the opposite route: there, the work failed; here,
        // the work was rightly not attempted at all.
        let cut_cannot_publish = sample.source_missing > 0;

        // Moved out, not cloned: `observe` is the sample's last stop, and every
        // remaining field it reads below is `Copy`.
        let failure_error = sample.failure_error;
        let quota_exceeded = sample.quota_exceeded;

        if drain_lost_everything || cut_cannot_publish {
            self.consecutive_failures = self.consecutive_failures.saturating_add(1);
            self.quota_failure_active = drain_lost_everything && quota_exceeded;
            if let Some(err) = failure_error {
                self.last_error = Some(err);
            } else if cut_cannot_publish {
                self.last_error = Some(format!(
                    "backup cut cannot publish: {} chunk(s) have no local sealed file \
                     (resealed away under the held cut); it must be re-cut",
                    sample.source_missing
                ));
            }
        } else {
            // The drain round-tripped, so cycle freshness is real. Durability
            // freshness is `last_publish_unix`, and restorability is neither.
            self.last_success_unix = Some(unix_secs());
            // A clean PUT is a fresh quota read plus a successful write. That
            // is direct evidence that the typed quota rejection which raised
            // the current failure streak no longer applies. Clear only this
            // drain-derived failure: ordinary drain samples must still never
            // launder a CAS failure recorded by `observe_failure()`.
            if self.quota_failure_active && sample.uploaded > 0 && sample.failed == 0 {
                self.consecutive_failures = 0;
                self.last_error = None;
                self.quota_failure_active = false;
            }
        }

        // Gained/erased ledger. A new cut legitimately resets the denominator;
        // within one cut, a falling `chunks_present` can only be reseal churn
        // invalidating bytes that were already uploaded and paid for.
        if self.session_generation != sample.target_generation {
            self.session_generation = sample.target_generation;
            self.session_gained = 0;
            self.session_erased = 0;
            self.recent_deltas.clear();
            self.cycles_since_net_gain = 0;
        } else if let Some(prev) = self.last.as_ref() {
            let delta = if sample.chunks_present >= prev.chunks_present {
                CycleDelta {
                    gained: sample.chunks_present - prev.chunks_present,
                    erased: 0,
                }
            } else {
                CycleDelta {
                    gained: 0,
                    erased: prev.chunks_present - sample.chunks_present,
                }
            };
            self.session_gained = self.session_gained.saturating_add(delta.gained);
            self.session_erased = self.session_erased.saturating_add(delta.erased);

            // A cycle that moved nothing is not a gain. Holding steady while
            // the link is dead looks identical to holding steady because
            // everything is already present, and only the second is healthy —
            // `complete` is what separates them, not this counter.
            if delta.gained > 0 {
                self.cycles_since_net_gain = 0;
            } else {
                self.cycles_since_net_gain = self.cycles_since_net_gain.saturating_add(1);
            }

            self.recent_deltas.push_back(delta);
            while self.recent_deltas.len() > NET_PROGRESS_WINDOW_CYCLES {
                self.recent_deltas.pop_front();
            }
        }

        // A cut naming manifest chunks with no local candidate is NOT complete,
        // however many candidates the drain got present. Every candidate can be
        // in cloud and the cut still never CAS, because the manifest references
        // shas the drain was never given a source for. Without this clause the
        // session clears here and the snapshot below reports `complete: true` on
        // a home that is provably unable to publish — measured on the primary
        // 2026-08-01, 08-05, 08-06 and 08-18, each time with the real count
        // available only inside `last_error`'s prose.
        //
        // Both terminal populations count, and they are disjoint: a candidate
        // whose frozen file vanished (`source_missing`) and a manifest-only ref
        // with no candidate at all (`unbackable_manifest_chunks`). Either alone
        // means this generation can never CAS.
        //
        // `source_missing` is folded in here rather than left alone because it
        // was only ever *masked*: in every observed case `chunks_present` was
        // short of `chunks_total`, so `complete` fell for the ordinary reason
        // and the terminal one was never tested. `cut_cannot_publish` above
        // already treats it as terminal for the failure streak — the two halves
        // of this file disagreed, and the reporting half was the wrong one.
        //
        // Deliberately NOT reusing `cut_cannot_publish`: that name carries
        // failure-streak and `last_error` semantics, and widening it would let
        // a drain sample clobber the more specific CAS abandon message. This is
        // a reporting verdict only.
        let cut_is_terminal = sample.source_missing > 0 || sample.unbackable_manifest_chunks > 0;
        let complete = !cut_is_terminal
            && (sample.chunks_total > 0 && sample.chunks_present >= sample.chunks_total
                || (sample.chunks_total == 0
                    && sample.uploaded == 0
                    && sample.already_present_walked == 0));

        // EWMA only when we actually pushed bytes this cycle.
        if sample.bytes_uploaded > 0 {
            let secs = sample.cycle_duration.as_secs_f64().max(0.001);
            let rate = sample.bytes_uploaded as f64 / secs;
            self.ewma_bps = Some(match self.ewma_bps {
                Some(prev) => EWMA_ALPHA * rate + (1.0 - EWMA_ALPHA) * prev,
                None => rate,
            });
            // The link rate, charged only for the time bytes were in flight.
            if sample.transfer_secs > 0.0 {
                let link = sample.bytes_uploaded as f64 / sample.transfer_secs;
                self.ewma_link_bps = Some(match self.ewma_link_bps {
                    Some(prev) => EWMA_ALPHA * link + (1.0 - EWMA_ALPHA) * prev,
                    None => link,
                });
            }
        }

        // Overhead is tracked on every cycle that produced a sample, including
        // ones that uploaded nothing: a cycle that walks the whole candidate
        // list and finds nothing to send still costs its walk, and that is
        // exactly the cost the count-bound half of the ETA has to charge.
        let overhead = (sample.cycle_duration.as_secs_f64() - sample.transfer_secs).max(0.0);
        self.ewma_overhead_secs = Some(match self.ewma_overhead_secs {
            Some(prev) => EWMA_ALPHA * overhead + (1.0 - EWMA_ALPHA) * prev,
            None => overhead,
        });

        if complete {
            self.clear_session();
        } else if sample.chunks_total > 0 && self.session_started.is_none() {
            self.session_started = Some(Instant::now());
            self.session_started_unix = Some(unix_secs());
        }

        self.last = Some(LastCycle {
            chunks_total: sample.chunks_total,
            chunks_present: sample.chunks_present,
            uploaded: sample.uploaded,
            already_present_walked: sample.already_present_walked,
            bytes_uploaded: sample.bytes_uploaded,
            failed: sample.failed,
            source_missing: sample.source_missing,
            unbackable_manifest_chunks: sample.unbackable_manifest_chunks,
            bytes_remaining: sample.bytes_remaining,
            cas_counter: sample.cas_counter,
            phase: sample.phase.to_string(),
            target_generation: sample.target_generation,
        });
    }

    pub fn snapshot(&self) -> BackupProgressSnapshot {
        if !self.enabled {
            return BackupProgressSnapshot {
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
            };
        }

        let Some(last) = &self.last else {
            // No cycle has produced a sample in this process. A durable
            // full-manifest commit still proves the home has a restore base;
            // without that proof this remains the loud "never completed"
            // state that caught the original silent durability gap.
            return BackupProgressSnapshot {
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
            };
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

    fn clear_session(&mut self) {
        self.session_started = None;
        self.session_started_unix = None;
    }

    /// Gained/erased summed over the recent window that decides
    /// `net_progressing`.
    fn recent_totals(&self) -> (u64, u64) {
        self.recent_deltas.iter().fold((0, 0), |(g, e), d| {
            (g.saturating_add(d.gained), e.saturating_add(d.erased))
        })
    }
}

/// Format a short human progress line for `lastdb status` (no trailing newline).
pub fn format_backup_progress_line(snap: &BackupProgressSnapshot) -> Option<String> {
    // An enabled uploader with a live failure streak is the case an operator
    // most needs to see, and it must be reported BEFORE the progress-bar path.
    // Note this fires even when the chunk bar reads complete: all sealed chunks
    // can be present in cloud while every CAS publish fails, and that home is
    // not restorable at the cut it thinks it is. Printing "100%" there is the
    // user-visible face of the streak-laundering bug.
    if snap.enabled && snap.consecutive_failures > 0 {
        let detail = snap
            .last_error
            .as_deref()
            .map_or_else(String::new, |e| format!(": {e}"));
        return Some(format!(
            "Backup: FAILING — {} consecutive failed cycle(s){detail}",
            snap.consecutive_failures
        ));
    }
    // An uploader that has never completed a cycle has no chunk numbers to
    // render either, so it also precedes the progress bar.
    if snap.enabled && !snap.complete && !snap.show_progress {
        return Some("Backup: no cycle has completed yet (nothing confirmed in cloud)".to_string());
    }
    if !snap.show_progress {
        return None;
    }
    let pct = snap
        .percent
        .map_or_else(|| "?%".into(), |p| format!("{p:.1}%"));
    let mut parts = vec![format!(
        "Recut: {pct} ({}/{} sealed chunks in cloud) — in-flight photograph; not restore base until CAS",
        snap.chunks_present, snap.chunks_total
    )];
    if let Some(elapsed) = snap.elapsed_secs {
        parts.push(format!("elapsed {}", format_duration_secs(elapsed)));
    }
    match snap.eta_secs {
        Some(eta) => parts.push(format!("ETA ~{}", format_duration_secs(eta))),
        // "calculating…" is the right words for a drain that has not yet earned
        // an estimate, and exactly the wrong words for one that is going
        // backwards — it reads as "working on it" while the bar falls. When the
        // recent window is net-negative, say which way it is moving instead.
        None if snap.recent_erased > snap.recent_gained => {
            parts.push(format!(
                "LOSING GROUND — {} erased vs {} gained over the last {} cycle(s), \
                 none gained in {}",
                snap.recent_erased,
                snap.recent_gained,
                snap.net_progress_window_cycles,
                if snap.cycles_since_net_gain == 1 {
                    "1 cycle".to_string()
                } else {
                    format!("{} cycles", snap.cycles_since_net_gain)
                }
            ));
        }
        None => parts.push("ETA calculating…".into()),
    }
    Some(parts.join(", "))
}

fn format_duration_secs(secs: u64) -> String {
    if secs < 60 {
        return format!("{secs}s");
    }
    let m = secs / 60;
    let s = secs % 60;
    if m < 60 {
        return format!("{m}m{s:02}s");
    }
    let h = m / 60;
    let m = m % 60;
    format!("{h}h{m:02}m")
}
