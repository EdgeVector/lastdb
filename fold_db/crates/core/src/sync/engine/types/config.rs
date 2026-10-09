/// How local store state is reflected into the durable upload log.
///
/// Local writes never await this path. `Off` disables continuous capture
/// (snapshot / download / sealed-chunk backup still work). `MutationLog` is
/// the product continuous plane (design-lastdb-cloud-sync-mutation-log-first
/// Phase A): every staged commit group-commits into the durable mutation log
/// while Cloud Sync is on.
///
/// The former `Watermark` store-diff cold capture path is retired — write-path
/// CDC watermark is no longer a product mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CaptureMode {
    /// No store-level continuous capture (local-first only / tests that
    /// disable export). Snapshot and sealed-chunk backup remain available.
    #[default]
    Off,
    /// Continuous durable mutation-log capture while Cloud Sync is on.
    ///
    /// Reuses pin-mode durable group-commit log primitives without requiring
    /// pin freeze, sealed-base inventory, or a snapshot cycle. Single
    /// `writer_id` stream is an allowed Phase A scaffold.
    MutationLog,
}

/// Configuration for the sync engine.
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// Whether the legacy personal `{user_hash}/log/{seq}.enc` and
    /// `{user_hash}/snapshots/*` export path is allowed to write cloud objects.
    ///
    /// LastStore-backed Mini homes use the new manifest/chunk backup path. They
    /// may still bootstrap or replay old-format cloud data, but they must not
    /// write both legacy log/snapshot objects and the manifest format.
    pub legacy_personal_cloud_sync: bool,
    /// Store-level / continuous capture mode. See [`CaptureMode`].
    /// Production LastStore Mini homes use [`CaptureMode::MutationLog`].
    pub capture_mode: CaptureMode,
    /// How often to sync when dirty (milliseconds).
    pub sync_interval_ms: u64,
    /// Final entry-count BACKSTOP for compaction (snapshot + delete old logs).
    ///
    /// Compaction is normally driven by [`SyncConfig::compaction_log_ratio`] (a
    /// SIZE trigger) plus the time bounds below — not by entry count. This count
    /// remains only as a last-resort ceiling so a pathological stream of tiny
    /// entries can never let the cloud log grow without bound between size-based
    /// snapshots. Set it very high; the size/time policy should fire first in
    /// practice. (Historically this was `100`, which re-snapshotted ~every 2h /
    /// ~12× a day — far too aggressively now that bootstrap replays the log tail
    /// concurrently; see the `parallelize-bootstrap-log-replay` work.)
    pub compaction_threshold: u64,
    /// SIZE trigger: re-snapshot once the cloud log has accumulated at least
    /// this multiple of the last snapshot's (≈ the DB's) byte size since that
    /// snapshot.
    ///
    /// At a ratio of `1.0` we snapshot when the log since the last snapshot has
    /// grown to roughly the size of the DB itself: a fresh snapshot (≈ DB size)
    /// then replaces a ≥ DB-size log tail, which (a) halves future bootstrap
    /// work, (b) compacts dead/superseded history out of the log (the store has
    /// edits and deletes, not just appends), and (c) bounds cloud log storage.
    /// Lower it (e.g. `0.5`) to snapshot more eagerly, raise it to defer longer.
    /// For a node churning a few MiB/day this works out to roughly a monthly
    /// snapshot, versus ~12/day under the old entry-count trigger. A value
    /// `<= 0.0` disables the size trigger (time + backstop only).
    pub compaction_log_ratio: f64,
    /// Upper time bound: snapshot at least this often even if the log never
    /// crosses the size ratio. Keeps a near-idle node's single cloud snapshot
    /// from going stale (and lets a long-offline peer bootstrap from something
    /// recent). Defaults to 30 days. `0` disables the upper time bound.
    pub compaction_max_interval_secs: u64,
    /// Lower time bound: never snapshot more often than this, regardless of the
    /// size ratio. A one-time bulk import can momentarily blow past the size
    /// ratio; this floor stops that from triggering a flurry of full-DB
    /// snapshots in quick succession. Defaults to 24 hours. `0` disables the
    /// lower bound (the size ratio alone gates).
    pub compaction_min_interval_secs: u64,
    /// Number of historical `{seq}.enc` snapshots to retain *in addition to*
    /// the always-present `latest.enc` pointer.
    ///
    /// `0` (the default) is the leanest: only `latest.enc` is kept — a single
    /// full-DB copy — and the timestamped `{seq}.enc` is not even uploaded.
    /// This is safe because the DB is append-only and the local store is the
    /// source of truth: `latest.enc` embeds its own `last_seq`, so restore
    /// needs nothing else, and a bad cloud snapshot is simply overwritten by
    /// the next compaction from the intact local store. Set `N > 0` to also
    /// keep the `N` newest `{seq}.enc` as point-in-time history, at +1 full-DB
    /// copy (hundreds of MB) each. Without bounding this, snapshots accumulated
    /// unbounded — a prod node reached 114 snapshots / 24 GiB.
    pub snapshot_retention: usize,
    /// Device lock TTL in seconds.
    pub lock_ttl_secs: u64,
    /// Maximum retries for network operations.
    pub max_retries: u32,
    /// Maximum pending entries to hold in the in-memory upload worker queue.
    ///
    /// This bounds only the in-memory upload worker queue. Accepted local writes
    /// first persist a durable outbox record; when this queue is full, additional
    /// durable outbox entries wait locally for a later sync cycle instead of
    /// blocking unrelated local writes. `0` means unlimited.
    pub max_pending: usize,
    /// Durable local upload-staging depth at which the outbox is considered
    /// overflowing. This is a cloud backup catch-up bound, not a local write
    /// admission check; hitting it must never reject or drop local DB
    /// mutations. Instead, the sync cycle forces a personal snapshot backup
    /// and, only once that snapshot succeeds, clears the (now-redundant)
    /// staged entries — see [`SyncConfig::outbox_overflow_max_age_secs`] for
    /// the paired age-based trigger.
    /// `0` means unlimited (depth trigger disabled).
    pub max_outbox_entries: usize,
    /// Oldest durable outbox entry age (seconds) at which the outbox is
    /// considered overflowing, in addition to [`SyncConfig::max_outbox_entries`].
    /// Same cap→snapshot valve: a stale head (slow or stuck upload catch-up)
    /// forces a personal snapshot backup and clears staging on success,
    /// rather than blocking or dropping writes.
    /// `0` disables the age trigger.
    pub outbox_overflow_max_age_secs: u64,
    /// Consecutive failed sync cycles after which sync reports itself degraded.
    ///
    /// This is the *liveness* half of degradation, and the only half that can
    /// see a stall which keeps the durable outbox empty. Depth-based triggers
    /// ([`SyncConfig::max_outbox_entries`]) are blind to a cloud path that
    /// fails before anything is staged, which is how cloud backup once failed
    /// on every cycle for 36 h while `sync_degraded` stayed `false`.
    ///
    /// Counted only on real cycle errors and cleared by any successful sync,
    /// so an idle node that never needs to sync never trips it. `0` disables
    /// the liveness trigger.
    pub sync_failure_degraded_threshold: u64,
    /// Maximum number of in-flight S3 round-trips (PUT/GET) per sync cycle.
    ///
    /// A sync cycle's wall-clock cost is dominated by per-entry HTTPS
    /// round-trips: each log entry is one S3 PUT (upload) or GET (download)
    /// against a presigned URL. Done serially, a one-time backlog — e.g. the
    /// ~1,700 personal entries a fresh multi-schema load generates — can
    /// monopolise the cycle for minutes (5–12 min observed in dogfood) and
    /// delay everything queued behind it. Fanning the round-trips out with a
    /// bounded concurrency cap turns that into roughly `ceil(N / cap)` serial
    /// latencies; R2 handles concurrent objects fine. Replay stays strictly
    /// sequential in seq order, so the contiguous-cursor invariant is
    /// unaffected — only the network legs parallelise. 0 or 1 means fully
    /// serial (the prior behaviour).
    ///
    /// Keep this modest: each in-flight GET holds a full ciphertext buffer
    /// (and briefly the unsealed plaintext) in RAM. High concurrency on a
    /// multi-GB catch-up was a contributing factor in the 2026-07-14
    /// lastdbd memory balloon (0.5 GB -> 74 GB footprint).
    pub sync_concurrency: usize,
    /// Cap on how many *new* log seqs a single steady-state download cycle
    /// will attempt, after listing. Remaining seqs wait for the next tick.
    ///
    /// Without this, a large catch-up lists every seq after the cursor and
    /// walks the whole backlog in one `download_entries` call — keeping the
    /// engine in a long, memory-heavy download for minutes. `0` means
    /// unlimited (tests / explicit opt-out only).
    pub max_download_entries_per_cycle: usize,
    /// Soft budget on total ciphertext bytes downloaded in one steady-state
    /// download cycle. Once exceeded, further seqs are deferred to the next
    /// tick (cursor already advanced for completed entries). `0` = unlimited.
    pub max_download_bytes_per_cycle: usize,
    /// Hard per-object size cap for a single log GET. Objects larger than this
    /// are skipped (cursor advances past them with a loud warn) so a poison
    /// multi-GB blob cannot pin tens of GB of process memory. `0` = unlimited.
    pub max_download_entry_bytes: usize,
    /// Cap on how many durable-outbox entries a single steady-state upload
    /// cycle will attempt. Remaining entries stay in the outbox for later
    /// ticks.
    ///
    /// Without this, `do_sync` clones up to `max_pending` (default 10k)
    /// entries and seals them in large chunks — each sealed body is held in
    /// RAM before the S3 PUT. A multi-thousand pending backlog with fat
    /// BatchPuts (embeddings, etc.) can balloon process/swap even when
    /// download reports **0 new entries**. `0` means unlimited (tests /
    /// explicit opt-out only).
    pub max_upload_entries_per_cycle: usize,
    /// Max **mutation-log segments** sealed + published in one continuous
    /// cycle. `0` = unlimited (tests only).
    ///
    /// Deliberately separate from [`Self::max_upload_entries_per_cycle`]. That
    /// cap is a RAM guard sized for fat outbox `BatchPut` entries (embeddings,
    /// multi-MB bodies) and defaults to **8**. Log segments are ~2 KB, so
    /// reusing the entry cap throttled the continuous plane to 8 × ~2 KB per
    /// ~48 s cycle = ~331 B/s on a link measured at 4.6 MB/s — 0.007% of
    /// available bandwidth, and ~120× short of the node's own commit rate, so
    /// log lag grew ~1 s per second and never converged (primary, 2026-08-08).
    ///
    /// A cap sized for big objects must not be reused on small ones. Byte
    /// pressure is already bounded by `max_upload_bytes_per_cycle` and the
    /// adaptive upload policy; this is only the per-cycle object-count ceiling.
    pub max_log_segments_per_cycle: usize,
    /// Soft budget on total *plaintext serialized* log-entry bytes sealed
    /// for upload in one steady-state cycle. Once exceeded, further pending
    /// entries are deferred. `0` = unlimited.
    pub max_upload_bytes_per_cycle: usize,
    /// Maximum number of in-flight log-entry downloads during a one-time
    /// *bootstrap* (new-device restore-from-cloud), independent of
    /// `sync_concurrency`.
    ///
    /// Bootstrap replays the ENTIRE log tail after the latest snapshot in one
    /// shot — potentially hundreds of thousands of entries. Done serially
    /// (download → unseal → replay, one entry at a time) a ~1 GiB log
    /// (~210k entries at ~120 ms/entry on a reused R2 connection) takes ~7
    /// HOURS. A one-time large fetch benefits from more parallelism than the
    /// steady-state cycle, so this defaults higher than `sync_concurrency`.
    /// As with steady-state replay, only the network+crypto legs parallelise:
    /// entries are buffered in seq order and REPLAYED strictly sequentially, so
    /// the contiguous-cursor invariant (bootstrap aborts on the first
    /// missing/unsealable seq) is preserved. The in-flight set is bounded by
    /// this cap *and* [`Self::max_download_entry_bytes`]. 0 or 1 means fully
    /// serial (the prior behaviour). Keep this within R2/presign concurrency
    /// limits — and prefer lean defaults over "fast at any memory cost".
    pub bootstrap_concurrency: usize,
    /// Maximum number of non-personal targets restored concurrently during a
    /// multi-target bootstrap. Personal restore runs first because its snapshot
    /// is a full-store restore; org/share targets are scoped by prefix and can
    /// safely restore in parallel after that.
    pub bootstrap_target_concurrency: usize,
    /// Opt in to bootstrapping a **fresh** (empty) store even when the cloud
    /// prefix already holds log history but has NO snapshot (`latest.enc`).
    ///
    /// By default (`false`) bootstrap fails loudly in that situation
    /// ([`crate::sync::error::SyncError::MissingSnapshot`]) instead of replaying
    /// only the log tail into a silently near-empty store — the "missing
    /// snapshot" migration trap where a device restoring an existing identity
    /// cannot tell a brand-new account apart from one whose snapshot is missing
    /// or whose snapshot upload failed. A brand-new account (no snapshot AND an
    /// empty log prefix) always bootstraps clean regardless of this flag; it
    /// only governs the history-present-but-snapshot-absent case. Set it `true`
    /// (the `--accept-fresh` / `allow_empty` escape hatch) to intentionally
    /// start fresh despite detected history.
    pub accept_fresh_bootstrap: bool,
    /// How long intentional Cloud Sync **off** may keep staging local
    /// mutations before stop-staging policy takes over.
    ///
    /// Temporary pauses (network flap, operator pause under this grace) may
    /// still buffer mutations for cheap incremental re-enable. Past this
    /// window the engine must stop accumulating cloud mutations and re-enable
    /// via pull + snapshot reconverge instead of draining a multi-day outbox.
    /// See brain `design-lastdb-cloud-sync-off-stop-staging`.
    /// Default: 3600 seconds (1 hour). `0` means stop staging immediately on
    /// intentional off (no grace buffer).
    pub sync_off_grace_secs: u64,
    /// Age **in seconds** of the oldest writer's cloud-confirmed recovery
    /// point at which continuous-plane status reports `sync_degraded` with
    /// reason `mutation_log_lag`.
    ///
    /// This is the continuous health story for CaptureMode::MutationLog —
    /// not sealed-chunk remaining % (design-lastdb-cloud-sync-mutation-log-first).
    /// A recovery point younger than this is normal mid-cycle publishing; at
    /// or above it, publish is diverging from durable capture. The trigger
    /// only arms while `log_lag > 0`: with nothing unpublished there is no
    /// divergence to measure and the age grows on its own, so a caught-up
    /// idle node never reads degraded here regardless of this value.
    ///
    /// Compared against `MutationLogPlaneStatus::recovery_point_age_secs`, and
    /// **never** against `log_lag`. `log_lag` is the difference of two
    /// nanosecond frontier watermarks, so a seconds-scale trip point read
    /// against it fires at every nonzero lag and the flag carries no bits.
    /// That was the shipped defect: `true` from 5s of lag through 258s of lag,
    /// `false` only at the exact instant lag was 0. Keep this operand
    /// seconds-denominated; do not "fix" a future mismatch by scaling this
    /// number up to nanoseconds.
    ///
    /// `0` disables the lag degradation trigger (status still reports lag/F).
    pub mutation_log_lag_degraded_threshold_secs: u64,
    /// Remaining publish backlog, as a **frontier delta in nanoseconds**, at
    /// or above which an upload pass that made progress re-arms the publisher
    /// immediately instead of waiting for the next sync interval.
    ///
    /// This is a scheduling nudge, not a health signal. It consumes the same
    /// units as [`crate::sync::engine::pin_log::MutationLogUploadReport::upload_backlog_after`]
    /// (`last_durable_frontier - published_frontier`), so it must stay separate
    /// from [`Self::mutation_log_lag_degraded_threshold_secs`]: one field
    /// serving both a nanosecond frontier delta and a seconds age is precisely
    /// what made the degraded flag meaningless.
    /// `0` disables backlog-driven catch-up wakes.
    pub mutation_log_backlog_wake_threshold_ns: u64,
    /// Wall-time box for looping bounded mutation-log upload batches inside
    /// one `do_sync` while publish backlog remains.
    ///
    /// One cycle still honors [`Self::max_log_segments_per_cycle`] and the
    /// byte cap, then `published_f` advances only after every PUT of that
    /// batch. Further batches in the same pass reuse that order. `0` means
    /// one batch (the pre-catch-up shape). Default 400_000 ms.
    pub mutation_log_upload_catchup_budget_ms: u64,
    /// While mutation-log upload backlog is at or above
    /// [`Self::mutation_log_backlog_wake_threshold_ns`], skip the peer-apply
    /// cycle unless this many milliseconds have passed since the last attempt.
    ///
    /// Peer apply still uses the same listing, download, replay, and
    /// incorporated-F path when it runs. `0` means never skip. Default
    /// 500_000 ms, which must stay above
    /// [`Self::mutation_log_upload_catchup_budget_ms`]: the skip check runs
    /// after that upload, so a shorter interval never skips during drain.
    pub mutation_log_peer_apply_min_interval_ms: u64,
    /// Quiet window before a mutation-log upload cycle seals the records
    /// appended since the previous seal.
    ///
    /// A per-write wake used to seal one file per change. With this window
    /// the cycle waits so several changes of one kind share a file. `0`
    /// disables the quiet window. The product factory sets 1000 ms;
    /// [`Self::default`] stays 0 so unit tests upload on the wake.
    pub mutation_log_coalesce_quiet_ms: u64,
    /// Maximum time a mutation-log coalesce hold may wait for more changes.
    ///
    /// A stream that never goes quiet would never flush on
    /// [`Self::mutation_log_coalesce_quiet_ms`] alone. This cap forces a
    /// seal. `0` disables the cap. The product factory sets 5000 ms;
    /// [`Self::default`] stays 0. Both fields at 0 upload immediately.
    pub mutation_log_coalesce_max_ms: u64,
}

impl SyncConfig {
    /// Whether intentional Cloud Sync off has exceeded [`Self::sync_off_grace_secs`].
    ///
    /// `disabled_at_secs` is the Unix timestamp when sync was intentionally
    /// paused/disabled; `None` means sync is considered **on**.
    pub fn sync_off_grace_expired(&self, disabled_at_secs: Option<u64>, now_secs: u64) -> bool {
        let Some(disabled_at) = disabled_at_secs else {
            return false;
        };
        let age = now_secs.saturating_sub(disabled_at);
        age >= self.sync_off_grace_secs
    }

    /// Whether the engine should still record local changes into the cloud
    /// staging plane.
    ///
    /// Sync on → always recording. Intentional off within grace → still
    /// recording (temporary buffer). Past grace → not recording.
    pub fn recording_local_changes(&self, disabled_at_secs: Option<u64>, now_secs: u64) -> bool {
        match disabled_at_secs {
            None => true,
            Some(_) => !self.sync_off_grace_expired(disabled_at_secs, now_secs),
        }
    }

    /// Re-enable strategy when Cloud Sync is intentionally off.
    ///
    /// - `None` when sync is on
    /// - `"incremental"` within grace (drain short staging buffer)
    /// - `"snapshot_reconcile"` past grace (pull then snapshot; no multi-day log drain)
    pub fn reenable_strategy(
        &self,
        disabled_at_secs: Option<u64>,
        now_secs: u64,
    ) -> Option<&'static str> {
        disabled_at_secs?;
        if self.sync_off_grace_expired(disabled_at_secs, now_secs) {
            Some("snapshot_reconcile")
        } else {
            Some("incremental")
        }
    }
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            legacy_personal_cloud_sync: true,
            // Default Off so intermediate bottles never experiment on primary
            // multi-device export until explicitly enabled (tests/config).
            capture_mode: CaptureMode::Off,
            sync_interval_ms: 30_000,
            // Final backstop for both process-local and remote log counts.
            // The size + time policy should usually fire first, but the remote
            // count bound keeps restarts from letting `{prefix}/log/` grow
            // without limit.
            compaction_threshold: 10_000,
            compaction_log_ratio: 1.0,
            compaction_max_interval_secs: 30 * 24 * 60 * 60, // 30 days
            compaction_min_interval_secs: 24 * 60 * 60,      // 24 hours
            snapshot_retention: 0,
            lock_ttl_secs: 300,
            max_retries: 2,
            // Static defaults are **fixed-mode** / floor hints. In production
            // (`LASTDB_SYNC_UPLOAD_MODE=auto`, the default) the adaptive
            // upload policy (see `upload_policy.rs`) overrides per-cycle
            // `max_pending`, entry/byte caps, and PUT concurrency from live
            // RSS headroom vs the 6 GiB memory-guard, EWMA network, and CPU.
            // Keep these as safe fixed-mode fallbacks (and unit-test anchors),
            // not the live product rate limit.
            max_pending: 8,
            max_outbox_entries: 100_000,
            // Matches the previous hardcoded staging-escalation age (1h): a
            // stuck/slow upload catch-up should force a snapshot well before
            // it becomes an operator incident, but not so eagerly that a
            // brief network hiccup triggers a full-DB snapshot.
            outbox_overflow_max_age_secs: 60 * 60,
            // Three consecutive failures: past any single network blip or
            // presign expiry, but still minutes — not hours — before an
            // operator is told the cloud path is broken.
            sync_failure_degraded_threshold: 3,
            // Lean defaults after 2026-07-14 lastdbd footprint balloon
            // (download_entries stacked concurrent full-body GETs; outbox seed
            // and upload queue also thrash on re-enable). Primary Mini already
            // holds ~4 GiB steady-state (embeddings + DB); keep per-cycle work
            // small so the 6 GiB memory-guard circuit breaker is not tripped.
            // Adaptive mode raises concurrency when headroom allows.
            sync_concurrency: 2,
            max_download_entries_per_cycle: 64,
            max_download_bytes_per_cycle: 32 * 1024 * 1024, // 32 MiB
            max_download_entry_bytes: 16 * 1024 * 1024,     // 16 MiB
            // Fixed-mode floor; auto derives entry count from byte budget.
            max_upload_entries_per_cycle: 8,
            // Log segments are ~2 KB, not multi-MB outbox entries: 1024 of
            // them is ~2 MiB/cycle, inside the byte budget below, and ~128x
            // the throughput of the old borrowed cap of 8.
            max_log_segments_per_cycle: 1024,
            // Fixed-mode budget; auto uses up to LASTDB_SYNC_UPLOAD_BUDGET_MAX_MB
            // (default 256 MiB) from RSS headroom.
            max_upload_bytes_per_cycle: 8 * 1024 * 1024, // 8 MiB
            bootstrap_concurrency: 4,
            bootstrap_target_concurrency: 4,
            // Safe default: refuse to bootstrap a hollow store when the account
            // has history but its snapshot is missing. Callers that truly want a
            // fresh start opt in explicitly.
            accept_fresh_bootstrap: false,
            // Temporary pause buffer before stop-staging (Tom 2026-07-30).
            sync_off_grace_secs: 60 * 60,
            // Continuous mutation-log plane: tolerate a short in-flight
            // publish cycle, but flag when the cloud-confirmed recovery point
            // falls this many SECONDS behind. Read against
            // `recovery_point_age_secs`, not against the ns `log_lag` delta.
            mutation_log_lag_degraded_threshold_secs: 32,
            // Any remaining frontier-delta backlog re-arms the publisher after
            // a pass that made progress. 32ns is far below one write's
            // granularity, so this trips on any real backlog — the intended
            // catch-up pacing, unchanged by the seconds split above.
            mutation_log_backlog_wake_threshold_ns: 32,
            // Loop bounded upload batches for ~400 s while backlog remains
            // so a 13 h unpublished log is not one batch per 270 s
            // `do_sync` (primary, 2026-10-05). One measured batch is ~80 s
            // (~1024 records); 400 s is above four of those so a fifth
            // batch starts and one pass can publish more than 5000 records.
            mutation_log_upload_catchup_budget_ms: 400_000,
            // During that drain, skip peer apply unless this interval has
            // elapsed. Must exceed the catch-up budget: the skip check runs
            // after that upload, so 300 s let every draining pass still
            // run listing+download (~170 s even after fold#1453).
            mutation_log_peer_apply_min_interval_ms: 500_000,
            // Product factory sets the live hold (1 s quiet, 5 s cap).
            // Default 0 keeps tests and explicit configs on the immediate
            // upload path. Both 0 means "do not hold".
            mutation_log_coalesce_quiet_ms: 0,
            mutation_log_coalesce_max_ms: 0,
        }
    }
}
