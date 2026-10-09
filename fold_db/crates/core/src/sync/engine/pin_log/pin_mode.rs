use super::*;

impl PinLog {
    // Pin mode is a toolkit-only surface. It is deliberately unwired, not
    // pending work: `design-lastdb-cloud-sync-mutation-log-first` (approved
    // 2026-08-04) demoted freeze-as-sync. On the continuous snapshot+log
    // cadence the freeze buys nothing, because chunks are content-addressed
    // and immutable, S is a manifest rather than a copy, and publish is a
    // single CAS -- see `lastdb-snapshot-needs-no-freeze-immutable-chunks-plus-one-cas`.
    // So "zero production callers" below is the decision, not a gap; do not
    // wire a caller or delete the surface on that finding alone.
    //
    // What survives the demotion is the write-path guard: rewriting a member
    // of a frozen S while an upload is in flight must still fail closed. That
    // is the packing lock `design-lastdb-backup-photograph-freeze-in-place`
    // (Tom, 2026-08-19) needs, and `guard_sealed_base_write` below proves it.
    //
    // Keep these helpers out of production builds; the continuous mutation-log
    // methods below are the live product surface.

    /// Activate the already-linearized target snapshot used by one append.
    ///
    /// The caller holds `SyncEngine::target_config_lock`, so this helper must
    /// not ask the engine for a second snapshot and deadlock on the same lock.
    pub(crate) async fn ensure_continuous_mutation_log_for_targets(
        &self,
        targets: &[SyncTarget],
    ) -> Result<(), String> {
        let mut state = self.state.lock().await;
        for target in targets {
            let target_id = target_id_for_prefix(&target.prefix);
            let runtime = state.entry(target_id.clone()).or_insert_with(|| {
                PinLogRuntime::new(
                    target_id,
                    target.label.clone(),
                    target.prefix.clone(),
                    0,
                    0,
                    true,
                )
            });
            runtime.target_label = target.label.clone();
            runtime.target_prefix = target.prefix.clone();
            runtime.active = true;
            // Never upgrade continuous ensure into a freeze.
        }
        Ok(())
    }

    /// Whether continuous full-home sealed-chunk re-upload is demoted because
    /// the mutation-log plane is the active continuous durability engine.
    ///
    /// Snapshots remain allowed for bootstrap / rare compact only — never as
    /// the steady-state publish loop (design-lastdb-cloud-sync-mutation-log-first).
    ///
    /// Held incomplete cuts are the one continuous exception: the uploader still
    /// drains + CASes an already-held target so demotion cannot strand a mid-
    /// drain publish (see `backup_uploader` continuous loop).
    pub fn continuous_sealed_home_backup_demoted(&self) -> bool {
        matches!(self.config.capture_mode, CaptureMode::MutationLog)
            && !self.config.legacy_personal_cloud_sync
    }
}

// lint:file-size-ok moved verbatim from pin_log.rs; cohesive unit, split further in a later pass
