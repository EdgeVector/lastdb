//! Residual capture-free plane compaction.

use super::*;

impl PlaneCompactor {
    /// Reclaim every capture-free allowlisted plane that has no compactor of
    /// its own.
    ///
    /// # Why a sweep rather than a fourth bespoke compactor
    ///
    /// `sync_pin_log` and `sync_capture_reexport` each got a
    /// hand-written trigger, in that order, each time the plane had already
    /// grown past a gigabyte before anyone noticed. `schema_index`,
    /// `idempotency` and `change_feed` are the same shape — capture-skipped
    /// bookkeeping whose superseded records are appends that nothing prunes —
    /// and on 2026-08-17 they held 447 MiB between them with a byte-return verb
    /// nobody was calling. A list plus one loop arms all of them, and arms the
    /// next one for free: `RESIDUAL_SELF_COMPACT_PLANES` is what the policy bar
    /// checks a new allowlist entry against.
    ///
    /// # Why it is safe with cloud on
    ///
    /// Every plane in the sweep is capture-skipped, so the rewrite emits no
    /// mutation-log records and cannot repeat the 2026-08-08 `tips` compaction
    /// that turned a reclaim into 11.58 GiB of new pin log. That is asserted in
    /// `sync::policy` *and* re-checked here, per plane, immediately before the
    /// rewrite — a list can be edited, and the check that matters is the one at
    /// the call site.
    ///
    /// # Where this is called from
    ///
    /// Both the retired engine and the retired engine, for
    /// the reason recorded on
    /// the retired engine: the tick alone is
    /// gated behind `config.legacy_personal_cloud_sync`, false on the LastStore
    /// backup home every current node runs, and a reclaim reachable only from a
    /// retired code path is not a retention policy.
    pub(crate) async fn maybe_compact_residual_capture_free_planes(&self) {
        for plane in RESIDUAL_SELF_COMPACT_PLANES {
            self.maybe_compact_residual_plane(plane).await;
        }
    }
}
