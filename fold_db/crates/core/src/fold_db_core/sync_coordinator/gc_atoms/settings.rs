//! Tunables for the automatic `gc-atoms` trigger.

use super::*;

/// Unreferenced-atom bytes tolerated before the automatic delete pass arms.
///
/// The primary accumulated 344 MiB of deletable bodies in under five hours
/// (measured 2026-08-18), so a 128 MiB cap arms inside a normal session. A cap
/// that only a multi-day uptime can reach is the defect this trigger fixes, not
/// a safety margin. The cap only decides when a pass starts. Every delete still
/// needs the grace window and the current lap's audit.
pub(crate) const DEFAULT_GC_ATOMS_MAX_UNREFERENCED_BYTES: u64 = 128 * 1024 * 1024;

/// Minimum spacing between automatic gc-atoms steps.
///
/// The local cadence owns this interval. The due-check still refuses a second
/// step inside one interval if a tick overlaps a slow previous step.
pub(crate) const DEFAULT_GC_ATOMS_STEP_INTERVAL_SECS: u64 = 30;

/// Wall-clock bound on one automatic gc-atoms step.
///
/// Each step advances the resumable probe or the bounded delete pass by whole
/// pages and then stops. The manual admin verb keeps its unbounded behavior.
/// The audit and reap share one budget of this size and the delete pass gets a
/// second one, so neither starves the other.
pub(crate) const DEFAULT_GC_ATOMS_STEP_BUDGET_SECS: u64 = 5;

/// Spacing between probe laps once a lap reaches its terminal result.
pub(crate) const DEFAULT_GC_ATOMS_RELAP_INTERVAL_SECS: u64 = 60 * 60;

/// Orphan candidates locked and revalidated in one automatic delete batch.
///
/// The batch holds a body lock and a count lock per candidate, so it stalls
/// every write whose atom hashes into those stripes. 64 of 256 stripes is the
/// most the default holds; the store clamps any override to 256.
pub(crate) const DEFAULT_GC_ATOMS_DELETE_CANDIDATE_CAP: usize = 64;

/// Zero-count candidates one step audits and one step reaps at most.
///
/// An audit is two durable rows, and a reaped atom is a locked, revalidated
/// delete. The cap keeps one step short; the next step takes the next ones.
pub(crate) const DEFAULT_GC_ATOMS_REAP_CANDIDATE_CAP: usize = 256;

/// Quiet time after boot before the cadence does any work.
///
/// A lap that finished before this process started is measured again at the
/// first step, which walks every plane. A daemon that restarts during a busy
/// boot window should not start that walk at once.
pub(crate) const DEFAULT_GC_ATOMS_START_DELAY_SECS: u64 = 10 * 60;

/// The approved grace window, in seconds.
pub(crate) const DEFAULT_GC_ATOMS_GRACE_WINDOW_SECS: u64 =
    crate::db_operations::DEFAULT_ATOM_GC_GRACE_WINDOW.as_secs();

/// Safety bound on bounded calls issued inside one step, so a state machine
/// that stops advancing cannot spin against the step deadline.
pub(crate) const GC_ATOMS_MAX_CALLS_PER_STEP: usize = 512;

/// Tunables for the automatic `gc-atoms` trigger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AutomaticGcAtomsSettings {
    pub(crate) max_unreferenced_bytes: u64,
    pub(crate) step_interval_secs: u64,
    pub(crate) step_budget_secs: u64,
    pub(crate) relap_interval_secs: u64,
    pub(crate) delete_candidate_cap: usize,
    pub(crate) reap_candidate_cap: usize,
    pub(crate) start_delay_secs: u64,
    pub(crate) grace_window_secs: u64,
    /// Emergency switch: the cap-armed pass deletes orphans with no grace
    /// window and no audit. Off unless `LASTDB_GC_ATOMS_BYPASS_GRACE` is set.
    pub(crate) bypass_grace: bool,
    /// Operator override of the peer-apply interlock. Off unless
    /// `LASTDB_GC_ATOMS_ALLOW_AFTER_PEER_APPLY` is set. With it on, automatic
    /// delete runs on a node that has applied a peer mutation (or cannot prove
    /// it never did), so the atoms of a lost multi-device write can be deleted.
    pub(crate) allow_after_peer_apply: bool,
}

impl Default for AutomaticGcAtomsSettings {
    fn default() -> Self {
        Self {
            max_unreferenced_bytes: DEFAULT_GC_ATOMS_MAX_UNREFERENCED_BYTES,
            step_interval_secs: DEFAULT_GC_ATOMS_STEP_INTERVAL_SECS,
            step_budget_secs: DEFAULT_GC_ATOMS_STEP_BUDGET_SECS,
            relap_interval_secs: DEFAULT_GC_ATOMS_RELAP_INTERVAL_SECS,
            delete_candidate_cap: DEFAULT_GC_ATOMS_DELETE_CANDIDATE_CAP,
            reap_candidate_cap: DEFAULT_GC_ATOMS_REAP_CANDIDATE_CAP,
            start_delay_secs: DEFAULT_GC_ATOMS_START_DELAY_SECS,
            grace_window_secs: DEFAULT_GC_ATOMS_GRACE_WINDOW_SECS,
            bypass_grace: false,
            allow_after_peer_apply: false,
        }
    }
}

impl AutomaticGcAtomsSettings {
    /// Read the env overrides once, at background-sync start.
    ///
    /// Naming matches the existing plane caps
    /// (`LASTDB_PIN_LOG_COMPACT_MAX_BYTES`,
    /// `LASTDB_ATOM_LOCATORS_COMPACT_MAX_BYTES`).
    pub(crate) fn from_env() -> Self {
        let defaults = Self::default();
        Self {
            max_unreferenced_bytes: gc_atoms_config_u64(
                "LASTDB_GC_ATOMS_MAX_UNREFERENCED_BYTES",
                defaults.max_unreferenced_bytes,
            ),
            step_interval_secs: gc_atoms_config_u64(
                "LASTDB_GC_ATOMS_STEP_INTERVAL_SECS",
                defaults.step_interval_secs,
            ),
            step_budget_secs: gc_atoms_config_u64(
                "LASTDB_GC_ATOMS_STEP_BUDGET_SECS",
                defaults.step_budget_secs,
            ),
            relap_interval_secs: gc_atoms_config_u64(
                "LASTDB_GC_ATOMS_RELAP_INTERVAL_SECS",
                defaults.relap_interval_secs,
            ),
            delete_candidate_cap: gc_atoms_config_u64(
                "LASTDB_GC_ATOMS_DELETE_CANDIDATE_CAP",
                defaults.delete_candidate_cap as u64,
            ) as usize,
            reap_candidate_cap: gc_atoms_config_u64(
                "LASTDB_GC_ATOMS_REAP_CANDIDATE_CAP",
                defaults.reap_candidate_cap as u64,
            )
            .max(1) as usize,
            start_delay_secs: gc_atoms_config_u64(
                "LASTDB_GC_ATOMS_START_DELAY_SECS",
                defaults.start_delay_secs,
            ),
            grace_window_secs: grace_window_secs_from_env(),
            bypass_grace: env_flag::var_truthy("LASTDB_GC_ATOMS_BYPASS_GRACE"),
            allow_after_peer_apply: crate::db_operations::allow_after_peer_apply_from_env(),
        }
    }

    /// The hard limit on the age of a completed lap that may authorize a delete.
    ///
    /// No progress waives it. A lap's reference markers describe the node while
    /// the lap ran; once the lap is older than the grace window, the same gap
    /// the grace window exists to cover has opened, so a long pass restarts the
    /// lap (a new audit) instead of finishing on old verdicts. Never shorter
    /// than the relap interval.
    pub(crate) fn max_lap_age_secs(&self) -> u64 {
        self.grace_window_secs.max(self.relap_interval_secs)
    }

    /// A zero byte cap turns the trigger off. The manual verb still works.
    pub(crate) fn enabled(&self) -> bool {
        self.max_unreferenced_bytes > 0
    }

    /// The per-atom gate for the cap-armed pass, or `None` when the operator
    /// asked for the ungated emergency pass.
    pub(crate) fn grace_gate(
        &self,
        lap_generation: u64,
        now: u64,
    ) -> Option<crate::db_operations::AutomaticGcAtomsGraceGate> {
        (!self.bypass_grace).then(|| crate::db_operations::AutomaticGcAtomsGraceGate {
            audit_id: crate::db_operations::automatic_gc_audit_id(lap_generation),
            now_unix_nanos: now.saturating_mul(1_000_000_000),
            grace_window: std::time::Duration::from_secs(self.grace_window_secs),
        })
    }
}

/// The grace window in force. Longer than the default is always allowed. A
/// shorter one is honored only with `LASTDB_GC_ATOMS_ALLOW_SHORT_GRACE`, which
/// exists so a synthetic home can age candidates in seconds. Never set it on a
/// home that holds data you need.
fn grace_window_secs_from_env() -> u64 {
    let requested = gc_atoms_config_u64(
        "LASTDB_GC_ATOMS_GRACE_WINDOW_SECS",
        DEFAULT_GC_ATOMS_GRACE_WINDOW_SECS,
    );
    if requested >= DEFAULT_GC_ATOMS_GRACE_WINDOW_SECS {
        return requested;
    }
    if env_flag::var_truthy("LASTDB_GC_ATOMS_ALLOW_SHORT_GRACE") {
        warn!(
            target: "fold_db::gc_atoms",
            grace_window_secs = requested,
            default_secs = DEFAULT_GC_ATOMS_GRACE_WINDOW_SECS,
            "automatic gc-atoms grace window is shorter than the approved default"
        );
        return requested;
    }
    warn!(
        target: "fold_db::gc_atoms",
        requested,
        using = DEFAULT_GC_ATOMS_GRACE_WINDOW_SECS,
        "LASTDB_GC_ATOMS_GRACE_WINDOW_SECS below the default is ignored without LASTDB_GC_ATOMS_ALLOW_SHORT_GRACE"
    );
    DEFAULT_GC_ATOMS_GRACE_WINDOW_SECS
}

pub(crate) fn gc_atoms_config_u64(name: &str, default: u64) -> u64 {
    env_flag::var_or(name, default)
}
