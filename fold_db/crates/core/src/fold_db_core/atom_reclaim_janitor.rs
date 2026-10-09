//! Bounded background janitor that reclaims unreferenced `atom:` bodies
//! after live Delete converge.
//!
//! ## Why
//!
//! Live `MutationType::Delete` (Skip) now converges tip absence to disk on
//! the persist lane instead of running the full atom-reachability sweep
//! inline (see [`super::purge::converge_delete_tips`],
//! `design-lastdb-delete-converge-then-reclaim`). Converge makes disk match
//! resident for that slot — the tip and its `tv:` chain are gone — but it
//! deliberately does not walk atom reverse edges or free atom bodies, so
//! that ACK never waits on a store-wide reachability scan. Something has to
//! do that second half, or every Delete leaks the atom bytes it stopped
//! reclaiming inline.
//!
//! This is that something: an opt-in periodic task, the same shape as
//! [`super::protein_reaper`] and [`super::tip_history_drain`], that runs an
//! ordered reclaim pass on a timer. The pass first waits for deferred-write
//! admission, then calls the existing `gc-atoms` verb
//! ([`crate::db_operations::AtomStore::gc_orphan_atoms_with_roots`]). That verb
//! checks live atom edges and target gates, prunes molecule-tip history through
//! its durable checkpoint, and writes its delete ledger. When that transition
//! completes, the pass runs local file-blob GC, which checks its own active
//! blob edges and target gates. A failed or incomplete stage blocks the later
//! stage. This module adds the ordered coordinator, not new reachability
//! logic.
//!
//! Cloud keep-set / backup-manifest shrink is a separate concern
//! ([`design-purged-atom-retirement-receipt`]) with its own automatic
//! cadence (`SyncCoordinator::start_automatic_gc_atoms`, cloud-sync only,
//! byte-budget gated, coupled to the backup publish lock). This janitor is
//! local-only and does not touch that lock or that cadence.
//!
//! ## Env
//!
//! | Variable | Meaning |
//! |----------|---------|
//! | `LASTDB_ATOM_RECLAIM_MS` | Interval for the background reclaim janitor. Unset or `0` disables it. |

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::db_operations::DbOperations;
use crate::schema::SchemaError;

use super::mutation_manager::MutationManager;

/// Env: background atom-reclaim janitor interval in milliseconds (`0` = off).
pub const ATOM_RECLAIM_MS_ENV: &str = "LASTDB_ATOM_RECLAIM_MS";

/// Suggested reclaim period — a full atom-plane-class scan, same order as
/// [`super::protein_reaper::DEFAULT_PROTEIN_REAPER_MS`]: hours, not
/// milliseconds.
pub const DEFAULT_ATOM_RECLAIM_MS: u64 = 21_600_000;

/// A reclaim pass skips rather than scan across an old deferred persist.
const ATOM_RECLAIM_PERSIST_CUT_TIMEOUT: Duration = Duration::from_secs(30);

/// The destructive stages of one reclaim pass.
///
/// The order is part of the safety contract. Atom and molecule-tip removal
/// must finish before local blob removal, because atom content can name a
/// shared local blob. Each stage has its own durable ledger and edge checks.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ReclaimStage {
    #[default]
    Admission,
    AtomsAndMoleculeTips,
    LocalBlobs,
    Complete,
}

impl ReclaimStage {
    fn next(self) -> Option<Self> {
        match self {
            Self::Admission => Some(Self::AtomsAndMoleculeTips),
            Self::AtomsAndMoleculeTips => Some(Self::LocalBlobs),
            Self::LocalBlobs => Some(Self::Complete),
            Self::Complete => None,
        }
    }
}

/// Results from one background reclaim pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReclaimPassReport {
    /// Last stage that this pass reached.
    pub stage: ReclaimStage,
    /// Atom bodies removed by the atom stage.
    pub atoms_deleted: u64,
    /// Molecule tip-version records removed by the atom stage.
    pub molecule_tip_versions_pruned: u64,
    /// Local file-blob rows removed after the atom stage.
    pub local_blobs_deleted: u64,
}

/// Run the ordered reclaim stages.
///
/// The persist cut is the admission gate for the full pass. Atom GC checks
/// atom edges and atom target gates. Its molecule-tip transition is ordered
/// and durable. Blob GC checks blob edges and blob target gates after the atom
/// transition completes. A failed stage stops the later stages.
async fn run_reclaim_pass(
    db_ops: &DbOperations,
    mutation_manager: &MutationManager,
    scan_started_at: chrono::DateTime<chrono::Utc>,
) -> Result<Option<ReclaimPassReport>, SchemaError> {
    let mut stage = ReclaimStage::Admission;
    if !mutation_manager
        .wait_for_atom_gc_persist_cut(ATOM_RECLAIM_PERSIST_CUT_TIMEOUT)
        .await
    {
        return Ok(None);
    }
    stage = stage
        .next()
        .expect("admission must lead to atom and molecule-tip reclaim");

    let extra_roots = mutation_manager.pending_pin_log_atom_roots().await?;
    let atoms = db_ops
        .atoms()
        .gc_orphan_atoms_with_roots_at_cut(false, None, false, &extra_roots, scan_started_at)
        .await?;

    // The atom pass can return while its bounded molecule-tip prologue still
    // has work. Do not advance to blob deletion until this transition settles.
    if atoms.prune_more_remaining {
        return Ok(Some(ReclaimPassReport {
            stage,
            atoms_deleted: atoms.atoms_deleted,
            molecule_tip_versions_pruned: atoms.tip_versions_pruned,
            local_blobs_deleted: 0,
        }));
    }

    stage = stage
        .next()
        .expect("atom and molecule-tip reclaim must lead to local blobs");
    let mut report = ReclaimPassReport {
        stage,
        atoms_deleted: atoms.atoms_deleted,
        molecule_tip_versions_pruned: atoms.tip_versions_pruned,
        local_blobs_deleted: 0,
    };

    #[cfg(feature = "sharing")]
    {
        let blobs = crate::db_operations::file_blob_gc::gc_orphan_file_blobs(db_ops, false).await?;
        report.local_blobs_deleted = blobs.file_blobs_deleted;
    }

    stage = stage
        .next()
        .expect("local blob reclaim must lead to complete");
    debug_assert_eq!(stage, ReclaimStage::Complete);
    report.stage = stage;
    Ok(Some(report))
}

/// Resolved atom-reclaim-janitor policy for this process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AtomReclaimJanitorPolicy {
    /// Background janitor period. `None` means no periodic reclaim.
    pub interval: Option<Duration>,
}

impl AtomReclaimJanitorPolicy {
    /// Parse from environment.
    pub fn from_env() -> Self {
        Self {
            interval: parse_atom_reclaim_interval(std::env::var(ATOM_RECLAIM_MS_ENV).ok()),
        }
    }
}

/// `LASTDB_ATOM_RECLAIM_MS` → interval; unset / empty / `0` → off.
#[must_use]
pub fn parse_atom_reclaim_interval(raw: Option<String>) -> Option<Duration> {
    let raw = raw?;
    let s = raw.trim();
    if s.is_empty() {
        return None;
    }
    match s.parse::<u64>() {
        Ok(0) => None,
        Ok(ms) => Some(Duration::from_millis(ms)),
        Err(_) => {
            tracing::warn!(
                raw = %s,
                env = ATOM_RECLAIM_MS_ENV,
                default_ms = DEFAULT_ATOM_RECLAIM_MS,
                "invalid LASTDB_ATOM_RECLAIM_MS; disabling background atom reclaim janitor"
            );
            None
        }
    }
}

/// Handle for the optional periodic atom-reclaim janitor task.
///
/// Aborted on [`super::fold_db::FoldDB::shutdown`] before the final
/// durability flush, and on Drop so tests do not leak tasks.
pub struct BackgroundAtomReclaimJanitorTask {
    join: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Set when we intentionally stop; the loop checks this so abort races
    /// do not log spurious janitor errors mid-shutdown.
    stop: Arc<AtomicBool>,
}

impl BackgroundAtomReclaimJanitorTask {
    /// No-op handle when the background janitor is disabled.
    pub fn disabled() -> Self {
        Self {
            join: Mutex::new(None),
            stop: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Spawn a periodic atom-reclaim pass on the current Tokio runtime.
    ///
    /// Live Delete ACK does not depend on this task: it reads the pin-log
    /// roots and calls `gc_orphan_atoms_with_roots` the same way the manual
    /// `gc-atoms` verb does, entirely off the request/persist-lane path.
    pub fn spawn(
        db_ops: Arc<DbOperations>,
        mutation_manager: Arc<MutationManager>,
        interval: Duration,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        // lint:spawn-bare-ok process-lifetime reclaim loop — not request-scoped.
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // Skip the immediate first tick so boot is not followed by a
            // pointless full atom scan before converge has produced anything
            // to reclaim.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                if stop_flag.load(Ordering::Relaxed) {
                    break;
                }
                let scan_started_at = chrono::Utc::now();
                match run_reclaim_pass(&db_ops, &mutation_manager, scan_started_at).await {
                    Ok(None) => {
                        if stop_flag.load(Ordering::Relaxed) {
                            break;
                        }
                        tracing::warn!(
                            target: "fold_node::database",
                            timeout_ms = ATOM_RECLAIM_PERSIST_CUT_TIMEOUT.as_millis() as u64,
                            "background reclaim skipped because the persistence admission cut did not drain"
                        );
                    }
                    Ok(Some(report))
                        if report.atoms_deleted > 0
                            || report.molecule_tip_versions_pruned > 0
                            || report.local_blobs_deleted > 0 =>
                    {
                        tracing::info!(
                            target: "fold_node::database",
                            atoms_deleted = report.atoms_deleted,
                            molecule_tip_versions_pruned = report.molecule_tip_versions_pruned,
                            local_blobs_deleted = report.local_blobs_deleted,
                            "background reclaim completed ordered atom, molecule-tip, and local-blob stages"
                        );
                    }
                    Ok(Some(_)) => {}
                    Err(e) => {
                        if stop_flag.load(Ordering::Relaxed) {
                            break;
                        }
                        tracing::warn!(
                            target: "fold_node::database",
                            error = %e,
                            "background reclaim stopped before all ordered stages completed"
                        );
                    }
                }
            }
        });
        Self {
            join: Mutex::new(Some(handle)),
            stop,
        }
    }

    /// Signal the loop to exit and abort the task (best-effort).
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self
            .join
            .lock()
            .expect("background atom reclaim janitor join lock")
            .take()
        {
            handle.abort();
        }
    }

    pub fn is_running(&self) -> bool {
        self.join
            .lock()
            .expect("background atom reclaim janitor join lock")
            .as_ref()
            .is_some_and(|h| !h.is_finished())
    }
}

impl Drop for BackgroundAtomReclaimJanitorTask {
    fn drop(&mut self) {
        self.stop();
    }
}
