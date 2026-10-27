//! Explicit isolated-node checkpoints. Default status and read paths do no audit work.
use super::{manifest::Manifest, RunHome};
use crate::{Error, Result};
use serde::Serialize;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub use super::diagnostic_files::FileDiagnostics;
pub use super::diagnostic_inventory::{CollectionInventory, LogicalInventory};

/// Numeric totals for the runs in one part of a captured published root.
#[derive(Debug, Serialize)]
pub struct RunPartDiagnostics {
    /// Number of run files in this part.
    pub runs: usize,
    /// Stored records, including tombstones and hidden older versions.
    pub records: u64,
    /// Sum of the recorded lengths of these run files.
    pub file_bytes: u64,
}

/// Physical files and optional logical keys of one captured published root.
#[derive(Debug, Serialize)]
pub struct RunHomeDiagnostics {
    /// Generation of the captured published root.
    pub generation: u64,
    /// Complete-commit frontier of the captured published root.
    pub frontier: u64,
    /// Physical file lengths and allocated bytes at the checkpoint.
    pub files: FileDiagnostics,
    /// Totals for each tiered level, with level zero first.
    pub levels: Vec<RunPartDiagnostics>,
    /// Runs selected for the current base merge.
    pub merging: RunPartDiagnostics,
    /// Oldest nonoverlapping base runs.
    pub base: RunPartDiagnostics,
    /// All published keys and tagged stored values, before block compression.
    pub published_record_bytes: u64,
    /// Tombstone records in all published runs, including hidden versions.
    pub published_tombstones: u64,
    /// Complete encoded run-key bytes of the published tombstone records.
    pub published_tombstone_key_bytes: u64,
    /// Tiered runs only; unknown while a new run awaits its hidden-byte count.
    pub counted_tier_stale_bytes: Option<u64>,
    /// Optional newest-key inventory; its completeness flag also applies.
    pub inventory: Option<LogicalInventory>,
}

/// Private captured root and numeric totals. Drop releases its immutable runs.
pub struct RunDiagnosticCheckpoint {
    root: Arc<Manifest>,
    report: RunHomeDiagnostics,
}

impl RunDiagnosticCheckpoint {
    /// The owner stops load before this audit. Only fixed-size aggregate output
    /// remains after this consumes and releases the captured root.
    pub fn finish(
        mut self,
        home: &RunHome,
        inventory: bool,
        deadline: Instant,
    ) -> RunHomeDiagnostics {
        if inventory {
            self.report.inventory = Some(super::diagnostic_inventory::inventory(
                home, &self.root, deadline,
            ));
        }
        self.report
    }
}

impl RunHome {
    /// Checkpoint only. The inventory holds this root, releases it before return,
    /// and has independent finite memory, work and wall-time caps.
    pub fn diagnostics(&self, include_inventory: bool) -> Result<RunHomeDiagnostics> {
        let deadline = Instant::now() + Duration::from_secs(60);
        Ok(self
            .diagnostic_checkpoint(deadline)?
            .finish(self, include_inventory, deadline))
    }

    /// Capture both homes before either inventory starts; they share one deadline.
    pub fn diagnostic_checkpoint(&self, deadline: Instant) -> Result<RunDiagnosticCheckpoint> {
        if !crate::commit_timing::enabled() {
            return Err(Error::Config(
                "storage diagnostics require an isolated timing capture".into(),
            ));
        }
        let (root, files) = super::diagnostic_files::inspect(self, deadline)?;
        let report = from_root(&root, files);
        Ok(RunDiagnosticCheckpoint { root, report })
    }
}

fn from_root(root: &Manifest, files: FileDiagnostics) -> RunHomeDiagnostics {
    let part = |runs: &[std::sync::Arc<super::manifest::RunFile>]| RunPartDiagnostics {
        runs: runs.len(),
        records: runs.iter().map(|run| run.meta.records).sum(),
        file_bytes: runs.iter().map(|run| run.meta.file_bytes).sum(),
    };
    RunHomeDiagnostics {
        generation: root.generation,
        frontier: root.frontier,
        files,
        levels: root.levels.iter().map(|level| part(level)).collect(),
        merging: part(&root.merging),
        base: part(&root.base),
        published_record_bytes: root.newest_first().map(|run| run.meta.record_bytes).sum(),
        published_tombstones: root.newest_first().map(|run| run.meta.tombstones).sum(),
        published_tombstone_key_bytes: root
            .newest_first()
            .map(|run| run.meta.tombstone_bytes)
            .sum(),
        counted_tier_stale_bytes: root
            .tiered()
            .map(|run| {
                run.hidden()
                    .map(|hidden| hidden.saturating_add(run.meta.tombstone_bytes))
            })
            .sum(),
        inventory: None,
    }
}
