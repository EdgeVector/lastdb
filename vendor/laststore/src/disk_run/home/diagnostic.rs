//! Explicit isolated-node checkpoints and merge-input cursors.
use super::manifest::{Manifest, RunFile};
use super::RunHome;
use crate::disk_run::reader_diagnostic::SummaryCursor;
use crate::{Error, Result};
use serde::Serialize;
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::sync::Arc;
use std::time::Instant;

pub use super::diagnostic_inventory::{CollectionInventory, LogicalInventory};

type InventoryRecord = (Vec<u8>, Option<u64>);

/// Allocated physical bytes at one captured checkpoint.
#[derive(Debug, Default, Serialize)]
pub struct FileDiagnostics {
    /// Allocated bytes of every regular file in the two native directories.
    pub total_allocated_bytes: u64,
}

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
    /// All published keys and stored values, before block compression.
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
    /// Consume and release the captured root, optionally building the logical inventory.
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
    /// Capture both homes before either inventory starts; they share one deadline.
    pub fn diagnostic_checkpoint(&self, deadline: Instant) -> Result<RunDiagnosticCheckpoint> {
        if !crate::diagnostics_enabled() {
            return Err(Error::Config(
                "storage diagnostics require an isolated timing capture".into(),
            ));
        }
        let (root, files) = inspect_files(self, deadline)?;
        let report = from_root(&root, files);
        Ok(RunDiagnosticCheckpoint { root, report })
    }
}

pub(in crate::disk_run::home) enum InventoryCursor<'a> {
    Run(SummaryCursor),
    Base {
        home: &'a RunHome,
        files: &'a [Arc<RunFile>],
        next: usize,
        current: Option<SummaryCursor>,
        allowance: u64,
    },
}

impl<'a> InventoryCursor<'a> {
    pub(in crate::disk_run::home) fn run(
        home: &RunHome,
        run: &RunFile,
        allowance: u64,
    ) -> Result<Self> {
        Ok(Self::Run(SummaryCursor::open(
            &run.path, &home.key, allowance,
        )?))
    }

    pub(in crate::disk_run::home) fn base(
        home: &'a RunHome,
        files: &'a [Arc<RunFile>],
        allowance: u64,
    ) -> Self {
        Self::Base {
            home,
            files,
            next: 0,
            current: None,
            allowance,
        }
    }

    pub(in crate::disk_run::home) fn next_record(
        &mut self,
        deadline: Instant,
    ) -> Result<Option<InventoryRecord>> {
        match self {
            Self::Run(cursor) => cursor.next_record(),
            Self::Base {
                home,
                files,
                next,
                current,
                allowance,
            } => loop {
                if Instant::now() >= deadline {
                    return Err(Error::ReadBudgetExceeded);
                }
                if let Some(cursor) = current {
                    if let Some(record) = cursor.next_record()? {
                        return Ok(Some(record));
                    }
                }
                *current = None;
                let Some(run) = files.get(*next) else {
                    return Ok(None);
                };
                *current = Some(SummaryCursor::open(&run.path, &home.key, *allowance)?);
                *next += 1;
            },
        }
    }
}

fn inspect_files(home: &RunHome, deadline: Instant) -> Result<(Arc<Manifest>, FileDiagnostics)> {
    let _commit = home.commit.lock().unwrap_or_else(|e| e.into_inner());
    let _merge = home.merge.lock().unwrap_or_else(|e| e.into_inner());
    home.refuse_if_poisoned()?;
    let root = home.held();
    let mut total_allocated_bytes = 0u64;
    for dir in [&home.root, &home.runs] {
        for entry in fs::read_dir(dir)? {
            if Instant::now() >= deadline {
                return Err(Error::ReadBudgetExceeded);
            }
            let metadata = fs::symlink_metadata(entry?.path())?;
            if metadata.is_file() {
                total_allocated_bytes =
                    total_allocated_bytes.saturating_add(metadata.blocks().saturating_mul(512));
            }
        }
    }
    Ok((
        root,
        FileDiagnostics {
            total_allocated_bytes,
        },
    ))
}

fn from_root(root: &Manifest, files: FileDiagnostics) -> RunHomeDiagnostics {
    let part = |runs: &[Arc<RunFile>]| RunPartDiagnostics {
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
