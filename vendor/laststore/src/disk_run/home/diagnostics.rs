//! Isolated diagnostics and logical key-inventory counting for a published home root.

use super::{invalid, unpoison, RunHome};
use crate::disk_run::codec::corrupt;
use crate::disk_run::{MergeIter, RunRange, RunReader};
use crate::{Error, Result};
use serde::Serialize;
use std::ops::Bound;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

/// A complete newest-key inventory, or an explicitly partial prefix after an audit cap.
#[derive(Debug, Serialize)]
pub struct LogicalInventory {
    /// True only after every selected run record enters the merge.
    pub complete: bool,
    /// Distinct newest keys counted, including live keys and tombstones.
    pub merged_keys: u64,
}

/// Published root coordinates and optional logical key inventory for diagnostics.
#[derive(Debug, Serialize)]
pub struct RunHomeDiagnostics {
    /// Generation of the captured published root.
    pub generation: u64,
    /// Complete-commit frontier of the captured published root.
    pub frontier: u64,
    /// Optional newest-key inventory.
    pub inventory: Option<LogicalInventory>,
}

impl RunHome {
    /// Capture isolated storage diagnostics for the current published root.
    pub fn diagnostics(&self, with_inv: bool, deadline: Instant) -> Result<RunHomeDiagnostics> {
        if !crate::diagnostics_enabled() {
            return Err(invalid(
                "storage diagnostics require an isolated timing capture",
            ));
        }
        let root = {
            let (_commit, _merge) = (unpoison(&self.commit_mutex), unpoison(&self.merge_mutex));
            self.refuse_if_poisoned()?;
            self.held()
        };
        let inventory = with_inv.then(|| {
            let runs: Vec<_> = root
                .tiered()
                .chain(&root.merging)
                .map(|r| r.path.clone())
                .collect();
            let base = root.base.iter().map(|r| r.path.clone()).collect();
            let (complete, merged_keys) =
                count_merged_keys(&self.key, root.retained_bytes(), &runs, base, deadline);
            LogicalInventory {
                complete,
                merged_keys,
            }
        });
        Ok(RunHomeDiagnostics {
            generation: root.generation,
            frontier: root.frontier,
            inventory,
        })
    }
}

fn count_merged_keys(
    key: &[u8; 32],
    retained: u64,
    runs: &[PathBuf],
    base: Vec<PathBuf>,
    deadline: Instant,
) -> (bool, u64) {
    let mut merged = 0u64;
    let complete = (|| -> Result<()> {
        let count = runs.len() + usize::from(!base.is_empty());
        if count == 0 {
            return Ok(());
        }
        if count > 512 || retained > 56 << 20 {
            return Err(Error::ReadBudgetExceeded);
        }
        let allow = ((56 << 20) - retained) / count as u64;
        let mut sources: Vec<_> = runs
            .iter()
            .map(|p| Ok(Arc::new(RunReader::open_bounded(p, key, allow, 0)?).iter()))
            .collect::<Result<_>>()?;
        if !base.is_empty() {
            sources.push(RunRange::sequence(
                base,
                key,
                Bound::Unbounded,
                Bound::Unbounded,
            ));
        }
        for item in MergeIter::new(sources) {
            if merged >= 50_000_000 || Instant::now() >= deadline {
                return Err(Error::ReadBudgetExceeded);
            }
            let (rk, _) = item?;
            if rk
                .first_chunk::<2>()
                .is_none_or(|p| rk.len() < 2 + u16::from_be_bytes(*p) as usize)
            {
                return Err(corrupt("diagnostic prefix"));
            }
            merged += 1;
        }
        Ok(())
    })()
    .is_ok();
    (complete, merged)
}
