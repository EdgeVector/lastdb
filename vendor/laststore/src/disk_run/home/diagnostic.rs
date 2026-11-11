//! Explicit isolated-node checkpoints, merge-input cursors, and newest-wins inventory.

use super::manifest::{Manifest, RunFile};
use super::RunHome;
use crate::disk_run::SummaryCursor;
use crate::{Error, Result};
use serde::Serialize;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::sync::Arc;
use std::time::Instant;

const MEMORY_BYTES: u64 = 64 * 1024 * 1024;
const MAX_RECORDS: u64 = 50_000_000;
const MAX_INPUTS: usize = 512;
type InventoryRecord = (Vec<u8>, Option<u64>);
type Head = Reverse<(Vec<u8>, usize, Option<u64>)>;

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

/// Merged current keys. Byte counts precede block compression and encryption.
#[derive(Debug, Default, Serialize)]
pub struct CollectionInventory {
    /// Distinct keys whose newest record contains a value.
    pub live_keys: u64,
    /// Complete storage keys, excluding the collection prefix in a run key.
    pub live_key_bytes: u64,
    /// Run keys include the collection-length and collection-name prefix.
    pub live_run_key_bytes: u64,
    /// Stored value bytes.
    pub live_stored_value_bytes: u64,
    /// Latest records that delete a key; older hidden versions are not counted.
    pub deleted_keys: u64,
    /// Storage-key bytes of newest tombstones, excluding the collection prefix.
    pub deleted_key_bytes: u64,
    /// Encoded run-key bytes of newest tombstones, including the collection prefix.
    pub deleted_run_key_bytes: u64,
}

/// A complete inventory, or an explicitly partial prefix after an audit cap.
#[derive(Debug, Serialize)]
pub struct LogicalInventory {
    /// Generation of the captured published root.
    pub generation: u64,
    /// Complete-commit frontier of the captured published root.
    pub frontier: u64,
    /// True only after every selected run record enters the merge.
    pub complete: bool,
    /// Fixed reason for a partial inventory, or none for a complete inventory.
    pub stopped_reason: Option<&'static str>,
    /// Fixed audit stage that refused admission; never a file name or record key.
    pub stopped_stage: Option<String>,
    /// Retained metadata and block allowance for each input cursor.
    pub cursor_allowance_bytes: u64,
    /// Simultaneous merge inputs; nonoverlapping base files share one cursor.
    pub input_cursors: usize,
    /// Shared working allowance for the one active decoder and its output.
    pub frame_workspace_bytes: u64,
    /// Elapsed inventory time in milliseconds from a monotonic clock.
    pub elapsed_ms: u64,
    /// Input run records examined, including hidden older versions.
    pub examined_run_records: u64,
    /// Distinct newest keys counted, including live keys and tombstones.
    pub merged_keys: u64,
    /// Maximum admitted temporary inventory memory in bytes.
    pub memory_limit_bytes: u64,
    /// Maximum number of input run records that the inventory examines.
    pub record_limit: u64,
    /// Whole seconds left in the shared checkpoint deadline at inventory start.
    pub time_limit_seconds: u64,
    /// Only fixed internal collection labels are emitted; other names share `other`.
    pub collections: BTreeMap<String, CollectionInventory>,
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
        with_inventory: bool,
        deadline: Instant,
    ) -> RunHomeDiagnostics {
        if with_inventory {
            self.report.inventory = Some(inventory(home, &self.root, deadline));
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
        let _commit = self.commit.lock().unwrap_or_else(|e| e.into_inner());
        let _merge = self.merge.lock().unwrap_or_else(|e| e.into_inner());
        self.refuse_if_poisoned()?;
        let root = self.held();
        let mut total_allocated_bytes = 0u64;
        for dir in [&self.root, &self.runs] {
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
        let part = |runs: &[Arc<RunFile>]| RunPartDiagnostics {
            runs: runs.len(),
            records: runs.iter().map(|r| r.meta.records).sum(),
            file_bytes: runs.iter().map(|r| r.meta.file_bytes).sum(),
        };
        let report = RunHomeDiagnostics {
            generation: root.generation,
            frontier: root.frontier,
            files: FileDiagnostics {
                total_allocated_bytes,
            },
            levels: root.levels.iter().map(|level| part(level)).collect(),
            merging: part(&root.merging),
            base: part(&root.base),
            published_record_bytes: root.newest_first().map(|r| r.meta.record_bytes).sum(),
            published_tombstones: root.newest_first().map(|r| r.meta.tombstones).sum(),
            published_tombstone_key_bytes: root
                .newest_first()
                .map(|r| r.meta.tombstone_bytes)
                .sum(),
            counted_tier_stale_bytes: root
                .tiered()
                .map(|r| r.hidden().map(|h| h.saturating_add(r.meta.tombstone_bytes)))
                .sum(),
            inventory: None,
        };
        Ok(RunDiagnosticCheckpoint { root, report })
    }
}

enum InventoryCursor<'a> {
    Run(SummaryCursor),
    Base {
        home: &'a RunHome,
        files: &'a [Arc<RunFile>],
        next: usize,
        current: Option<SummaryCursor>,
        allowance: u64,
    },
}

impl InventoryCursor<'_> {
    fn next_record(&mut self, deadline: Instant) -> Result<Option<InventoryRecord>> {
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
                let Some(run) = files.get(*next) else {
                    *current = None;
                    return Ok(None);
                };
                *current = Some(SummaryCursor::open(&run.path, &home.key, *allowance)?);
                *next += 1;
            },
        }
    }
}

fn inventory(home: &RunHome, root: &Manifest, deadline: Instant) -> LogicalInventory {
    let started = Instant::now();
    let mut report = LogicalInventory {
        complete: false,
        generation: root.generation,
        frontier: root.frontier,
        stopped_reason: None,
        stopped_stage: None,
        cursor_allowance_bytes: 0,
        input_cursors: 0,
        frame_workspace_bytes: SummaryCursor::frame_workspace_bytes(),
        elapsed_ms: 0,
        examined_run_records: 0,
        merged_keys: 0,
        memory_limit_bytes: MEMORY_BYTES,
        record_limit: MAX_RECORDS,
        time_limit_seconds: deadline.saturating_duration_since(started).as_secs(),
        collections: BTreeMap::new(),
    };
    match collect(home, root, &mut report, deadline) {
        Ok(()) => report.complete = true,
        Err(Error::ReadBudgetExceeded) => {
            report.stopped_reason = Some(if Instant::now() >= deadline {
                "shared_time_cap"
            } else {
                "memory_or_work_cap"
            })
        }
        Err(_) => report.stopped_reason = Some("record_read_failed"),
    }
    report.elapsed_ms = started.elapsed().as_millis().try_into().unwrap_or(u64::MAX);
    report
}

fn collect(
    home: &RunHome,
    root: &Manifest,
    report: &mut LogicalInventory,
    deadline: Instant,
) -> Result<()> {
    let count = root.tiered().count() + root.merging.len() + usize::from(!root.base.is_empty());
    report.input_cursors = count;
    if count == 0 {
        return Ok(());
    }
    if count > MAX_INPUTS {
        return Err(Error::ReadBudgetExceeded);
    }
    let desc = std::mem::size_of::<InventoryCursor<'_>>() + std::mem::size_of::<Head>();
    let metadata = root.retained_bytes().saturating_add((count * desc) as u64);
    let allowance = MEMORY_BYTES
        .checked_sub(report.frame_workspace_bytes)
        .and_then(|n| n.checked_sub(metadata))
        .ok_or(Error::ReadBudgetExceeded)?
        / count as u64;
    report.cursor_allowance_bytes = allowance;
    if allowance < 128 * 1024 {
        return Err(Error::ReadBudgetExceeded);
    }
    let (mut cursors, mut heads) = (Vec::with_capacity(count), BinaryHeap::with_capacity(count));
    for run in root.tiered().chain(&root.merging) {
        let mut cursor =
            InventoryCursor::Run(SummaryCursor::open(&run.path, &home.key, allowance)?);
        push_next(&mut cursor, cursors.len(), &mut heads, report, deadline)?;
        cursors.push(cursor);
    }
    if !root.base.is_empty() {
        let mut cursor = InventoryCursor::Base {
            home,
            files: &root.base,
            next: 0,
            current: None,
            allowance,
        };
        push_next(&mut cursor, cursors.len(), &mut heads, report, deadline)?;
        cursors.push(cursor);
    }
    while let Some(Reverse((key, slot, bytes))) = heads.pop() {
        add_record(report, &key, bytes)?;
        report.merged_keys += 1;
        while heads
            .peek()
            .is_some_and(|Reverse((next, _, _))| *next == key)
        {
            let Reverse((_, old, _)) = heads.pop().expect("equal head exists");
            push_next(&mut cursors[old], old, &mut heads, report, deadline)?;
        }
        push_next(&mut cursors[slot], slot, &mut heads, report, deadline)?;
    }
    Ok(())
}

fn push_next(
    cursor: &mut InventoryCursor<'_>,
    slot: usize,
    heads: &mut BinaryHeap<Head>,
    report: &mut LogicalInventory,
    deadline: Instant,
) -> Result<()> {
    if report.examined_run_records >= MAX_RECORDS || Instant::now() >= deadline {
        return Err(Error::ReadBudgetExceeded);
    }
    if let Some((key, bytes)) = cursor.next_record(deadline)? {
        report.examined_run_records += 1;
        heads.push(Reverse((key, slot, bytes)));
    }
    Ok(())
}

fn add_record(report: &mut LogicalInventory, key: &[u8], bytes: Option<u64>) -> Result<()> {
    let prefix = key
        .get(..2)
        .ok_or_else(|| Error::Corrupt("diagnostic collection prefix".into()))?;
    let length = u16::from_be_bytes([prefix[0], prefix[1]]) as usize;
    let name = key
        .get(2..2 + length)
        .ok_or_else(|| Error::Corrupt("diagnostic collection length".into()))?;
    const KNOWN: [&str; 7] = [
        "__native_home",
        "records",
        "metadata",
        "schema_states",
        "schemas",
        "change_feed",
        "cas_blobs",
    ];
    let label = KNOWN
        .into_iter()
        .find(|l| l.as_bytes() == name)
        .unwrap_or("other");
    let row = report.collections.entry(label.to_string()).or_default();
    let key_bytes = (key.len() - length - 2) as u64;
    if let Some(bytes) = bytes {
        row.live_keys += 1;
        row.live_key_bytes += key_bytes;
        row.live_run_key_bytes += key.len() as u64;
        row.live_stored_value_bytes += bytes;
    } else {
        row.deleted_keys += 1;
        row.deleted_key_bytes += key_bytes;
        row.deleted_run_key_bytes += key.len() as u64;
    }
    Ok(())
}
