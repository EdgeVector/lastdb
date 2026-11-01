//! Checkpoint-only newest-wins inventory; no key or body leaves this module.
use super::diagnostic_cursor::InventoryCursor;
use super::manifest::Manifest;
use super::RunHome;
use crate::disk_run::reader::stream::SummaryCursor;
use crate::{Error, Result};
use serde::Serialize;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::time::Instant;

const MEMORY_BYTES: u64 = 64 * 1024 * 1024;
const MAX_RECORDS: u64 = 50_000_000;
const MAX_SECONDS: u64 = 60;
const MAX_INPUTS: usize = 512;
type Head = Reverse<(Vec<u8>, usize, Option<u64>)>;

/// Merged current keys. Byte counts precede block compression and encryption.
#[derive(Debug, Default, Serialize)]
pub struct CollectionInventory {
    /// Distinct keys whose newest record contains a value.
    pub live_keys: u64,
    /// Complete storage keys, excluding the collection prefix in a run key.
    pub live_key_bytes: u64,
    /// Run keys include the collection-length and collection-name prefix.
    pub live_run_key_bytes: u64,
    /// Tagged stored form, including its one-byte value tag; not expanded JSON.
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

impl LogicalInventory {
    fn new() -> Self {
        Self {
            complete: false,
            generation: 0,
            frontier: 0,
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
            time_limit_seconds: MAX_SECONDS,
            collections: BTreeMap::new(),
        }
    }
}

pub(super) fn inventory(home: &RunHome, root: &Manifest, deadline: Instant) -> LogicalInventory {
    let started = Instant::now();
    let mut report = LogicalInventory::new();
    report.generation = root.generation;
    report.frontier = root.frontier;
    report.time_limit_seconds = deadline.saturating_duration_since(started).as_secs();
    match collect(home, root, &mut report, deadline) {
        Ok(()) => report.complete = true,
        Err(Error::ReadBudgetExceeded) => {
            report.stopped_reason = Some(if Instant::now() >= deadline {
                "shared_time_cap"
            } else {
                "memory_or_work_cap"
            })
        }
        Err(Error::Config(message)) if message.starts_with("checkpoint audit cap: ") => {
            report.stopped_reason = Some("memory_or_work_cap");
            report.stopped_stage = message
                .strip_prefix("checkpoint audit cap: ")
                .map(str::to_string);
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
    let descriptor_bytes = std::mem::size_of::<InventoryCursor<'_>>() + std::mem::size_of::<Head>();
    let metadata = root
        .retained_bytes()
        .saturating_add((count * descriptor_bytes) as u64);
    let allowance = MEMORY_BYTES
        .checked_sub(report.frame_workspace_bytes)
        .ok_or(Error::ReadBudgetExceeded)?
        .checked_sub(metadata)
        .ok_or(Error::ReadBudgetExceeded)?
        / count as u64;
    report.cursor_allowance_bytes = allowance;
    if allowance < 128 * 1024 {
        return Err(Error::ReadBudgetExceeded);
    }
    let mut cursors = Vec::with_capacity(count);
    let mut heads = BinaryHeap::with_capacity(count);
    for run in root.tiered().chain(&root.merging) {
        check_work(report, deadline)?;
        let mut cursor = InventoryCursor::run(home, run, allowance)?;
        push_next(&mut cursor, cursors.len(), &mut heads, report, deadline)?;
        cursors.push(cursor);
    }
    if !root.base.is_empty() {
        let mut cursor = InventoryCursor::base(home, &root.base, allowance);
        push_next(&mut cursor, cursors.len(), &mut heads, report, deadline)?;
        cursors.push(cursor);
    }
    while let Some(Reverse((key, slot, bytes))) = heads.pop() {
        check_work(report, deadline)?;
        add_record(report, &key, bytes)?;
        report.merged_keys += 1;
        // Slot order is newest first, so the first equal key is authoritative.
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
    check_work(report, deadline)?;
    if let Some((key, bytes)) = cursor.next_record(deadline)? {
        report.examined_run_records += 1;
        heads.push(Reverse((key, slot, bytes)));
    }
    Ok(())
}

fn check_work(report: &LogicalInventory, deadline: Instant) -> Result<()> {
    if report.examined_run_records >= MAX_RECORDS || Instant::now() >= deadline {
        return Err(Error::ReadBudgetExceeded);
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
    let label = collection_label(name);
    if !report.collections.contains_key(label) {
        report
            .collections
            .insert(label.to_string(), CollectionInventory::default());
    }
    let row = report
        .collections
        .get_mut(label)
        .expect("collection inserted");
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

fn collection_label(name: &[u8]) -> &'static str {
    const KNOWN: &[&str] = &[
        "__native_home",
        "blob_counts",
        "records",
        "metadata",
        "schema_states",
        "schemas",
        "db_catalog",
        "field_keys",
        "public_keys",
        "idempotency",
        "schema_superseded_by",
        "change_feed",
        "attribution_ledger",
        "keep_small",
        "cas_blobs",
    ];
    KNOWN
        .iter()
        .copied()
        .find(|label| label.as_bytes() == name)
        .unwrap_or("other")
}
