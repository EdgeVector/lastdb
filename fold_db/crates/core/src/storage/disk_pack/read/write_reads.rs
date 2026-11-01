//! Exact disk results belong only to one private write's immutable root.
//!
//! The caller checks private and accepted writes first. Saved results contain
//! neither pending values nor decoded storage blocks. A later private put or
//! delete therefore shadows them without invalidation. Ordinary reads never
//! allocate this store, and no result survives the complete write preparation.

use super::*;
use std::collections::BTreeMap;

const MAX_BYTES: usize = 1024 * 1024;
const MAX_VALUE_BYTES: usize = 16 * 1024;
type Coordinate = (String, String);

#[derive(Default)]
struct Saved {
    values: BTreeMap<Coordinate, Option<Vec<u8>>>,
    bytes: usize,
    reused_records: u64,
    reused_bytes: u64,
}

pub(super) struct WriteReads {
    saved: Mutex<Saved>,
    started_us: u64,
}

impl Default for WriteReads {
    fn default() -> Self {
        Self {
            saved: Mutex::new(Saved::default()),
            started_us: laststore::commit_timing::unix_us(),
        }
    }
}

impl ReadContext {
    /// Outer None is a miss; Some(None) is a confirmed disk absence.
    pub(super) fn reuse_disk_value(
        &self,
        call: &mut PhysicalRead,
        coordinate: &Coordinate,
    ) -> StorageResult<Option<Option<Vec<u8>>>> {
        let Some(reads) = &self.write_reads else {
            return Ok(None);
        };
        let mut saved = reads.saved.lock().map_err(|_| poisoned())?;
        let Some(value) = saved.values.get(coordinate) else {
            return Ok(None);
        };
        let bytes = value.as_ref().map_or(0, Vec::len);
        // Reuse does not grant extra keys or body bytes. Charge before copying.
        self.account_memory_record(call, coordinate.0.len() + coordinate.1.len(), bytes)?;
        let value = value.clone();
        saved.reused_records += 1;
        saved.reused_bytes += bytes as u64;
        Ok(Some(value))
    }

    pub(super) fn remember_disk_value(
        &self,
        coordinate: &Coordinate,
        value: Option<&[u8]>,
    ) -> StorageResult<()> {
        let Some(reads) = &self.write_reads else {
            return Ok(());
        };
        let body_bytes = value.map_or(0, <[u8]>::len);
        if body_bytes > MAX_VALUE_BYTES {
            return Ok(());
        }
        let mut saved = reads.saved.lock().map_err(|_| poisoned())?;
        // Include both owned keys, the value and a conservative map-entry charge.
        let bytes = coordinate.0.len()
            + coordinate.1.len()
            + body_bytes
            + std::mem::size_of::<(Coordinate, Option<Vec<u8>>)>()
            + 8 * std::mem::size_of::<usize>();
        if bytes <= MAX_BYTES.saturating_sub(saved.bytes) && !saved.values.contains_key(coordinate)
        {
            saved
                .values
                .insert(coordinate.clone(), value.map(<[u8]>::to_vec));
            saved.bytes += bytes;
        }
        Ok(())
    }

    pub(super) fn report_write_reads(&self) {
        if !laststore::commit_timing::enabled() {
            return;
        }
        let Some(reads) = &self.write_reads else {
            return;
        };
        if let Ok(saved) = reads.saved.lock() {
            laststore::commit_timing::emit(
                "LASTDB_SNAPSHOT_TIMING",
                &serde_json::json!({
                    "kind": "write_reads", "started_us": reads.started_us,
                    "ended_us": laststore::commit_timing::unix_us(),
                    "counts": {"reused_records": saved.reused_records,
                        "reused_bytes": saved.reused_bytes, "saved_records": saved.values.len(),
                        "saved_bytes": saved.bytes},
                }),
            );
        }
    }
}

fn poisoned() -> StorageError {
    StorageError::BackendError("private write read results lock is poisoned".into())
}
