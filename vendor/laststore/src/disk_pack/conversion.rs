//! Reverse conversion uses the latest complete native generation, not an old copy.

use super::home::DiskHome;
use super::lifetime::{DurableReference, ReferencePurpose};
use super::maintenance::visit_generation;
use crate::{durability, HashGroupKey, LastStore, LastStoreOptions, Result, TxnOp};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;

pub(super) const REVERSE_FENCE: &str = "laststore-legacy-source-sealed-v1";
const BATCH_BYTES: u64 = 64 * 1024 * 1024;

/// Result of a complete reverse conversion at one exclusive native boundary.
#[derive(Debug)]
pub struct ReverseReceipt {
    /// Complete native root selected under the writer lease.
    pub generation: u64,
    /// Last complete mutation batch copied to the new home.
    pub frontier: u64,
    /// Live records emitted to the new legacy home.
    pub records: u64,
    /// Opaque body bytes preserved without decryption.
    pub stored_bytes: u64,
}

/// Create a new legacy destination from the current native data, including writes
/// after the original conversion. Caller excludes other source processes/admins.
/// The exclusive journal owner refuses an active native node. A durable source
/// fence prevents a later native reopen from accepting writes in the old home.
pub fn reverse_to_legacy(home: &DiskHome, destination: &Path) -> Result<ReverseReceipt> {
    if destination.try_exists()? {
        return Err(crate::Error::Config(
            "reverse destination already exists".into(),
        ));
    }
    let writer = home.journal()?;
    let current = writer.current().clone();
    let reference = DurableReference::create(home.path(), &current, ReferencePurpose::Rollback, 0)?;
    let mut options = LastStoreOptions::hash_group();
    options.hash_group_key = HashGroupKey::PartitionPrefix;
    options.hash_group_partition_fanout = u32::from(home.descriptor().hash_fanout);
    options.layout_epoch = home.descriptor().layout_epoch;
    let target = LastStore::open_with(destination, options)?;
    let mut operations = Vec::new();
    let (mut bytes, mut records, mut stored_bytes) = (0u64, 0u64, 0u64);
    visit_generation(home.path(), &current, |record| {
        let Some(body) = record.body else {
            return Ok(());
        };
        if record.body_bytes > BATCH_BYTES {
            return Err(crate::Error::WriteCapacity(
                "reverse record exceeds 64 MiB".into(),
            ));
        }
        let cost =
            record.body_bytes + record.collection.len() as u64 + record.key.len() as u64 + 256;
        if !operations.is_empty() && (operations.len() == 4096 || bytes + cost > BATCH_BYTES) {
            target.transaction(std::mem::take(&mut operations))?;
            bytes = 0;
        }
        let mut value = Vec::new();
        value
            .try_reserve_exact(record.body_bytes as usize)
            .map_err(|_| super::resource())?;
        body.read_to_end(&mut value)?;
        operations.push(TxnOp::put(record.collection, record.key, value));
        bytes += cost;
        records += 1;
        stored_bytes += record.body_bytes;
        Ok(())
    })?;
    if !operations.is_empty() {
        target.transaction(operations)?;
    }
    target.flush()?;
    durability::sync_directory(&File::open(destination)?)?;
    let destination = std::fs::canonicalize(destination)?;
    let mut fence = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(home.path().join(REVERSE_FENCE))?;
    let bytes = serde_json::to_vec(&serde_json::json!({"format":1,"destination":destination,
        "generation":current.generation,"frontier":current.frontier}))
    .map_err(|error| crate::Error::Config(error.to_string()))?;
    fence.write_all(&bytes)?;
    durability::sync_dirty_file(&fence)?;
    durability::sync_directory(&File::open(home.path())?)?;
    reference.release()?;
    Ok(ReverseReceipt {
        generation: current.generation,
        frontier: current.frontier,
        records,
        stored_bytes,
    })
}
