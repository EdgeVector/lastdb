//! Reverse conversion uses the latest complete native generation, not an old copy.

use super::conversion_fence as fence;
use super::home::DiskHome;
use super::lifetime::{DurableReference, ReferencePurpose};
use super::maintenance::visit_generation;
use crate::{durability, HashGroupKey, LastStore, LastStoreOptions, Result, TxnOp};
use std::fs::File;
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
    let token = fence::pending(destination)?;
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
    let value = serde_json::json!({"format":1,"destination":destination,
        "generation":current.generation,"frontier":current.frontier,"token":token.to_string(),
        "reference":reference.info().identity.to_string(),
        "layout_digest":fence::digest(&fence::bounded_file(&destination.join("laststore-layout-v1"), 4096)?)});
    fence::publish(home.path(), REVERSE_FENCE, &value)?;
    fence::finish(&destination, token)?;
    reference.release()?;
    Ok(ReverseReceipt {
        generation: current.generation,
        frontier: current.frontier,
        records,
        stored_bytes,
    })
}

/// Complete an interrupted cutover only after its durable source fence identifies
/// this exact pending target. A target without a source fence stays dormant.
/// No source data is opened as a database, overwritten, or deleted.
pub fn finish_conversion(source: &Path, destination: &Path) -> Result<()> {
    let prepared = fence::forward_value(source)?;
    let forward = prepared.is_some();
    let value = if let Some(value) = prepared {
        value
    } else {
        fence::read(source, REVERSE_FENCE)?
    };
    let destination = std::fs::canonicalize(destination)?;
    if value["format"] != 1
        || value["destination"].as_str().map(Path::new) != Some(destination.as_path())
    {
        return Err(crate::Error::Corrupt(
            "conversion source fence destination differs".into(),
        ));
    }
    let token = conversion_uuid(&value, "token")?;
    let reference = conversion_uuid(&value, "reference")?;
    if !destination.join(fence::PENDING).try_exists()? {
        if fence::bounded_file(&destination.join(fence::COMPLETE), 36)?
            != token.to_string().as_bytes()
        {
            return Err(crate::Error::Corrupt(
                "completed conversion token differs from source fence".into(),
            ));
        }
        return release_conversion_reference(
            if forward { &destination } else { source },
            reference,
        );
    }
    fence::validate(&destination, token)?;
    if forward {
        let _writer = super::write_lock::WriteLock::acquire_conversion(&destination, token)?;
        let raw = fence::bounded_file(&destination.join("CURRENT"), 4096)?;
        if raw.len() != 4096 || value["current_digest"] != fence::digest(&raw) {
            return Err(crate::Error::Corrupt(
                "pending native boundary differs from source fence".into(),
            ));
        }
        let fixed: &[u8; 4096] = raw
            .as_slice()
            .try_into()
            .map_err(|_| crate::Error::Corrupt("pending CURRENT length".into()))?;
        let current = super::Current::decode(fixed)?;
        super::home::HomeDescriptor::read(&destination)?.check_current(&current)?;
        let _inventory = super::lifetime::generation_inventory(&destination, &current)?;
        super::journal::JournalTail::read(&destination)?
            .validate_for_current(&destination, &current)?;
        if !source.join(fence::FORWARD_FENCE).try_exists()? {
            fence::publish(source, fence::FORWARD_FENCE, &value)?;
        }
        fence::finish(&destination, token)?;
        release_conversion_reference(&destination, reference)?;
    } else {
        let _target = LastStore::open_pending_conversion(&destination)?;
        fence::validate(&destination, token)?;
        let layout = fence::bounded_file(&destination.join("laststore-layout-v1"), 4096)?;
        if value["layout_digest"] != fence::digest(&layout) {
            return Err(crate::Error::Corrupt(
                "pending legacy layout differs from source fence".into(),
            ));
        }
        fence::finish(&destination, token)?;
        release_conversion_reference(source, reference)?;
    }
    Ok(())
}

fn conversion_uuid(value: &serde_json::Value, field: &str) -> Result<uuid::Uuid> {
    value[field]
        .as_str()
        .and_then(|value| uuid::Uuid::parse_str(value).ok())
        .filter(|identity| !identity.is_nil())
        .ok_or_else(|| crate::Error::Corrupt("conversion fence UUID".into()))
}

fn release_conversion_reference(root: &Path, reference: uuid::Uuid) -> Result<()> {
    match super::lifetime::release_reference(root, reference) {
        // A retry after the completed unlink still needs its directory barrier.
        Err(crate::Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            durability::sync_directory(&File::open(root.join("references"))?)
        }
        result => result,
    }
}
