//! The copy and the check of a v1 to v2 disk format conversion: each stored
//! record of the v1 generation goes into the run home of its collection, and
//! after the reopen each record is read back and compared.

use super::physical::CAS_COLLECTION;
use super::*;
use laststore::disk_pack::maintenance::visit_generation;
use laststore::disk_run::home::RunHome;
use std::io::Read;

const BATCH_RECORDS: usize = 4096;
const BATCH_BYTES: usize = 16 * 1024 * 1024;
const VERIFY_RECORDS: usize = 1000;

/// Copy each stored record of `current`. Tombstones are not copied: an
/// absent key in a new home reads the same.
pub(super) fn copy(
    source: &DiskHome,
    current: &Current,
    homes: &[RunHome; 2],
) -> StorageResult<(u64, u64)> {
    let mut batches: [Vec<HomeMutation>; 2] = Default::default();
    let mut batch_bytes = [0usize; 2];
    let mut records = 0u64;
    let mut logical = 0u64;
    let commit = |slot: usize, batch: Vec<HomeMutation>| -> laststore::Result<()> {
        RunHome::check_mutations(&batch)?;
        homes[slot].commit(batch).map(drop)
    };
    visit_generation(source.path(), current, |record| {
        let Some(body) = record.body else {
            return Ok(());
        };
        let value = read_body(body, record.body_bytes)?;
        let size = record.collection.len() + record.key.len() + value.len();
        let slot = slot(record.collection);
        batch_bytes[slot] += size;
        logical += size as u64;
        records += 1;
        batches[slot].push(HomeMutation {
            collection: record.collection.into(),
            key: record.key.into(),
            body: Some(value),
        });
        if batches[slot].len() >= BATCH_RECORDS || batch_bytes[slot] >= BATCH_BYTES {
            commit(slot, std::mem::take(&mut batches[slot]))?;
            batch_bytes[slot] = 0;
        }
        Ok(())
    })
    .map_err(map)?;
    for (slot, batch) in batches.into_iter().enumerate() {
        if !batch.is_empty() {
            commit(slot, batch).map_err(map)?;
        }
    }
    Ok((records, logical))
}

/// Read every v1 record again and compare it with the run homes in batches.
pub(super) fn verify(
    source: &DiskHome,
    current: &Current,
    homes: &[RunHome; 2],
) -> StorageResult<u64> {
    type Pending = Vec<((String, String), Vec<u8>)>;
    let mut pending: [Pending; 2] = Default::default();
    let mut pending_bytes = [0usize; 2];
    let mut verified = 0u64;
    let check = |slot: usize, pending: Pending| -> laststore::Result<u64> {
        let (coordinates, expected): (Vec<_>, Vec<_>) = pending.into_iter().unzip();
        let found = homes[slot].read(unlimited()).get_many(&coordinates)?;
        for ((coordinate, expected), found) in coordinates.iter().zip(&expected).zip(found) {
            if found?.as_deref() != Some(expected.as_slice()) {
                return Err(laststore::Error::Config(format!(
                    "disk format conversion: record differs after reopen in collection {}",
                    coordinate.0
                )));
            }
        }
        Ok(coordinates.len() as u64)
    };
    visit_generation(source.path(), current, |record| {
        let Some(body) = record.body else {
            return Ok(());
        };
        let value = read_body(body, record.body_bytes)?;
        let slot = slot(record.collection);
        pending_bytes[slot] += value.len();
        pending[slot].push(((record.collection.into(), record.key.into()), value));
        if pending[slot].len() >= VERIFY_RECORDS || pending_bytes[slot] >= BATCH_BYTES {
            verified += check(slot, std::mem::take(&mut pending[slot]))?;
            pending_bytes[slot] = 0;
        }
        Ok(())
    })
    .map_err(map)?;
    for (slot, rest) in pending.into_iter().enumerate() {
        if !rest.is_empty() {
            verified += check(slot, rest).map_err(map)?;
        }
    }
    Ok(verified)
}

/// The run home of a collection: 0 for the records, 1 for the CAS files.
fn slot(collection: &str) -> usize {
    usize::from(collection == CAS_COLLECTION)
}

fn read_body(body: &mut dyn Read, size: u64) -> laststore::Result<Vec<u8>> {
    let limit = laststore::disk_run::MAX_RUN_VALUE_BYTES as u64;
    if size > limit {
        return Err(laststore::Error::Config(
            "disk format conversion: a v1 value exceeds the run value limit".into(),
        ));
    }
    let mut value = Vec::with_capacity(size as usize);
    body.take(limit + 1).read_to_end(&mut value)?;
    if value.len() as u64 != size {
        return Err(laststore::Error::Config(
            "disk format conversion: a v1 body has a different size than its record".into(),
        ));
    }
    Ok(value)
}

fn unlimited() -> ReadBudget {
    ReadBudget {
        pages: u64::MAX,
        blocks: u64::MAX,
        bytes: u64::MAX,
        keys: u64::MAX,
    }
}
