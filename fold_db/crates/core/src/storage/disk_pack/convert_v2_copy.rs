//! The copy and the check of a v1 to v2 disk format conversion: each stored
//! record of the v1 generation goes in its v2 form (`convert_v2_records`)
//! into the run home of its collection. After the reopen, each v2 form is
//! computed again and compared with the record that the home reads back.

use super::convert_v2::invalid;
use super::convert_v2_records::Transform;
use super::physical::CAS_COLLECTION;
use super::*;
use laststore::disk_pack::maintenance::visit_generation_streaming;
use laststore::disk_run::home::{RunHome, MAX_VALUE_BYTES};
use std::io::Read;

const BATCH_RECORDS: usize = 4096;
const BATCH_BYTES: usize = 16 * 1024 * 1024;
const VERIFY_RECORDS: usize = 1000;
/// A stored v1 value with an outer envelope is larger than its v2 form, so
/// the read accepts more than a v2 value can hold.
const READ_LIMIT: u64 = 2 * MAX_VALUE_BYTES as u64;

type Pending = Vec<((String, String), Vec<u8>)>;

/// Copy each stored record of `current` in its v2 form, then the added
/// records. Tombstones are not copied: an absent key in a new home reads the
/// same. A record that the v1 node reads as absent is not copied either, and
/// `Transform::extras` then refuses the home. Each home merges after each
/// batch, so the copy never holds most of a home in unmerged runs. Returns
/// the records and their bytes.
pub(super) fn copy(
    source: &DiskHome,
    current: &Current,
    homes: &[RunHome; 2],
    transform: &mut Transform<'_>,
) -> StorageResult<(u64, u64)> {
    let mut batches: [Vec<HomeMutation>; 2] = Default::default();
    let mut batch_bytes = [0usize; 2];
    let (mut records, mut logical) = (0u64, 0u64);
    let mut failure = None;
    let visited = visit_generation_streaming(source.path(), current, |record| {
        let Some(body) = record.body else {
            return Ok(());
        };
        let stored = read_body(body, record.body_bytes)?;
        if transform.replaced(record.collection, record.key) {
            return Ok(());
        }
        let Some(value) = transform
            .value(record.collection, record.key, stored)
            .map_err(|error| stop(&mut failure, error))?
        else {
            return Ok(());
        };
        let value = fits(record.collection, value).map_err(|error| stop(&mut failure, error))?;
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
            commit(&homes[slot], std::mem::take(&mut batches[slot]))?;
            batch_bytes[slot] = 0;
        }
        Ok(())
    });
    if let Some(error) = failure {
        return Err(error);
    }
    visited.map_err(map)?;
    let extras = transform.extras()?;
    for extra in &extras {
        let body = extra.body.as_ref().map_or(0, Vec::len);
        logical += (extra.collection.len() + extra.key.len() + body) as u64;
    }
    records += extras.len() as u64;
    batches[0].extend(extras);
    for (slot, batch) in batches.into_iter().enumerate() {
        if !batch.is_empty() {
            commit(&homes[slot], batch).map_err(map)?;
        }
    }
    Ok((records, logical))
}

/// Read every v1 record again, compute its v2 form, and compare it with the
/// run homes in batches; then check the added records.
pub(super) fn verify(
    source: &DiskHome,
    current: &Current,
    homes: &[RunHome; 2],
    transform: &mut Transform<'_>,
) -> StorageResult<u64> {
    let mut pending: [Pending; 2] = Default::default();
    let mut pending_bytes = [0usize; 2];
    let mut verified = 0u64;
    let mut failure = None;
    let visited = visit_generation_streaming(source.path(), current, |record| {
        let Some(body) = record.body else {
            return Ok(());
        };
        let stored = read_body(body, record.body_bytes)?;
        if transform.replaced(record.collection, record.key) {
            return Ok(());
        }
        let Some(value) = transform
            .value(record.collection, record.key, stored)
            .map_err(|error| stop(&mut failure, error))?
        else {
            return Ok(());
        };
        let slot = slot(record.collection);
        pending_bytes[slot] += value.len();
        pending[slot].push(((record.collection.into(), record.key.into()), value));
        if pending[slot].len() >= VERIFY_RECORDS || pending_bytes[slot] >= BATCH_BYTES {
            verified += check(&homes[slot], std::mem::take(&mut pending[slot]))
                .map_err(|error| stop(&mut failure, error))?;
            pending_bytes[slot] = 0;
        }
        Ok(())
    });
    if let Some(error) = failure {
        return Err(error);
    }
    visited.map_err(map)?;
    for (slot, rest) in pending.into_iter().enumerate() {
        if !rest.is_empty() {
            verified += check(&homes[slot], rest)?;
        }
    }
    let extras = transform.extras()?;
    let coordinates: Vec<(String, String)> = extras
        .iter()
        .map(|extra| (extra.collection.clone(), extra.key.clone()))
        .collect();
    let found = homes[0]
        .read(unlimited())
        .get_many(&coordinates)
        .map_err(map)?;
    for (extra, found) in extras.iter().zip(found) {
        let found = found.map_err(map)?.ok_or_else(|| {
            invalid("disk format conversion: an added record is missing after reopen")
        })?;
        transform.check_extra(extra, &found)?;
        verified += 1;
    }
    Ok(verified)
}

/// Commit one batch, then merge the home until it needs no merge.
fn commit(home: &RunHome, batch: Vec<HomeMutation>) -> laststore::Result<()> {
    RunHome::check_mutations(&batch)?;
    home.commit(batch).map(drop)?;
    while home.merge_step()?.is_some() {}
    Ok(())
}

/// Compare one batch of v2 forms with what a home reads back. A plain atom
/// row must also decode as the node decodes it.
fn check(home: &RunHome, pending: Pending) -> StorageResult<u64> {
    let (coordinates, expected): (Vec<_>, Vec<_>) = pending.into_iter().unzip();
    let found = home.read(unlimited()).get_many(&coordinates).map_err(map)?;
    for (((collection, key), expected), found) in coordinates.iter().zip(&expected).zip(found) {
        if found.map_err(map)?.as_deref() != Some(expected.as_slice()) {
            return Err(invalid(&format!(
                "disk format conversion: record differs after reopen in collection {collection}"
            )));
        }
        Transform::check_atom(collection, key, expected)?;
    }
    Ok(coordinates.len() as u64)
}

/// Refuse a v2 form that a run home cannot hold, with its size.
fn fits(collection: &str, value: Vec<u8>) -> StorageResult<Vec<u8>> {
    if value.len() > MAX_VALUE_BYTES {
        return Err(invalid(&format!(
            "disk format conversion: a value in collection {collection} has {} bytes; a v2 \
             home holds values up to {MAX_VALUE_BYTES} bytes",
            value.len()
        )));
    }
    Ok(value)
}

/// Keep the first error of the transform, and stop the visit.
fn stop(failure: &mut Option<StorageError>, error: StorageError) -> laststore::Error {
    *failure = Some(error);
    laststore::Error::Config("disk format conversion stopped".into())
}

/// The run home of a collection: 0 for the records, 1 for the CAS files.
fn slot(collection: &str) -> usize {
    usize::from(collection == CAS_COLLECTION)
}

fn read_body(body: &mut dyn Read, size: u64) -> laststore::Result<Vec<u8>> {
    if size > READ_LIMIT {
        return Err(laststore::Error::Config(
            "disk format conversion: a v1 value exceeds the conversion read limit".into(),
        ));
    }
    let mut value = Vec::with_capacity(size as usize);
    body.take(READ_LIMIT + 1).read_to_end(&mut value)?;
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
