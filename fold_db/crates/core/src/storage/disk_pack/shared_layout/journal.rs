//! Durable roll-forward for one write across the shared and device-private roots.

use super::{invalid, Lane, SharedHome};
use crate::storage::disk_pack::physical::V2Home;
use crate::storage::disk_pack::{map, HomeMutation, StorageResult};
use laststore::disk_run::home::RunHome;
use laststore::disk_run::{Found, RunOptions, RunReader, RunWriter};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::ErrorKind;
use std::os::unix::fs::DirBuilderExt;
use std::path::Path;

const STAGE: &str = ".shared-write-stage";
const PENDING: &str = ".shared-write";
const META: &[u8] = &[0];

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Intent {
    homes: [uuid::Uuid; 3],
    before: [u64; 3],
    counts: [u32; 3],
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Coordinate {
    collection: String,
    key: String,
    has_body: bool,
}

pub(in crate::storage::disk_pack) fn write(
    v2: &V2Home,
    shared: &SharedHome,
    operations: Vec<HomeMutation>,
) -> StorageResult<()> {
    let scope = super::Scope::with_writes(
        &shared.scope.read().expect("shared disk scope"),
        &operations,
    )?;
    let homes = [v2.home.as_ref(), v2.cas.as_ref(), shared.private.as_ref()];
    let mut batches = [Vec::new(), Vec::new(), Vec::new()];
    for op in operations {
        batches[scope.lane(&op.collection, &op.key) as usize].push(op);
    }
    let intent = Intent {
        homes: homes.map(RunHome::identity),
        before: homes.map(frontier),
        counts: std::array::from_fn(|lane| batches[lane].len() as u32),
    };
    if intent.counts.iter().filter(|count| **count != 0).count() > 1 {
        stage(&shared.private, &shared.journal_key, &intent, &batches)?;
        apply(&homes, &intent, batches)?;
        release(&shared.private)?;
    } else {
        apply(&homes, &intent, batches)?;
    }
    *shared.scope.write().expect("shared disk scope") = scope;
    Ok(())
}

pub(in crate::storage::disk_pack) fn recover(
    v2: &V2Home,
    private: &RunHome,
    key: &[u8; 32],
) -> StorageResult<()> {
    let pending = private.path().join(PENDING);
    if pending.try_exists()? {
        let reader = RunReader::open(&pending_run(&pending)?, key).map_err(map)?;
        let metadata = match reader.get(META).map_err(map)? {
            Some(Found::Value(bytes)) => bytes,
            _ => return Err(invalid("shared disk journal header is missing")),
        };
        let intent: Intent = decode(&metadata)?;
        let homes = [v2.home.as_ref(), v2.cas.as_ref(), private];
        let batches = read(&reader, &homes, &intent)?;
        apply(&homes, &intent, batches)?;
        drop(reader);
        release(private)?;
    }
    clear_stage(private)
}

fn pending_run(pending: &Path) -> StorageResult<std::path::PathBuf> {
    if !fs::symlink_metadata(pending)?.is_dir() {
        return Err(invalid("shared disk journal is not a directory"));
    }
    let mut entries = fs::read_dir(pending)?;
    let entry = entries
        .next()
        .transpose()?
        .ok_or_else(|| invalid("shared disk journal run is missing"))?;
    if entries.next().is_some() || !entry.file_type()?.is_file() {
        return Err(invalid("shared disk journal contains unexpected files"));
    }
    Ok(entry.path())
}

fn stage(
    private: &RunHome,
    key: &[u8; 32],
    intent: &Intent,
    batches: &[Vec<HomeMutation>; 3],
) -> StorageResult<()> {
    let path = private.path().join(STAGE);
    let pending = private.path().join(PENDING);
    if pending.try_exists()? {
        return Err(invalid("a shared disk journal needs recovery"));
    }
    fs::DirBuilder::new().mode(0o700).create(&path)?;
    let mut writer = RunWriter::create(&path, key, RunOptions::default()).map_err(map)?;
    writer.push(META, Some(&encode(intent)?)).map_err(map)?;
    for (lane, batch) in batches.iter().enumerate() {
        for (slot, op) in batch.iter().enumerate() {
            let coordinate = Coordinate {
                collection: op.collection.clone(),
                key: op.key.clone(),
                has_body: op.body.is_some(),
            };
            writer
                .push(&position(lane, slot, false), Some(&encode(&coordinate)?))
                .map_err(map)?;
            if let Some(body) = &op.body {
                writer
                    .push(&position(lane, slot, true), Some(body))
                    .map_err(map)?;
            }
        }
    }
    writer.finish().map_err(map)?;
    fs::rename(&path, &pending)?;
    File::open(private.path())?.sync_all()?;
    Ok(())
}

fn read(
    reader: &RunReader,
    homes: &[&RunHome; 3],
    intent: &Intent,
) -> StorageResult<[Vec<HomeMutation>; 3]> {
    let count: u64 = intent.counts.iter().map(|count| u64::from(*count)).sum();
    if count == 0
        || count > super::super::physical::V2_BATCH_OPERATIONS as u64
        || reader.records() < count + 1
        || reader.records() > 2 * count + 1
        || (*homes).map(RunHome::identity) != intent.homes
    {
        return Err(invalid("shared disk journal identity or operation bound"));
    }
    let mut entries = reader.iter();
    next(&mut entries, META)?;
    let mut batches = [Vec::new(), Vec::new(), Vec::new()];
    let mut bytes = 0u64;
    for (lane, batch) in batches.iter_mut().enumerate() {
        let home = homes[lane];
        let after = expected_after(intent, lane)?;
        let current = frontier(home);
        if current != intent.before[lane] && current != after {
            return Err(invalid("shared disk journal frontier differs"));
        }
        for slot in 0..intent.counts[lane] as usize {
            let row = next(&mut entries, &position(lane, slot, false))?;
            let coordinate: Coordinate = decode(&row)?;
            let body = if coordinate.has_body {
                Some(next(&mut entries, &position(lane, slot, true))?)
            } else {
                None
            };
            if current == after {
                continue;
            }
            bytes = bytes
                .saturating_add(row.len() as u64)
                .saturating_add(body.as_ref().map_or(0, |body| body.len() as u64));
            if bytes > lastdb_storage_policy::native_write::PENDING_MEMORY_BYTES * 2 {
                return Err(invalid("shared disk journal byte limit"));
            }
            batch.push(HomeMutation {
                collection: coordinate.collection,
                key: coordinate.key,
                body,
            });
        }
    }
    if entries.next().is_some() {
        return Err(invalid("shared disk journal has unexpected operations"));
    }
    Ok(batches)
}

fn next(
    entries: &mut impl Iterator<Item = laststore::Result<(Vec<u8>, Found)>>,
    expected: &[u8],
) -> StorageResult<Vec<u8>> {
    match entries.next().transpose().map_err(map)? {
        Some((key, Found::Value(value))) if key == expected => Ok(value),
        _ => Err(invalid("shared disk journal operation order differs")),
    }
}

fn apply(
    homes: &[&RunHome; 3],
    intent: &Intent,
    mut batches: [Vec<HomeMutation>; 3],
) -> StorageResult<()> {
    for lane in [Lane::Files, Lane::Records, Lane::Private] {
        let slot = lane as usize;
        let home = homes[slot];
        let after = expected_after(intent, slot)?;
        let current = frontier(home);
        if current == after {
            continue;
        }
        if current != intent.before[slot] || batches[slot].len() != intent.counts[slot] as usize {
            return Err(invalid("shared disk journal frontier differs"));
        }
        let written = home
            .commit(std::mem::take(&mut batches[slot]))
            .map_err(map)?;
        if written.frontier != after {
            return Err(invalid(
                "shared disk write did not reach its expected frontier",
            ));
        }
    }
    Ok(())
}

fn position(lane: usize, slot: usize, body: bool) -> [u8; 7] {
    let mut key = [1, lane as u8, 0, 0, 0, 0, u8::from(body)];
    key[2..6].copy_from_slice(&(slot as u32).to_be_bytes());
    key
}

fn encode(value: &impl Serialize) -> StorageResult<Vec<u8>> {
    serde_json::to_vec(value).map_err(|_| invalid("shared disk journal encoding"))
}

fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> StorageResult<T> {
    serde_json::from_slice(bytes).map_err(|_| invalid("shared disk journal decoding"))
}

fn frontier(home: &RunHome) -> u64 {
    home.current_root().frontier()
}

fn expected_after(intent: &Intent, lane: usize) -> StorageResult<u64> {
    intent.before[lane]
        .checked_add(u64::from(intent.counts[lane] != 0))
        .ok_or_else(|| invalid("shared disk frontier overflow"))
}

fn release(private: &RunHome) -> StorageResult<()> {
    let path: &Path = private.path();
    // A crash leaves either the replayable run or an already complete
    // staging directory. The staging directory is never a replay source.
    fs::rename(path.join(PENDING), path.join(STAGE))?;
    File::open(path)?.sync_all()?;
    clear_stage(private)
}

fn clear_stage(private: &RunHome) -> StorageResult<()> {
    match fs::remove_dir_all(private.path().join(STAGE)) {
        Ok(()) => File::open(private.path())?.sync_all()?,
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}
