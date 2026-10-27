//! Backup-only CURRENT admission. Normal boot, publication and codecs stay unchanged.
use super::{
    corrupt, get_len, get_slice, get_varint, Manifest, RunFile, RunMeta, MAGIC, MAGIC_V1,
    MAX_CURRENT_BYTES, MAX_LEVELS, MAX_LEVEL_RUNS, MAX_RUN_KEY_BYTES, RUN_SUFFIX,
};
use crate::frame::VerifiedFrameReader;
use crate::read_receipt::TemporaryBytes;
use crate::{Error, Result};
use std::fs::OpenOptions;
use std::io::Read;
use std::mem::size_of;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicU64},
    Arc,
};
use uuid::Uuid;
use zeroize::Zeroizing;

const ARC_HEADER: usize = 2 * size_of::<usize>();

impl Manifest {
    pub(in crate::disk_run::home) fn load_paths_bounded(
        current: &Path,
        runs: &Path,
        key: &[u8; 32],
        home_id: Uuid,
        maximum: u64,
    ) -> Result<Self> {
        let maximum = maximum.min(lastdb_storage_policy::BACKUP_RESOURCES.working_memory_bytes);
        let minimum = 2 * lastdb_storage_policy::BACKUP_RESOURCES.stream_buffer_bytes + 16;
        if maximum < minimum {
            return Err(Error::ReadBudgetExceeded);
        }
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
            .open(current)?;
        let length = crate::read_receipt_io::status(|| file.metadata())?.len();
        if length < crate::frame::min_encoded_len() as u64 || length > MAX_CURRENT_BYTES as u64 {
            return Err(corrupt("CURRENT length"));
        }
        let mut input = VerifiedFrameReader::open(&file, 0, length, key)?;
        let bytes = input.payload_len();
        if bytes
            .checked_add(input.memory_bytes())
            .map_or(true, |n| n > maximum)
        {
            return Err(Error::ReadBudgetExceeded);
        }
        let count = usize::try_from(bytes).map_err(|_| Error::ReadBudgetExceeded)?;
        let mut payload = Zeroizing::new(Vec::new());
        payload
            .try_reserve_exact(count)
            .map_err(|_| Error::ReadBudgetExceeded)?;
        if (payload.capacity() as u64)
            .checked_add(input.memory_bytes())
            .map_or(true, |n| n > maximum)
        {
            return Err(Error::ReadBudgetExceeded);
        }
        let _payload_charge = TemporaryBytes::new(payload.capacity());
        payload.resize(count, 0);
        input.read_exact(&mut payload)?;
        let mut extra = [0];
        if input.read(&mut extra)? != 0 {
            return Err(corrupt("CURRENT length"));
        }
        drop(input);
        let mut budget = Admission::new(maximum, payload.capacity() as u64)?;
        budget.add((size_of::<Self>() + ARC_HEADER) as u64)?;
        let manifest = decode(&payload, runs, &mut budget)?;
        if manifest.home_id != home_id {
            return Err(corrupt("CURRENT names another home"));
        }
        Ok(manifest)
    }

    pub(in crate::disk_run::home) fn retained_bytes(&self) -> u64 {
        let mut bytes = (size_of::<Self>() + ARC_HEADER) as u64;
        bytes =
            bytes.saturating_add((self.levels.capacity() * size_of::<Vec<Arc<RunFile>>>()) as u64);
        if let Some(cut) = &self.merge_cut {
            bytes = bytes.saturating_add(cut.capacity() as u64);
        }
        for list in self.levels.iter().chain([&self.merging, &self.base]) {
            bytes = bytes.saturating_add((list.capacity() * size_of::<Arc<RunFile>>()) as u64);
            for run in list {
                bytes = bytes
                    .saturating_add((size_of::<RunFile>() + ARC_HEADER) as u64)
                    .saturating_add(run.path.capacity() as u64)
                    .saturating_add(run.meta.first_key.capacity() as u64)
                    .saturating_add(run.meta.last_key.capacity() as u64);
            }
        }
        bytes
    }
}

fn decode(bytes: &[u8], runs: &Path, budget: &mut Admission) -> Result<Manifest> {
    let with_base = match bytes.get(..8) {
        Some(magic) if magic == MAGIC.as_slice() => true,
        Some(magic) if magic == MAGIC_V1.as_slice() => false,
        _ => return Err(corrupt("bad CURRENT")),
    };
    if bytes.len() < 24 {
        return Err(corrupt("bad CURRENT"));
    }
    let home_id = Uuid::from_slice(&bytes[8..24]).map_err(|_| corrupt("bad CURRENT"))?;
    let mut at = 24;
    let generation = get_varint(bytes, &mut at)?;
    let frontier = get_varint(bytes, &mut at)?;
    let count = get_len(bytes, &mut at, MAX_LEVELS)?;
    let mut levels = budget.array(count)?;
    for _ in 0..count {
        levels.push(run_list(bytes, &mut at, runs, with_base, budget)?);
    }
    let (mut merging, mut merge_cut, mut base) = (Vec::new(), None, Vec::new());
    if with_base {
        merging = run_list(bytes, &mut at, runs, true, budget)?;
        let cut = get_len(bytes, &mut at, MAX_RUN_KEY_BYTES + 1)?;
        if let Some(length) = cut.checked_sub(1) {
            merge_cut = Some(budget.copy(get_slice(bytes, &mut at, length)?)?);
        }
        base = run_list(bytes, &mut at, runs, true, budget)?;
    }
    if at != bytes.len() || generation == 0 {
        return Err(corrupt("bad CURRENT"));
    }
    let manifest = Manifest {
        home_id,
        generation,
        frontier,
        levels,
        merging,
        merge_cut,
        base,
    };
    check_shape(&manifest, budget)?;
    Ok(manifest)
}

fn run_list(
    bytes: &[u8],
    at: &mut usize,
    directory: &Path,
    counts: bool,
    budget: &mut Admission,
) -> Result<Vec<Arc<RunFile>>> {
    let count = get_len(bytes, at, MAX_LEVEL_RUNS)?;
    let mut runs = budget.array(count)?;
    for _ in 0..count {
        let run_id = Uuid::from_slice(get_slice(bytes, at, 16)?)
            .map_err(|_| corrupt("bad run entry in CURRENT"))?;
        let records = get_varint(bytes, at)?;
        let (tombstones, record_bytes, tombstone_bytes, file_bytes, hidden) = if counts {
            let mut values = [0; 5];
            for value in &mut values {
                *value = get_varint(bytes, at)?;
            }
            (values[0], values[1], values[2], values[3], values[4])
        } else {
            let size = get_varint(bytes, at)?;
            (0, size, 0, size, 0)
        };
        let length = get_len(bytes, at, MAX_RUN_KEY_BYTES)?;
        let first_key = budget.copy(get_slice(bytes, at, length)?)?;
        let length = get_len(bytes, at, MAX_RUN_KEY_BYTES)?;
        let last_key = budget.copy(get_slice(bytes, at, length)?)?;
        if run_id.is_nil()
            || records == 0
            || tombstones > records
            || tombstone_bytes > record_bytes
            || first_key > last_key
        {
            return Err(corrupt("bad run entry in CURRENT"));
        }
        budget.add((size_of::<RunFile>() + ARC_HEADER) as u64)?;
        let path = budget.path(directory, run_id)?;
        let meta = RunMeta {
            run_id,
            records,
            tombstones,
            record_bytes,
            tombstone_bytes,
            file_bytes,
            first_key,
            last_key,
        };
        runs.push(Arc::new(RunFile {
            meta,
            path,
            obsolete: AtomicBool::new(false),
            hidden: AtomicU64::new(hidden),
        }));
    }
    Ok(runs)
}

fn check_shape(manifest: &Manifest, budget: &mut Admission) -> Result<()> {
    let count = manifest.newest_first().count();
    let mut ids = budget.array(count)?;
    ids.extend(manifest.newest_first().map(|run| run.meta.run_id));
    ids.sort_unstable();
    if ids.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(corrupt("CURRENT names a run twice"));
    }
    if manifest.merge_cut.is_some() && manifest.merging.is_empty() {
        return Err(corrupt("CURRENT has a base merge cut but no merging runs"));
    }
    if !manifest
        .base
        .windows(2)
        .all(|pair| pair[0].meta.last_key < pair[1].meta.first_key)
        || manifest.base.iter().any(|run| run.meta.tombstones > 0)
    {
        return Err(corrupt("CURRENT has an unordered base"));
    }
    Ok(())
}

struct Admission {
    maximum: u64,
    used: u64,
}
impl Admission {
    fn new(maximum: u64, used: u64) -> Result<Self> {
        let mut budget = Self { maximum, used: 0 };
        budget.add(used)?;
        Ok(budget)
    }
    fn add(&mut self, bytes: u64) -> Result<()> {
        self.used = self
            .used
            .checked_add(bytes)
            .filter(|n| *n <= self.maximum)
            .ok_or(Error::ReadBudgetExceeded)?;
        Ok(())
    }
    fn array<T>(&mut self, count: usize) -> Result<Vec<T>> {
        let bytes = count
            .checked_mul(size_of::<T>())
            .ok_or(Error::ReadBudgetExceeded)?;
        self.add(bytes as u64)?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(count)
            .map_err(|_| Error::ReadBudgetExceeded)?;
        self.add((values.capacity().saturating_sub(count) * size_of::<T>()) as u64)?;
        Ok(values)
    }
    fn copy(&mut self, bytes: &[u8]) -> Result<Vec<u8>> {
        let mut value = self.array(bytes.len())?;
        value.extend_from_slice(bytes);
        Ok(value)
    }
    fn path(&mut self, directory: &Path, id: Uuid) -> Result<PathBuf> {
        let mut name = [0; 42];
        let _ = id.hyphenated().encode_lower(&mut name[..36]);
        name[36..].copy_from_slice(RUN_SUFFIX.as_bytes());
        let name = std::str::from_utf8(&name).map_err(|_| corrupt("bad run path"))?;
        let size = directory
            .as_os_str()
            .len()
            .checked_add(1 + name.len())
            .ok_or(Error::ReadBudgetExceeded)?;
        self.add(size as u64)?;
        let mut path = PathBuf::new();
        path.try_reserve_exact(size)
            .map_err(|_| Error::ReadBudgetExceeded)?;
        self.add(path.capacity().saturating_sub(size) as u64)?;
        path.push(directory);
        path.push(name);
        Ok(path)
    }
}
