//! Hidden-version counting, checkpoints, orphaned-run cleanup, and isolated diagnostics.

use super::compaction::tail;
use super::manifest::{Manifest, RunFile};
use super::{invalid, open_nofollow, sweep_runs, unpoison, write_private_file, RunHome};
use crate::disk_run::codec::corrupt;
use crate::disk_run::{Found, MergeIter, RunRange, RunReader};
use crate::durability::sync_directory;
use crate::frame::{decode_frame, encode_frame};
use crate::space::SpacePolicy;
use crate::{Error, Result};
use serde::Serialize;
use std::collections::HashSet;
use std::fs;
use std::io::Read;
use std::ops::Bound;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

const MAX_ENTRIES: usize = 1_000_000;
const COUNT_KEYS: usize = 4096;

/// Specification for an authenticated private checkpoint file inside a [`RunHome`].
#[derive(Debug, Clone, Copy)]
pub struct CheckpointSpec {
    /// File name inside the home directory.
    pub file_name: &'static str,
    /// Eight-byte domain tag bound into the encrypted payload.
    pub magic: &'static [u8; 8],
    /// Maximum unencrypted checkpoint body size in bytes.
    pub max_bytes: usize,
}

impl CheckpointSpec {
    fn validate(self) -> Result<()> {
        let valid = !self.file_name.is_empty()
            && !self.file_name.starts_with('.')
            && !self.file_name.contains('/');
        valid
            .then_some(())
            .ok_or_else(|| invalid("invalid checkpoint file name"))
    }
}

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

fn euid() -> u32 {
    rustix::process::geteuid().as_raw()
}

impl RunHome {
    pub(super) fn count_hidden(&self, held: &Manifest) -> Result<()> {
        let runs: Vec<&Arc<RunFile>> = held.newest_first().collect();
        for (at, run) in runs.iter().enumerate().take(held.tiered().count()) {
            if run.hidden().is_none() {
                run.set_hidden(self.hidden_bytes(run, &runs[at + 1..])?);
            }
        }
        Ok(())
    }

    fn hidden_bytes(&self, run: &RunFile, older: &[&Arc<RunFile>]) -> Result<u64> {
        let (rk0, rk1) = (&run.meta.first_key, &run.meta.last_key);
        let older: Vec<_> = older
            .iter()
            .copied()
            .filter(|o| o.meta.first_key <= *rk1 && *rk0 <= o.meta.last_key)
            .collect();
        if older.is_empty() {
            return Ok(0);
        }
        let reader = Arc::new(RunReader::open(&run.path, &self.key)?);
        let mut readers: Vec<Option<RunReader>> = (0..older.len()).map(|_| None).collect();
        let (mut records, mut hidden) = (reader.iter(), 0u64);
        loop {
            let keys: Vec<Vec<u8>> = records
                .by_ref()
                .take(COUNT_KEYS)
                .map(|r| r.map(|(k, _)| k))
                .collect::<Result<_>>()?;
            let Some(first) = keys.first() else {
                return Ok(hidden);
            };
            let mut open: Vec<usize> = (0..keys.len()).collect();
            for (slot, old) in older.iter().enumerate() {
                if old.meta.last_key < *first {
                    readers[slot] = None;
                    continue;
                }
                let lookups: Vec<&[u8]> = open
                    .iter()
                    .filter_map(|&at| old.meta.covers(&keys[at]).then_some(keys[at].as_slice()))
                    .collect();
                if !lookups.is_empty() {
                    if readers[slot].is_none() {
                        readers[slot] = Some(RunReader::open(&old.path, &self.key)?);
                    }
                    let r = readers[slot].as_ref().unwrap();
                    let mut hits = r
                        .get_many_with(&lookups, |rd, n, sc| {
                            rd.read_index(n, Some(sc)).map(Arc::new)
                        })?
                        .into_iter();
                    open.retain(|&at| {
                        let Some(found) = old
                            .meta
                            .covers(&keys[at])
                            .then(|| hits.next().unwrap())
                            .flatten()
                        else {
                            return true;
                        };
                        let vlen = match found {
                            Found::Value(v) => v.len(),
                            Found::Tombstone => 0,
                        };
                        hidden = hidden.saturating_add((keys[at].len() + vlen) as u64);
                        false
                    });
                }
                if keys.last().is_some_and(|last| old.meta.last_key <= *last) {
                    readers[slot] = None;
                }
                if open.is_empty() {
                    break;
                }
            }
        }
    }

    /// Run one explicit base-compaction step under `policy`.
    pub fn cleanup_step(&self, policy: &SpacePolicy) -> Result<bool> {
        self.refuse_if_poisoned()?;
        let _merge = unpoison(&self.merge_mutex);
        if self.has_unsealed_commits() {
            return self.seal_step().map(|()| true);
        }
        let held = self.held();
        if !held.merging.is_empty() {
            return self.base_step(None, policy).map(|()| true);
        }
        let tiered: Vec<_> = held.tiered().cloned().collect();
        (!tiered.is_empty())
            .then(|| self.base_step(Some(tail(&tiered)), policy))
            .transpose()
            .map(|r| r.is_some())
    }

    /// Read one authenticated private checkpoint, if present.
    pub fn load_checkpoint(&self, spec: CheckpointSpec) -> Result<Option<Vec<u8>>> {
        spec.validate()?;
        let mut file = match open_nofollow(&self.root_dir.join(spec.file_name)) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            res => res?,
        };
        let meta = file.metadata()?;
        let is_priv =
            meta.is_file() && meta.mode() & 0o077 == 0 && meta.nlink() == 1 && meta.uid() == euid();
        if !is_priv || meta.len() > (spec.max_bytes + 4096) as u64 {
            return Err(Error::Corrupt("invalid cleanup checkpoint file".into()));
        }
        let mut bytes = Vec::new();
        (&mut file)
            .take((spec.max_bytes + 4097) as u64)
            .read_to_end(&mut bytes)?;
        let payload = decode_frame(&self.key, &bytes, spec.max_bytes + 24, (None, None))?.2;
        let ok = payload.len() >= 24
            && &payload[..8] == spec.magic
            && &payload[8..24] == self.identity.as_bytes();
        let err = || Error::Corrupt("cleanup checkpoint belongs to another role or home".into());
        ok.then(|| Some(payload[24..].to_vec())).ok_or_else(err)
    }

    /// Durably replace one authenticated private checkpoint.
    pub fn save_checkpoint(&self, spec: CheckpointSpec, bytes: &[u8]) -> Result<()> {
        spec.validate()?;
        if bytes.len() > spec.max_bytes {
            return Err(Error::Corrupt(
                "cleanup checkpoint exceeds its byte bound".into(),
            ));
        }
        let _publish = unpoison(&self.publish_mutex);
        self.refuse_if_poisoned()?;
        let id = Uuid::new_v4();
        let payload = [spec.magic.as_slice(), self.identity.as_bytes(), bytes].concat();
        let pending = self.root_dir.join(format!(".{}.{id}.tmp", spec.file_name));
        write_private_file(&pending, &encode_frame(&self.key, id, 0, &payload)?)?;
        fs::rename(&pending, self.root_dir.join(spec.file_name))
            .map_err(Error::from)
            .and_then(|()| sync_directory(&self.root_dir))
            .inspect_err(|_| drop(fs::remove_file(&pending)))
    }

    /// Remove an authenticated private checkpoint after verifying its ownership.
    pub fn clear_checkpoint(&self, spec: CheckpointSpec) -> Result<()> {
        let _publish = self.publish_lock()?;
        self.refuse_if_poisoned()?;
        if self.load_checkpoint(spec)?.is_some() {
            fs::remove_file(self.root_dir.join(spec.file_name))?;
            sync_directory(&self.root_dir)?;
        }
        Ok(())
    }

    /// Remove unnamed files after writers/mergers finish, returning `(held_bytes, removed_files)`.
    pub fn cleanup_files(&self) -> Result<(u64, u64)> {
        let (_merge, _commit, _publish) = (
            unpoison(&self.merge_mutex),
            self.commit_lock()?,
            self.publish_lock()?,
        );
        self.refuse_if_poisoned()?;
        let (current, (retired_len, held)) = (self.held(), self.live_retired());
        if retired_len > MAX_ENTRIES {
            return Err(Error::WriteCapacity(
                "cleanup held-file bound exceeded".into(),
            ));
        }
        let keep: HashSet<_> = current
            .newest_first()
            .chain(&held)
            .map(|r| r.meta.id)
            .collect();
        let removed = sweep_runs(&self.runs_dir, &keep, MAX_ENTRIES)?;
        sync_directory(&self.runs_dir)?;
        current.verify_files("cleanup found an invalid current run file")?;
        Ok((held.iter().map(|r| r.meta.file_bytes).sum(), removed))
    }

    /// Capture storage diagnostics for the current published root.
    pub fn diagnostics(&self, with_inv: bool, deadline: Instant) -> Result<RunHomeDiagnostics> {
        let root = {
            let (_merge, _commit) = (unpoison(&self.merge_mutex), unpoison(&self.commit_mutex));
            self.refuse_if_poisoned()?;
            self.held()
        };
        let inventory = with_inv.then(|| {
            let mem = root.mem.all_records();
            let runs: Vec<_> = root
                .tiered()
                .chain(&root.merging)
                .map(|r| r.path.clone())
                .collect();
            let base = root.base.iter().map(|r| r.path.clone()).collect();
            let (complete, merged_keys) =
                count_merged_keys(&self.key, root.retained_bytes(), mem, &runs, base, deadline);
            LogicalInventory {
                complete,
                merged_keys,
            }
        });
        Ok(RunHomeDiagnostics {
            generation: root.generation,
            frontier: root.total_frontier(),
            inventory,
        })
    }
}

fn count_merged_keys(
    key: &[u8; 32],
    retained: u64,
    mem: Vec<(Vec<u8>, Found)>,
    runs: &[PathBuf],
    base: Vec<PathBuf>,
    deadline: Instant,
) -> (bool, u64) {
    let mut merged = 0u64;
    let complete = (|| -> Result<()> {
        let file_count = runs.len() + usize::from(!base.is_empty());
        if mem.is_empty() && file_count == 0 {
            return Ok(());
        }
        if file_count > 512 || retained > 56 << 20 {
            return Err(Error::ReadBudgetExceeded);
        }
        let mut sources = Vec::with_capacity(usize::from(!mem.is_empty()) + file_count);
        if !mem.is_empty() {
            sources.push(RunRange::memory(mem));
        }
        if file_count > 0 {
            let allow = ((56 << 20) - retained) / file_count as u64;
            for p in runs {
                sources.push(Arc::new(RunReader::open_with_limit(p, key, Some(allow))?).iter());
            }
            if !base.is_empty() {
                sources.push(RunRange::sequence(
                    base,
                    key,
                    Bound::Unbounded,
                    Bound::Unbounded,
                ));
            }
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
