use super::dependency_state::*;
use super::store::{directory, read_json, regular, write_json, LocalStore};
use super::{invalid, StorageResult};
use cloud_backup_protocol::{BackupJob, Digest, FileDependencyKind};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fs::{self, File};
use std::path::{Path, PathBuf};

pub(super) struct DependencyStore {
    pub root: PathBuf,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Checked<T> {
    sha256: Digest,
    value: T,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DependencyAlias {
    job: BackupJob,
    pointer: Digest,
    kind: FileDependencyKind,
    index: u64,
}

impl DependencyStore {
    pub fn open(store: &LocalStore, job: &BackupJob) -> StorageResult<Self> {
        let root = store
            .root()
            .join(format!("dependencies-{}", job.commit_id.as_str()));
        directory(&root)?;
        Ok(Self { root })
    }

    pub fn state(
        &self,
        job: &BackupJob,
        current: &Digest,
    ) -> StorageResult<Option<DependencyState>> {
        let path = self.root.join("state.json");
        if !path.try_exists()? {
            return Ok(None);
        }
        let value: DependencyState = self.read(&path)?;
        value.validate(job, current)?;
        Ok(Some(value))
    }

    pub fn save_state(&self, state: &DependencyState) -> StorageResult<()> {
        self.write(&self.root.join("state.json"), state)
    }

    pub fn entry(&self, job: &BackupJob, index: u64) -> StorageResult<BackupFileDependencyEntry> {
        let value: BackupFileDependencyEntry = self.read(&self.entry_path(index))?;
        value.validate(job, index)?;
        Ok(value)
    }

    pub fn save_entry(&self, entry: &BackupFileDependencyEntry) -> StorageResult<()> {
        entry.validate(&entry.job, entry.index)?;
        self.write(&self.entry_path(entry.index), entry)
    }

    pub fn find(
        &self,
        job: &BackupJob,
        pointer: &Digest,
        kind: FileDependencyKind,
    ) -> StorageResult<Option<u64>> {
        let path = self.alias_path(pointer, kind);
        if !path.try_exists()? {
            return Ok(None);
        }
        let alias: DependencyAlias = self.read(&path)?;
        if alias.job != *job
            || alias.pointer != *pointer
            || alias.kind != kind
            || alias.index >= ENTRIES_MAX
        {
            return Err(invalid("backup dependency lookup identity differs"));
        }
        Ok(Some(alias.index))
    }

    pub fn save_alias(&self, entry: &BackupFileDependencyEntry) -> StorageResult<()> {
        let alias = DependencyAlias {
            job: entry.job.clone(),
            pointer: entry.dependency.logical_pointer_sha256.clone(),
            kind: entry.dependency.kind,
            index: entry.index,
        };
        self.write(&self.alias_path(&alias.pointer, alias.kind), &alias)
    }

    fn alias_path(&self, pointer: &Digest, kind: FileDependencyKind) -> PathBuf {
        self.root.join(format!(
            "pointer-{}.json",
            super::super::file_recovery::entry_key(pointer, kind)
        ))
    }

    pub fn entry_path(&self, index: u64) -> PathBuf {
        self.root.join(format!("entry-{index:06}.json"))
    }

    pub fn entry_bytes(&self, index: u64) -> StorageResult<u64> {
        optional_size(&self.entry_path(index))
    }

    pub fn entry_metadata_bytes(&self, entry: &BackupFileDependencyEntry) -> StorageResult<u64> {
        Ok(self.entry_bytes(entry.index)?
            + optional_size(&self.alias_path(
                &entry.dependency.logical_pointer_sha256,
                entry.dependency.kind,
            ))?
            + self.part_bytes(entry.index, parts_for(entry))?)
    }

    pub fn tracked_spool_bytes(&self, state: &DependencyState) -> StorageResult<u64> {
        let mut bytes = state.catalog_bytes;
        for name in ["state.json", "state.pending"] {
            bytes += optional_size(&self.root.join(name))?;
        }
        if let Some(index) = state.active_external {
            bytes = bytes
                .checked_sub(state.active_entry_bytes)
                .ok_or_else(|| invalid("backup catalog byte count differs"))?
                + self.entry_bytes(index)?
                + optional_size(&self.entry_path(index).with_extension("pending"))?
                + self.part_bytes(index, parts_for(&self.entry(&state.job, index)?))?;
        }
        if bytes > cloud_backup_protocol::limits::DURABLE_LOCAL_SPOOL_MAX_BYTES {
            return Err(invalid("backup dependency spool exceeds its cap"));
        }
        Ok(bytes)
    }
    pub fn blob_path(&self, index: u64) -> PathBuf {
        self.root.join(format!("blob-{index:06}"))
    }
    pub fn pending(&self) -> PathBuf {
        self.root.join("source.pending")
    }

    pub(super) fn read<T: DeserializeOwned + Serialize>(&self, path: &Path) -> StorageResult<T> {
        let checked: Checked<T> = read_json(path, ENTRY_MAX)?;
        if cloud_backup_protocol::input_digest("lastdb:backup-capture:local:v1", &checked.value)
            .map_err(super::protocol)?
            != checked.sha256
        {
            return Err(invalid("backup dependency local checksum"));
        }
        Ok(checked.value)
    }

    pub(super) fn write<T: Serialize>(&self, path: &Path, value: &T) -> StorageResult<()> {
        let checked = Checked {
            sha256: cloud_backup_protocol::input_digest("lastdb:backup-capture:local:v1", value)
                .map_err(super::protocol)?,
            value,
        };
        write_json(path, &checked, ENTRY_MAX).map(|_| ())
    }

    pub fn spool_bytes(&self, state: &DependencyState) -> StorageResult<u64> {
        let entries = state.entries;
        let mut bytes = 0u64;
        for name in ["state.json", "state.pending"] {
            bytes += optional_size(&self.root.join(name))?;
        }
        for index in 0..entries.saturating_add(3).min(ENTRIES_MAX) {
            let path = self.entry_path(index);
            bytes = bytes
                .checked_add(
                    optional_size(&path)?
                        + optional_size(&path.with_extension("pending"))?
                        + if index < entries {
                            self.part_bytes(index, parts_for(&self.entry(&state.job, index)?))?
                        } else {
                            0
                        },
                )
                .ok_or_else(|| invalid("backup dependency spool overflow"))?;
        }
        for index in 0..entries {
            // Entries carry the exact alias key; no arbitrary directory lookup.
            let entry = self.entry(&state.job, index)?;
            let path = self.alias_path(
                &entry.dependency.logical_pointer_sha256,
                entry.dependency.kind,
            );
            bytes += optional_size(&path)? + optional_size(&path.with_extension("pending"))?;
        }
        if bytes > cloud_backup_protocol::limits::DURABLE_LOCAL_SPOOL_MAX_BYTES {
            return Err(invalid("backup dependency spool exceeds its cap"));
        }
        Ok(bytes)
    }

    pub fn release_blob(&self, index: u64) -> StorageResult<()> {
        remove(&self.blob_path(index))?;
        // Repeat the barrier even when a previous unlink returned uncertain.
        File::open(&self.root)?.sync_all()?;
        Ok(())
    }

    pub fn release_pending(&self) -> StorageResult<()> {
        remove(&self.pending())?;
        File::open(&self.root)?.sync_all()?;
        Ok(())
    }

    pub(super) fn remove_owned(&self, path: &Path) -> StorageResult<()> {
        if path.parent() != Some(self.root.as_path()) {
            return Err(invalid("backup scratch parent"));
        }
        remove(path)?;
        File::open(&self.root)?.sync_all()?;
        Ok(())
    }

    pub(super) fn payload_bytes(&self, state: &DependencyState) -> StorageResult<u64> {
        let mut bytes = 0u64;
        for name in [
            "source.pending",
            "native-cas.pending",
            "native-cas.json.pending",
            "native-cas.inner.pending",
        ] {
            bytes = bytes
                .checked_add(optional_size(&self.root.join(name))?)
                .ok_or_else(|| invalid("backup payload size overflow"))?;
        }
        if let Some(index) = state.active_external {
            bytes = bytes
                .checked_add(optional_size(&self.blob_path(index))?)
                .ok_or_else(|| invalid("backup payload size overflow"))?;
        }
        Ok(bytes)
    }
}

/// Terminal cleanup uses only the private flat directory of the exact saved job.
/// It also works after partial or corrupt local dependency metadata. No native
/// source file and no other job directory is enumerated or removed.
pub(super) fn cleanup_owned(store: &LocalStore, job: &BackupJob) -> StorageResult<()> {
    let root = store
        .root()
        .join(format!("dependencies-{}", job.commit_id.as_str()));
    if !root.try_exists()? {
        File::open(store.root())?.sync_all()?;
        return Ok(());
    }
    directory(&root)?;
    let mut count = 0u64;
    for entry in fs::read_dir(&root)? {
        let entry = entry?;
        count += 1;
        if count
            > (2 * cloud_backup_protocol::limits::MANIFEST_PAGE_ENTRIES_MAX as u64 + 5)
                * (ENTRIES_MAX + 3)
                + 6
        {
            return Err(invalid(
                "backup dependency cleanup file count exceeds its cap",
            ));
        }
        let name = entry.file_name();
        let name = name
            .to_str()
            .ok_or_else(|| invalid("backup dependency cleanup name"))?;
        let simple = matches!(
            name,
            "state.json"
                | "state.pending"
                | "source.pending"
                | "native-cas.pending"
                | "native-cas.json.pending"
                | "native-cas.inner.pending"
        );
        let numbered = name
            .strip_prefix("entry-")
            .and_then(|s| {
                s.strip_suffix(".json")
                    .or_else(|| s.strip_suffix(".pending"))
            })
            .or_else(|| name.strip_prefix("blob-"));
        let pointer = name
            .strip_prefix("pointer-")
            .and_then(|s| {
                s.strip_suffix(".json")
                    .or_else(|| s.strip_suffix(".pending"))
            })
            .and_then(|s| s.split_once('-'))
            .is_some_and(|(hash, kind)| {
                hash.len() == 64
                    && hash
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                    && matches!(kind, "main" | "thumbnail" | "poster")
            });
        let chunk = name
            .strip_prefix("chunk-")
            .and_then(|tail| {
                tail.strip_suffix(".json")
                    .or_else(|| tail.strip_suffix(".pending"))
            })
            .and_then(|tail| tail.split_once('-'))
            .is_some_and(|(index, part)| {
                index.len() == 6
                    && part.len() == 3
                    && index
                        .bytes()
                        .chain(part.bytes())
                        .all(|byte| byte.is_ascii_digit())
                    && index.parse::<u64>().is_ok_and(|value| value < ENTRIES_MAX)
                    && part.parse::<usize>().is_ok_and(|value| {
                        value < cloud_backup_protocol::limits::MANIFEST_PAGE_ENTRIES_MAX
                    })
            });
        if !simple
            && !pointer
            && !chunk
            && !numbered.is_some_and(|n| {
                n.len() == 6
                    && n.bytes().all(|b| b.is_ascii_digit())
                    && n.parse::<u64>().is_ok_and(|n| n < ENTRIES_MAX)
            })
        {
            return Err(invalid("unexpected backup dependency cleanup member"));
        }
        regular(&entry.path())?;
        fs::remove_file(entry.path())?;
    }
    File::open(&root)?.sync_all()?;
    fs::remove_dir(root)?;
    File::open(store.root())?.sync_all()?;
    Ok(())
}

pub(super) fn optional_size(path: &Path) -> StorageResult<u64> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.is_file() => {
            regular(path)?;
            Ok(meta.len())
        }
        Ok(_) => Err(invalid("backup dependency path is not regular")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error.into()),
    }
}

fn parts_for(entry: &BackupFileDependencyEntry) -> u16 {
    match entry.coverage {
        FileCoverage::BackupOwnedExtents { parts, .. } => parts,
        FileCoverage::ExternalNeeded => {
            let parts = entry
                .dependency
                .expected_stored_bytes
                .div_ceil(cloud_backup_protocol::limits::CLOUD_OBJECT_MAX_BYTES);
            if parts <= cloud_backup_protocol::limits::MANIFEST_PAGE_ENTRIES_MAX as u64 {
                parts as u16
            } else {
                0
            }
        }
        _ => 0,
    }
}

fn remove(path: &Path) -> StorageResult<()> {
    if optional_size(path)? != 0 || path.try_exists()? {
        match fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}
