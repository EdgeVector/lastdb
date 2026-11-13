//! Durable flat holds of unchanged run-home files for snapshots, commit retention, and backup.

use super::cleanup::euid;
use super::commit::RunCommit;
use super::manifest::Manifest;
use super::{
    invalid, open::ReadSlot, open_nofollow, read_descriptor_file, verified_hard_link,
    write_private_file, ReadBudget, RunHome, RunRead, CURRENT, RUNS, RUN_HOME_MARKER,
};
use crate::disk_run::RUN_SUFFIX;
use crate::durability::sync_directory;
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Read;
use std::os::unix::fs::{DirBuilderExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use uuid::Uuid;

pub(super) fn private_parent(path: &Path) -> Result<()> {
    let parent = path.parent().ok_or_else(|| invalid("log file parent"))?;
    let info = fs::symlink_metadata(parent)?;
    if !path.is_absolute()
        || parent.canonicalize()? != parent
        || !info.is_dir()
        || info.uid() != euid()
        || info.mode() & 0o077 != 0
    {
        return Err(invalid("log private directory"));
    }
    Ok(())
}

pub(super) fn link_synced(source: &Path, target: &Path, bytes: u64) -> Result<()> {
    verified_hard_link(source, target, bytes, "log retained inode differs")?;
    File::open(target)?.sync_all()?;
    sync_directory(target.parent().ok_or_else(|| invalid("log link parent"))?)
}

impl RunCommit {
    /// The held root protects the new run from compaction until this link is durable.
    pub fn retain_file(&self, destination: &Path) -> Result<()> {
        private_parent(destination)?;
        let file = self
            .root
            .0
            .newest_first()
            .find(|f| f.meta.run_id == self.run_id)
            .ok_or_else(|| invalid("commit run is absent from its held root"))?;
        if file.meta.file_bytes != self.run_bytes {
            return Err(invalid("commit run length"));
        }
        link_synced(&file.path, destination, self.run_bytes)
    }
}

/// The native role of a captured file, independent of any cloud provider.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CapturedRunKind {
    /// The clear home identity descriptor.
    Descriptor,
    /// The original encrypted CURRENT inode.
    Current,
    /// An immutable encrypted sorted run.
    Run,
}

/// One original native file and its durable flat hardlink.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapturedRunFile {
    /// Original native path relative to the home.
    pub path: PathBuf,
    /// Opaque file name inside the flat capture directory.
    pub held: String,
    /// Native role; no storage-provider policy is implied.
    pub kind: CapturedRunKind,
    /// Exact unchanged file length.
    pub bytes: u64,
}

/// One original immutable file, or an exact generated snapshot selector.
#[derive(Clone, Debug)]
pub struct SnapshotFile {
    /// Exact path relative to the native home, including snapshot selectors.
    pub path: PathBuf,
    /// Complete stored file length, at most one native part.
    pub bytes: u64,
}

/// A durable exact run-home cut. Drop never releases its hardlinks.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapturedRunHome {
    /// Original home identity.
    #[serde(serialize_with = "write_uuid", deserialize_with = "read_uuid")]
    pub home_id: Uuid,
    /// Physical generation selected under the publication lock.
    pub generation: u64,
    /// Logical frontier of the captured state.
    pub frontier: u64,
    /// The complete descriptor, CURRENT and run file set.
    pub files: Vec<CapturedRunFile>,
    #[serde(skip)]
    root: PathBuf,
    #[serde(skip)]
    manifest: Arc<OnceLock<Arc<Manifest>>>,
}

fn write_uuid<S: serde::Serializer>(v: &Uuid, out: S) -> std::result::Result<S::Ok, S::Error> {
    out.serialize_str(&v.to_string())
}

fn read_uuid<'de, D: serde::Deserializer<'de>>(inp: D) -> std::result::Result<Uuid, D::Error> {
    Uuid::parse_str(&String::deserialize(inp)?).map_err(serde::de::Error::custom)
}

impl RunHome {
    /// Link one exact cut into an absent private directory on this filesystem.
    pub fn capture_files(&self, destination: &Path) -> Result<CapturedRunHome> {
        let _publish = self.publish.lock().unwrap_or_else(|e| e.into_inner());
        self.refuse_if_poisoned()?;
        fs::DirBuilder::new().mode(0o700).create(destination)?;
        let selected = self.held();
        let mut capture = CapturedRunHome {
            home_id: self.identity,
            generation: selected.generation,
            frontier: selected.frontier,
            files: Vec::new(),
            root: destination.to_path_buf(),
            manifest: Arc::new(OnceLock::new()),
        };
        let (marker, current) = (self.root.join(RUN_HOME_MARKER), self.root.join(CURRENT));
        for (src, path, held, kind, len) in [
            (
                &marker,
                RUN_HOME_MARKER,
                "descriptor",
                CapturedRunKind::Descriptor,
                64,
            ),
            (
                &current,
                CURRENT,
                "current",
                CapturedRunKind::Current,
                fs::metadata(&current)?.len(),
            ),
        ] {
            capture.link(src, path, held, kind, len)?;
        }
        for r in selected.newest_first() {
            let held = format!("{}{RUN_SUFFIX}", r.meta.run_id);
            let path = format!("{RUNS}/{held}");
            capture.link(
                &r.path,
                &path,
                &held,
                CapturedRunKind::Run,
                r.meta.file_bytes,
            )?;
        }
        capture.files.sort_by(|a, b| a.path.cmp(&b.path));
        let mut encoded =
            serde_json::to_vec(&capture).map_err(|_| invalid("run capture metadata encoding"))?;
        encoded.push(b'\n');
        write_private_file(&destination.join("capture.json"), &encoded)?;
        sync_directory(destination)?;
        sync_directory(
            destination
                .parent()
                .ok_or_else(|| invalid("capture parent"))?,
        )?;
        Ok(capture)
    }
}

impl CapturedRunHome {
    fn link(
        &mut self,
        src: &Path,
        path: &str,
        held: &str,
        kind: CapturedRunKind,
        bytes: u64,
    ) -> Result<()> {
        verified_hard_link(
            src,
            &self.root.join(held),
            bytes,
            "run capture inode changed",
        )?;
        self.files.push(CapturedRunFile {
            path: path.into(),
            held: held.into(),
            kind,
            bytes,
        });
        Ok(())
    }

    /// Reopen with explicit combined capture metadata-memory admission.
    pub fn open_bounded(root: &Path, key: &[u8; 32], maximum: u64) -> Result<Self> {
        let dir = fs::symlink_metadata(root)?;
        if !dir.is_dir() || dir.mode() & 0o077 != 0 {
            return Err(invalid("run capture directory"));
        }
        let file = open_nofollow(&root.join("capture.json"))?;
        let meta = file.metadata()?;
        if !meta.is_file()
            || meta.uid() != dir.uid()
            || meta.mode() & 0o077 != 0
            || meta.len().saturating_mul(3) > maximum
        {
            return Err(Error::ReadBudgetExceeded);
        }
        let mut capture: Self = serde_json::from_reader(file.take(64 << 20))
            .map_err(|_| invalid("run capture metadata format"))?;
        capture.root = root.to_path_buf();
        if read_descriptor_file(&root.join("descriptor"))? != capture.home_id {
            return Err(invalid("run capture descriptor identity"));
        }
        let avail = maximum
            .checked_sub(capture.file_metadata_bytes())
            .ok_or(Error::ReadBudgetExceeded)?;
        let manifest =
            Manifest::load_paths_bounded(&root.join("current"), root, key, capture.home_id, avail)?;
        if (manifest.generation, manifest.frontier) != (capture.generation, capture.frontier) {
            return Err(invalid("run capture frontier"));
        }
        capture.validate_files(&manifest)?;
        let _ = capture.manifest.set(Arc::new(manifest));
        Ok(capture)
    }

    fn file_metadata_bytes(&self) -> u64 {
        let heap: usize = self
            .files
            .iter()
            .map(|f| f.path.capacity() + f.held.capacity())
            .sum();
        (self.files.capacity() * std::mem::size_of::<CapturedRunFile>() + heap) as u64
    }

    /// In-memory metadata retained by this durable cut, excluding file bodies.
    pub fn retained_bytes(&self) -> u64 {
        self.file_metadata_bytes() + self.manifest.get().map_or(0, |m| m.retained_bytes())
    }

    /// Additional bytes of a read-call slot vector at this exact cut.
    pub fn reader_bytes(&self) -> u64 {
        self.manifest.get().map_or(0, |m| {
            m.newest_first().count() * std::mem::size_of::<ReadSlot>()
        }) as u64
    }

    fn validate_files(&self, manifest: &Manifest) -> Result<()> {
        let closure = || invalid("run capture exact file closure");
        let count = manifest
            .newest_first()
            .count()
            .checked_add(2)
            .ok_or_else(|| invalid("run capture file count"))?;
        if self.files.len() != count
            || self.files.windows(2).any(|p| p[0].path >= p[1].path)
            || (&*self.files[0].path, &*self.files[0].held) != (Path::new(CURRENT), "current")
            || (&*self.files[1].path, &*self.files[1].held)
                != (Path::new(RUN_HOME_MARKER), "descriptor")
            || self.files[2..].iter().any(|f| {
                f.path.parent() != Some(Path::new(RUNS))
                    || f.path.file_name() != Some(std::ffi::OsStr::new(&f.held))
            })
        {
            return Err(closure());
        }
        for r in manifest.newest_first() {
            let held = format!("{}{RUN_SUFFIX}", r.meta.run_id);
            if self.files[2..]
                .binary_search_by(|f| f.held.cmp(&held))
                .is_err()
                || fs::metadata(&r.path)?.len() != r.meta.file_bytes
            {
                return Err(closure());
            }
        }
        for f in &self.files {
            let m = fs::symlink_metadata(self.root.join(&f.held))?;
            if !m.is_file() || m.len() != f.bytes {
                return Err(invalid("run capture file length"));
            }
        }
        Ok(())
    }

    /// Open one unchanged held file. Callers separately verify its complete hash.
    pub fn reader(&self, native: &Path) -> Result<File> {
        let f = self
            .files
            .iter()
            .find(|f| f.path == native)
            .ok_or_else(|| invalid("run capture unknown file"))?;
        let input = open_nofollow(&self.root.join(&f.held))?;
        let m = input.metadata()?;
        (m.is_file() && m.len() == f.bytes)
            .then_some(input)
            .ok_or_else(|| invalid("run capture reader length"))
    }

    /// Read the exact captured root while the same native owner keeps its lock.
    pub fn read(&self, owner: &RunHome, budget: ReadBudget) -> Result<RunRead> {
        let retained = self.retained_bytes();
        let avail = lastdb_model::storage_policy::BACKUP_RESOURCES
            .working_memory_bytes
            .checked_sub(retained)
            .filter(|&a| a >= self.reader_bytes())
            .ok_or(Error::ReadBudgetExceeded)?;
        if owner.identity != self.home_id {
            return Err(invalid("run capture owner identity"));
        }
        let m = Arc::clone(
            self.manifest
                .get()
                .ok_or_else(|| invalid("run capture is not reopened"))?,
        );
        RunRead::bounded_from(
            m,
            owner.key,
            budget,
            avail,
            Some(retained),
            Arc::clone(&owner.lock),
        )
    }

    /// Release only recognized flat capture links, including an interrupted capture.
    pub fn release_saved(root: &Path) -> Result<()> {
        let entries = match fs::read_dir(root) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        for entry in entries {
            let entry = entry?;
            let raw = entry.file_name();
            let name = raw
                .to_str()
                .ok_or_else(|| invalid("run capture release name"))?;
            let valid_run = name
                .strip_suffix(RUN_SUFFIX)
                .and_then(|s| Uuid::parse_str(s).ok())
                .is_some_and(|id| !id.is_nil() && format!("{id}{RUN_SUFFIX}") == name);
            if !matches!(name, "descriptor" | "current" | "capture.json") && !valid_run {
                return Err(invalid("run capture release coordinate"));
            }
            if !fs::symlink_metadata(entry.path())?.is_file() {
                return Err(invalid("run capture release type"));
            }
            fs::remove_file(entry.path())?;
        }
        sync_directory(root)?;
        fs::remove_dir(root)?;
        sync_directory(
            root.parent()
                .ok_or_else(|| invalid("run capture release parent"))?,
        )
    }
}
