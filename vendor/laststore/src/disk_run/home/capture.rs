//! Durable flat holds of unchanged run-home files. Never a second owner.
use super::{
    manifest::Manifest, read_descriptor_file, RunHome, RunRead, RunReadCosts, RUN_HOME_MARKER,
};
use crate::disk_pack::ReadBudget;
use crate::durability::sync_directory;
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use uuid::Uuid;

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
fn invalid(message: &str) -> Error {
    Error::Config(message.into())
}
fn write_uuid<S: serde::Serializer>(value: &Uuid, out: S) -> std::result::Result<S::Ok, S::Error> {
    out.serialize_str(&value.to_string())
}
fn read_uuid<'de, D: serde::Deserializer<'de>>(input: D) -> std::result::Result<Uuid, D::Error> {
    let value = String::deserialize(input)?;
    Uuid::parse_str(&value).map_err(serde::de::Error::custom)
}

impl RunHome {
    /// Link one exact cut into an absent private directory on this filesystem.
    /// This takes only the publication mutex; hashing occurs after this call.
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
            manifest: Arc::default(),
        };
        capture.link(
            &self.root.join(RUN_HOME_MARKER),
            RUN_HOME_MARKER,
            "descriptor",
            CapturedRunKind::Descriptor,
            64,
        )?;
        let current = self.root.join("CURRENT");
        capture.link(
            &current,
            "CURRENT",
            "current",
            CapturedRunKind::Current,
            fs::metadata(&current)?.len(),
        )?;
        for run in selected.newest_first() {
            let held = format!("{}.ldrun", run.meta.run_id);
            capture.link(
                &run.path,
                &format!("runs/{held}"),
                &held,
                CapturedRunKind::Run,
                run.meta.file_bytes,
            )?;
        }
        capture.files.sort_by(|a, b| a.path.cmp(&b.path));
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(destination.join("capture.json"))?;
        serde_json::to_writer(&mut output, &capture)
            .map_err(|_| invalid("run capture metadata encoding"))?;
        output.write_all(b"\n")?;
        output.sync_all()?;
        sync_directory(&File::open(destination)?)?;
        sync_directory(&File::open(
            destination
                .parent()
                .ok_or_else(|| invalid("capture parent"))?,
        )?)?;
        Ok(capture)
    }
}
impl CapturedRunHome {
    fn link(
        &mut self,
        source: &Path,
        path: &str,
        held: &str,
        kind: CapturedRunKind,
        bytes: u64,
    ) -> Result<()> {
        let before = fs::symlink_metadata(source)?;
        if !before.is_file() || before.len() != bytes {
            return Err(invalid("run capture original file"));
        }
        let target = self.root.join(held);
        fs::hard_link(source, &target)?;
        let after = fs::symlink_metadata(&target)?;
        if !after.is_file()
            || (before.dev(), before.ino(), before.len()) != (after.dev(), after.ino(), after.len())
        {
            return Err(invalid("run capture inode changed"));
        }
        self.files.push(CapturedRunFile {
            path: path.into(),
            held: held.into(),
            kind,
            bytes,
        });
        Ok(())
    }
    /// Reopen and authenticate a durable cut without touching live CURRENT.
    pub fn open(root: &Path, key: &[u8; 32]) -> Result<Self> {
        Self::open_bounded(
            root,
            key,
            lastdb_storage_policy::BACKUP_RESOURCES.working_memory_bytes,
        )
    }
    /// Reopen with explicit combined capture metadata-memory admission.
    pub fn open_bounded(root: &Path, key: &[u8; 32], maximum: u64) -> Result<Self> {
        let directory = fs::symlink_metadata(root)?;
        if !directory.is_dir() || directory.mode() & 0o077 != 0 {
            return Err(invalid("run capture directory"));
        }
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
            .open(root.join("capture.json"))?;
        let metadata = file.metadata()?;
        if !metadata.is_file()
            || metadata.uid() != directory.uid()
            || metadata.mode() & 0o077 != 0
            || metadata.len().saturating_mul(3) > maximum
        {
            return Err(crate::Error::ReadBudgetExceeded);
        }
        let mut capture: Self = serde_json::from_reader(file.take(64 * 1024 * 1024))
            .map_err(|_| invalid("run capture metadata format"))?;
        capture.root = root.to_path_buf();
        if read_descriptor_file(&root.join("descriptor"))? != capture.home_id {
            return Err(invalid("run capture descriptor identity"));
        }
        let maximum = maximum
            .checked_sub(capture.file_metadata_bytes())
            .ok_or(crate::Error::ReadBudgetExceeded)?;
        let manifest = Manifest::load_paths_bounded(
            &root.join("current"),
            root,
            key,
            capture.home_id,
            maximum,
        )?;
        if manifest.generation != capture.generation || manifest.frontier != capture.frontier {
            return Err(invalid("run capture frontier"));
        }
        capture.validate_files(&manifest)?;
        let _ = capture.manifest.set(Arc::new(manifest));
        Ok(capture)
    }
    fn file_metadata_bytes(&self) -> u64 {
        (self.files.capacity() * std::mem::size_of::<CapturedRunFile>()
            + self
                .files
                .iter()
                .map(|f| f.path.capacity() + f.held.capacity())
                .sum::<usize>()) as u64
    }
    /// In-memory metadata retained by this durable cut, excluding file bodies.
    pub fn retained_bytes(&self) -> u64 {
        self.file_metadata_bytes() + self.manifest.get().map_or(0, |m| m.retained_bytes())
    }
    /// Additional bytes of a read-call slot vector at this exact cut.
    pub fn reader_bytes(&self) -> u64 {
        self.manifest.get().map_or(0, |m| {
            m.newest_first().count()
                * std::mem::size_of::<(
                    Arc<super::manifest::RunFile>,
                    Option<crate::disk_run::RunReader>,
                )>()
        }) as u64
    }
    fn validate_files(&self, manifest: &Manifest) -> Result<()> {
        let count = manifest
            .newest_first()
            .count()
            .checked_add(2)
            .ok_or_else(|| invalid("run capture file count"))?;
        if self.files.len() != count
            || self
                .files
                .windows(2)
                .any(|pair| pair[0].path >= pair[1].path)
            || self.files[0].path != Path::new("CURRENT")
            || self.files[0].held != "current"
            || self.files[1].path != Path::new(RUN_HOME_MARKER)
            || self.files[1].held != "descriptor"
        {
            return Err(invalid("run capture exact file closure"));
        }
        let runs = &self.files[2..];
        for file in runs {
            if file.path.parent() != Some(Path::new("runs"))
                || file.path.file_name() != Some(std::ffi::OsStr::new(&file.held))
            {
                return Err(invalid("run capture exact file closure"));
            }
        }
        for run in manifest.newest_first() {
            let mut name = [0; 42];
            let _ = run.meta.run_id.hyphenated().encode_lower(&mut name[..36]);
            name[36..].copy_from_slice(super::super::RUN_SUFFIX.as_bytes());
            if runs
                .binary_search_by(|file| file.held.as_bytes().cmp(name.as_slice()))
                .is_err()
            {
                return Err(invalid("run capture exact file closure"));
            }
        }
        for file in &self.files {
            let meta = fs::symlink_metadata(self.root.join(&file.held))?;
            if !meta.is_file() || meta.len() != file.bytes {
                return Err(invalid("run capture file length"));
            }
        }
        for run in manifest.newest_first() {
            if fs::metadata(&run.path)?.len() != run.meta.file_bytes {
                return Err(invalid("run capture referenced file length"));
            }
        }
        Ok(())
    }
    /// Open one unchanged held file. Callers separately verify its complete hash.
    pub fn reader(&self, native: &Path) -> Result<File> {
        let file = self
            .files
            .iter()
            .find(|f| f.path == native)
            .ok_or_else(|| invalid("run capture unknown file"))?;
        let input = OpenOptions::new()
            .read(true)
            .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
            .open(self.root.join(&file.held))?;
        if !input.metadata()?.is_file() || input.metadata()?.len() != file.bytes {
            return Err(invalid("run capture reader length"));
        }
        Ok(input)
    }
    /// Read the exact captured root while the same native owner keeps its lock.
    pub fn read(&self, owner: &RunHome, budget: ReadBudget) -> Result<RunRead> {
        if self.retained_bytes().saturating_add(self.reader_bytes())
            > lastdb_storage_policy::BACKUP_RESOURCES.working_memory_bytes
        {
            return Err(crate::Error::ReadBudgetExceeded);
        }
        if owner.identity != self.home_id {
            return Err(invalid("run capture owner identity"));
        }
        let manifest = Arc::clone(
            self.manifest
                .get()
                .ok_or_else(|| invalid("run capture is not reopened"))?,
        );
        let available = lastdb_storage_policy::BACKUP_RESOURCES
            .working_memory_bytes
            .checked_sub(self.retained_bytes())
            .ok_or(crate::Error::ReadBudgetExceeded)?;
        let runs = super::read_only::slots(&manifest, available)?;
        let shared_metadata_bytes = self.retained_bytes()
            + (runs.capacity() * std::mem::size_of::<super::read_only::ReadSlot>()) as u64;
        Ok(RunRead {
            bounded_streams: true,
            shared_metadata_bytes,
            runs,
            manifest,
            key: owner.key,
            budget,
            costs: RunReadCosts::default(),
            _owner_lock: Arc::clone(&owner.lock),
        })
    }
    /// Remove only this capture's flat files after the caller proves release.
    pub fn release(&self) -> Result<()> {
        for file in &self.files {
            match fs::remove_file(self.root.join(&file.held)) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        match fs::remove_file(self.root.join("capture.json")) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        sync_directory(&File::open(&self.root)?)?;
        Ok(())
    }
    /// Release only recognized flat capture links, including an interrupted capture.
    /// The caller supplies the separate durable terminal or unsubmitted authority.
    pub fn release_saved(root: &Path) -> Result<()> {
        let entries = match fs::read_dir(root) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name();
            let name = name
                .to_str()
                .ok_or_else(|| invalid("run capture release name"))?;
            let run = name
                .strip_suffix(".ldrun")
                .and_then(|s| Uuid::parse_str(s).ok());
            if !matches!(name, "descriptor" | "current" | "capture.json")
                && !run.is_some_and(|id| !id.is_nil() && format!("{id}.ldrun") == name)
            {
                return Err(invalid("run capture release coordinate"));
            }
            if !fs::symlink_metadata(entry.path())?.is_file() {
                return Err(invalid("run capture release type"));
            }
            fs::remove_file(entry.path())?;
        }
        sync_directory(&File::open(root)?)?;
        fs::remove_dir(root)?;
        sync_directory(&File::open(
            root.parent()
                .ok_or_else(|| invalid("run capture release parent"))?,
        )?)
    }
}
