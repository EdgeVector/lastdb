//! Native run snapshot ownership behind the existing opaque file planner.
use super::{
    invalid, protocol, Inner, PackRuntime, RunBackupHome, RunBackupProfile, StorageResult,
};
use cloud_backup_protocol::{DecimalU64, Digest, Id};
use laststore::disk_run::home::{CapturedRunHome, ReadBudget, RunRead, SnapshotFile};
use sha2::{Digest as _, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub(super) struct RunSource {
    pub runtime: Arc<PackRuntime>,
    pub main: CapturedRunHome,
    pub cas: CapturedRunHome,
    pub root: PathBuf,
    pub files: Vec<SnapshotFile>,
    version: u16,
}
impl RunSource {
    pub fn open(runtime: Arc<PackRuntime>, root: PathBuf, key: &[u8; 32]) -> StorageResult<Self> {
        let resources = lastdb_storage_policy::BACKUP_RESOURCES;
        // Each transport can hold a producer, queued and HTTP-consumer buffer.
        // Keep the existing codec's plaintext/ciphertext/JSON work space as well.
        let workspace = resources
            .stream_buffer_bytes
            .checked_mul(resources.concurrent_streams as u64)
            .and_then(|n| n.checked_mul(3))
            .and_then(|n| n.checked_add(super::CODEC_PLAINTEXT_MAX_BYTES * 3))
            .ok_or_else(|| invalid("run capture workspace size overflow"))?;
        let maximum = resources
            .working_memory_bytes
            .checked_sub(workspace)
            .ok_or_else(|| invalid("run capture workspace allowance"))?;
        let main = CapturedRunHome::open_bounded(
            &root.join("main"),
            &super::super::run_node::home_key(key),
            maximum,
        )
        .map_err(super::super::map)?;
        let remaining = maximum
            .checked_sub(main.retained_bytes())
            .ok_or_else(|| invalid("run paired metadata allowance"))?;
        let cas = CapturedRunHome::open_bounded(
            &root.join("cas"),
            &super::super::run_node::cas_key(key),
            remaining,
        )
        .map_err(super::super::map)?;
        let (version, marker_bytes) = marker_profile(&runtime, &root)?;
        let count = main
            .files
            .len()
            .checked_add(cas.files.len())
            .and_then(|n| n.checked_add(1))
            .ok_or_else(|| invalid("run capture file count"))?;
        let inventory = (count * std::mem::size_of::<SnapshotFile>()) as u64
            + main
                .files
                .iter()
                .chain(&cas.files)
                .map(|f| f.path.as_os_str().len() as u64 + 9)
                .sum::<u64>();
        if main
            .retained_bytes()
            .saturating_add(cas.retained_bytes())
            .saturating_add(inventory)
            > maximum
        {
            return Err(invalid("run paired inventory allowance"));
        }
        let mut files = Vec::new();
        files
            .try_reserve_exact(count)
            .map_err(|_| invalid("run inventory allocation"))?;
        files.push(SnapshotFile {
            path: "laststore-pack-node-v1".into(),
            bytes: marker_bytes,
        });
        for (prefix, source) in [("disk-run", &main), ("disk-cas", &cas)] {
            for file in &source.files {
                files.push(SnapshotFile {
                    path: Path::new(prefix).join(&file.path),
                    bytes: file.bytes,
                });
            }
        }
        files.sort_by(|a, b| a.path.cmp(&b.path));
        let source = Self {
            runtime,
            main,
            cas,
            root,
            files,
            version,
        };
        if source.retained_bytes() > maximum {
            return Err(invalid("run paired retained metadata allowance"));
        }
        Ok(source)
    }
    fn node_file(root: &Path) -> StorageResult<File> {
        Ok(OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(root.join("node"))?)
    }
    pub fn reader(&self, path: &Path) -> StorageResult<File> {
        if path == Path::new("laststore-pack-node-v1") {
            return Self::node_file(&self.root);
        }
        for (prefix, source) in [("disk-run", &self.main), ("disk-cas", &self.cas)] {
            if let Ok(tail) = path.strip_prefix(prefix) {
                return source.reader(tail).map_err(super::super::map);
            }
        }
        Err(invalid("run backup unknown path"))
    }
    pub fn profile(&self) -> StorageResult<RunBackupProfile> {
        let profile = RunBackupProfile {
            version: self.version,
            main: self.home_profile("disk-run", &self.main)?,
            cas: self.home_profile("disk-cas", &self.cas)?,
        };
        profile.validate()?;
        Ok(profile)
    }
    fn home_profile(&self, prefix: &str, home: &CapturedRunHome) -> StorageResult<RunBackupHome> {
        let bytes = home
            .files
            .iter()
            .try_fold(0u64, |n, f| n.checked_add(f.bytes))
            .ok_or_else(|| invalid("run backup size overflow"))?;
        Ok(RunBackupHome {
            home_id: Id::try_from(home.home_id.to_string()).map_err(protocol)?,
            generation: DecimalU64::new(home.generation),
            frontier: DecimalU64::new(home.frontier),
            descriptor_sha256: hash(
                self.reader(&Path::new(prefix).join("laststore-run-home-v2"))?,
            )?,
            current_sha256: hash(self.reader(&Path::new(prefix).join("CURRENT"))?)?,
            files: home.files.len() as u64,
            bytes,
        })
    }
    pub fn reads(&self, budget: ReadBudget) -> StorageResult<(RunRead, RunRead)> {
        let homes = &self.runtime.physical;
        let retained = self
            .retained_bytes()
            .saturating_add(self.main.reader_bytes())
            .saturating_add(self.cas.reader_bytes());
        if retained > lastdb_storage_policy::BACKUP_RESOURCES.working_memory_bytes {
            return Err(invalid("run paired read allowance"));
        }
        let mut main = self
            .main
            .read(&homes.home, budget)
            .map_err(super::super::map)?;
        let mut cas = self
            .cas
            .read(&homes.cas, budget)
            .map_err(super::super::map)?;
        main.reserve_metadata(retained).map_err(super::super::map)?;
        cas.reserve_metadata(retained).map_err(super::super::map)?;
        Ok((main, cas))
    }
    fn retained_bytes(&self) -> u64 {
        self.main.retained_bytes()
            + self.cas.retained_bytes()
            + (self.files.capacity() * std::mem::size_of::<SnapshotFile>()
                + self.files.iter().map(|f| f.path.capacity()).sum::<usize>()) as u64
    }
    pub fn release_saved(root: &Path) -> StorageResult<()> {
        CapturedRunHome::release_saved(&root.join("main")).map_err(super::super::map)?;
        CapturedRunHome::release_saved(&root.join("cas")).map_err(super::super::map)?;
        for name in ["node", "pair-ready"] {
            match fs::remove_file(root.join(name)) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        match fs::remove_dir(root) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        if let Some(parent) = root.parent().filter(|p| p.exists()) {
            File::open(parent)?.sync_all()?;
        }
        Ok(())
    }
}
fn marker_profile(runtime: &PackRuntime, root: &Path) -> StorageResult<(u16, u64)> {
    use schema_types::key_comparison::KeyOrderVersion;
    let mut marker = RunSource::node_file(root)?;
    let mut bytes = Vec::new();
    (&mut marker).take(129).read_to_end(&mut bytes)?;
    let version = match (bytes.as_slice(), runtime.range_order()) {
        (super::super::run_node::RUN_NODE_BYTES, KeyOrderVersion::LegacyUtf8V1) => 1,
        (super::super::run_admission::TYPED_NODE_BYTES, KeyOrderVersion::TypedScalarV1) => 2,
        _ => {
            return Err(invalid(
                "run backup node marker differs from its logical order",
            ))
        }
    };
    Ok((version, bytes.len() as u64))
}

pub(super) fn certify(inner: &Inner, key: &[u8; 32]) -> StorageResult<()> {
    let source = inner
        .run
        .as_ref()
        .ok_or_else(|| invalid("run source unavailable"))?;
    let (main, cas) = source.reads(super::super::budget())?;
    super::super::run_restore::verify_readers_for_profile(
        &main,
        &cas,
        key,
        &inner.store.root,
        source.version,
    )
    .map(|_| ())
}
fn hash(mut input: File) -> StorageResult<Digest> {
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 65536];
    loop {
        let n = input.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    Digest::try_from(crate::hex::hex_lower(hash.finalize())).map_err(protocol)
}
impl Inner {
    pub fn available(&self) -> bool {
        self.run.is_some()
    }
    pub fn files(&self) -> StorageResult<&[SnapshotFile]> {
        self.run
            .as_ref()
            .map(|run| run.files.as_slice())
            .ok_or_else(|| invalid("byte retained source unavailable"))
    }
    pub fn file_reader(&self, path: &Path) -> StorageResult<File> {
        self.run
            .as_ref()
            .ok_or_else(|| invalid("byte retained source unavailable"))?
            .reader(path)
    }
    /// The store of each run file: the CAS files go to B2, the rest to R2.
    pub fn destination(
        &self,
        path: &Path,
    ) -> StorageResult<Option<cloud_backup_protocol::v2::Destination>> {
        super::run_backup_destination(
            path.to_str()
                .ok_or_else(|| invalid("run backup path encoding"))?,
        )
        .map(Some)
    }
}
