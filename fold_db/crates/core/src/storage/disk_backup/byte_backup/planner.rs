//! Snapshot plan over frozen run files, source ownership, and client metadata.
use super::{
    codec, invalid, object_name, payload_name, protocol, run_backup_destination,
    store::{sync_dir, Store},
    ByteBackupCapture, ClientPage, ClientRoot, Inner, PackRuntime, RunBackupHome, RunBackupProfile,
    State, StorageError, StorageResult, CODEC_PLAINTEXT_MAX_BYTES, HELD_NODE_FILE, MAX_OBJECTS,
};
use crate::storage::cloud_backup_plan::{
    plan_files, ClientNativeFile, PlanError, PlanFile, PlanSegment, PlannedRecord,
};
use crate::storage::disk_pack::{budget, map, run_node, NODE_MARKER, RUN_DIR};
use cloud_backup_protocol::v2::Object;
use cloud_backup_protocol::{DecimalU64, Digest, Id, RelativePath};
use lastdb_model::storage_policy::BACKUP_RESOURCES;
use laststore::disk_run::home::{CapturedRunHome, SnapshotFile, CURRENT, RUN_HOME_MARKER};
use sha2::{Digest as _, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub(super) struct RunSource {
    pub runtime: Arc<PackRuntime>,
    pub main: CapturedRunHome,
    pub root: PathBuf,
    pub files: Vec<SnapshotFile>,
    version: u16,
}

impl RunSource {
    pub fn open(runtime: Arc<PackRuntime>, root: PathBuf, key: &[u8; 32]) -> StorageResult<Self> {
        let ws = (BACKUP_RESOURCES.stream_buffer_bytes
            * (BACKUP_RESOURCES.concurrent_streams as u64 * 3))
            .saturating_add(CODEC_PLAINTEXT_MAX_BYTES * 3);
        let max = BACKUP_RESOURCES
            .working_memory_bytes
            .checked_sub(ws)
            .ok_or_else(|| invalid("run capture workspace allowance"))?;
        let mut marker_raw = Vec::new();
        open_nofollow(&root.join(HELD_NODE_FILE))?
            .take(129)
            .read_to_end(&mut marker_raw)?;
        let version = run_node::profile(&marker_raw)
            .ok_or_else(|| invalid("run backup node marker is not a supported node format"))?;
        let main = CapturedRunHome::open_bounded(&root.join("main"), &run_node::home_key(key), max)
            .map_err(map)?;
        let count = main.files.len() + 1;
        let inv = (count * std::mem::size_of::<SnapshotFile>()) as u64
            + main
                .files
                .iter()
                .map(|f| f.path.as_os_str().len() as u64 + 9)
                .sum::<u64>();
        if main.retained_bytes().saturating_add(inv) > max {
            return Err(invalid("run paired inventory allowance"));
        }
        let mut files = Vec::new();
        files
            .try_reserve_exact(count)
            .map_err(|_| invalid("run inventory allocation"))?;
        files.push(SnapshotFile {
            path: NODE_MARKER.into(),
            bytes: marker_raw.len() as u64,
        });
        files.extend(main.files.iter().map(|f| SnapshotFile {
            path: Path::new(RUN_DIR).join(&f.path),
            bytes: f.bytes,
        }));
        files.sort_by(|a, b| a.path.cmp(&b.path));
        let source = Self {
            runtime,
            main,
            root,
            files,
            version,
        };
        (source.retained_bytes() <= max)
            .then_some(source)
            .ok_or_else(|| invalid("run paired retained metadata allowance"))
    }

    pub fn total_bytes(&self) -> StorageResult<u64> {
        sum_bytes(
            self.files.iter().map(|f| f.bytes),
            "run capture size overflow",
        )
    }

    pub fn reader(&self, path: &Path) -> StorageResult<File> {
        if path == Path::new(NODE_MARKER) {
            return open_nofollow(&self.root.join(HELD_NODE_FILE));
        }
        let rel = path
            .strip_prefix(RUN_DIR)
            .map_err(|_| invalid("run backup unknown path"))?;
        self.main.reader(rel).map_err(map)
    }

    pub fn profile(&self) -> StorageResult<RunBackupProfile> {
        let hash = |name| -> StorageResult<Digest> {
            let mut h = Sha256::new();
            io::copy(&mut self.reader(&Path::new(RUN_DIR).join(name))?, &mut h)?;
            Digest::try_from(lastdb_model::hex::hex_lower(h.finalize())).map_err(protocol)
        };
        let profile = RunBackupProfile {
            version: self.version,
            main: RunBackupHome {
                home_id: Id::try_from(self.main.home_id.to_string()).map_err(protocol)?,
                generation: DecimalU64::new(self.main.generation),
                frontier: DecimalU64::new(self.main.frontier),
                descriptor_sha256: hash(RUN_HOME_MARKER)?,
                current_sha256: hash(CURRENT)?,
                files: self.main.files.len() as u64,
                bytes: sum_bytes(
                    self.main.files.iter().map(|f| f.bytes),
                    "run backup size overflow",
                )?,
            },
        };
        profile.validate().map(|()| profile)
    }

    pub fn certify(&self, key: &[u8; 32]) -> StorageResult<()> {
        let retained = self
            .retained_bytes()
            .saturating_add(self.main.reader_bytes());
        if retained > BACKUP_RESOURCES.working_memory_bytes {
            return Err(invalid("run paired read allowance"));
        }
        let mut main = self
            .main
            .read(&self.runtime.physical.home, budget())
            .map_err(map)?;
        main.reserve_metadata(retained).map_err(map)?;
        super::super::run_restore::verify_captured_account(&main, key, self.version)
    }

    fn retained_bytes(&self) -> u64 {
        self.main.retained_bytes()
            + (self.files.capacity() * std::mem::size_of::<SnapshotFile>()
                + self.files.iter().map(|f| f.path.capacity()).sum::<usize>()) as u64
    }

    pub fn release_saved(root: &Path) -> StorageResult<()> {
        CapturedRunHome::release_saved(&root.join("main")).map_err(map)?;
        let ignore = |r: io::Result<()>| match r {
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            other => other,
        };
        for name in [HELD_NODE_FILE, "pair-ready", "pair-ready.pending"] {
            ignore(fs::remove_file(root.join(name)))?;
        }
        ignore(fs::remove_dir(root))?;
        if let Some(p) = root.parent().filter(|p| p.exists()) {
            sync_dir(p)?;
        }
        Ok(())
    }
}

pub(super) fn held_root(runtime: &PackRuntime, id: uuid::Uuid) -> PathBuf {
    runtime
        .physical
        .path()
        .join(laststore::disk_run::home::BACKUP_DIR)
        .join("holds")
        .join(id.to_string())
}

pub(super) fn clear_objects(store: &Store, last: u64) -> StorageResult<()> {
    for index in 0..=last {
        for name in [object_name(index), payload_name(index)] {
            store.remove(&name)?;
            store.remove(&format!("{name}.pending"))?;
        }
    }
    Ok(())
}

pub(super) fn finish(
    inner: Inner,
    key: &[u8; 32],
    admit_export: bool,
) -> StorageResult<ByteBackupCapture> {
    let mut state = inner
        .state
        .lock()
        .map_err(|_| invalid("byte source state lock"))?;
    if let Some(run) = &inner.run {
        let (files, bytes) = (run.files.len() as u64, run.total_bytes()?);
        if state.native_files != 0 && (state.native_files, state.native_bytes) != (files, bytes) {
            return Err(invalid("byte frozen source inventory changed"));
        }
        (state.native_files, state.native_bytes) = (files, bytes);
    }
    if state.is_active() && admit_export && inner.run.is_none() {
        return Err(invalid("byte retained source unavailable"));
    }
    if !state.ready && state.is_active() && inner.run.is_some() {
        build(&inner, &mut state, key)?;
    }
    inner.advance(&mut state)?;
    drop(state);
    let capture = ByteBackupCapture {
        inner: Arc::new(inner),
    };
    capture.finish_terminal().map(|()| capture)
}

fn open_nofollow(path: &Path) -> StorageResult<File> {
    Ok(OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?)
}

fn sum_bytes(lens: impl IntoIterator<Item = u64>, msg: &'static str) -> StorageResult<u64> {
    lens.into_iter()
        .try_fold(0u64, u64::checked_add)
        .ok_or_else(|| invalid(msg))
}

fn build(inner: &Inner, state: &mut State, key: &[u8; 32]) -> StorageResult<()> {
    let source = inner
        .run
        .as_ref()
        .ok_or_else(|| invalid("byte retained source unavailable"))?;
    source.certify(key)?;
    let inv = &source.files;
    (state.native_files, state.native_bytes) = (inv.len() as u64, source.total_bytes()?);
    (state.metadata_pack_objects, state.multi_file_pack_objects) = (0, 0);
    inner.store.write("job.json", state)?;
    let files = inv
        .iter()
        .enumerate()
        .map(|(id, f)| {
            let text = f
                .path
                .to_str()
                .ok_or_else(|| invalid("byte native UTF8 path"))?;
            Ok(PlanFile {
                id,
                path: RelativePath::try_from(text.to_owned()).map_err(protocol)?,
                bytes: f.bytes,
                destination: run_backup_destination(text)?,
            })
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let (mut native_files, scope) = (
        Vec::with_capacity(files.len()),
        state.begin.job.stream.scope.clone(),
    );
    let (objects, packs, multi) = plan_files(
        &files,
        &scope,
        |file| source.reader(&inv[file.id].path),
        |index, object, segments, nfiles| {
            native_files.extend(nfiles);
            save(inner, state, index, (object, segments, None)).map(|_| ())
        },
    )?;
    (state.metadata_pack_objects, state.multi_file_pack_objects) = (packs, multi);
    finish_metadata(inner, state, key, objects, &native_files)?;
    state.ready = true;
    state.validate()?;
    inner.store.write("job.json", state)
}

fn save(
    inner: &Inner,
    state: &mut State,
    index: u64,
    spec: (Object, Vec<PlanSegment>, Option<String>),
) -> StorageResult<PlannedRecord> {
    if index >= MAX_OBJECTS {
        return Err(invalid("byte source object count bound"));
    }
    let name = object_name(index);
    let rec = PlannedRecord::upsert(
        inner.store.read_opt(&name)?,
        &state.begin.job,
        spec,
        &payload_name(index),
        |i| ((i as u64) < state.native_files).then_some(u64::MAX),
    )?;
    inner.store.write(&name, &rec)?;
    state.objects = state.objects.max(index + 1);
    inner.store.write("job.json", state).map(|()| rec)
}

pub fn seal_pages<P: serde::Serialize, R: serde::Serialize, E>(
    native_files: &[ClientNativeFile],
    mut index: u64,
    mut make_page: impl FnMut(u32, Vec<ClientNativeFile>) -> Result<P, E>,
    make_root: impl FnOnce(Vec<super::ClientPageReference>) -> Result<R, E>,
    mut seal_one: impl FnMut(u64, &[u8]) -> Result<PlannedRecord, E>,
    map_err: impl Fn(StorageError) -> E,
) -> Result<u64, E> {
    let mut pages = Vec::new();
    for (i, chunk) in native_files.chunks(codec::MAX_PAGE_FILES).enumerate() {
        let plain =
            codec::encode_metadata(&make_page(i as u32, chunk.to_vec())?).map_err(&map_err)?;
        let obj = seal_one(index, &plain)?;
        let pr = super::ClientPageReference::for_object(i as u32, &obj.input)
            .map_err(|e| map_err(protocol(e)))?;
        pages.push(pr);
        index += 1;
    }
    let plain = codec::encode_metadata(&make_root(pages)?).map_err(&map_err)?;
    seal_one(index, &plain).map(|_| index + 1)
}

fn finish_metadata(
    inner: &Inner,
    state: &mut State,
    key: &[u8; 32],
    index: u64,
    native_files: &[ClientNativeFile],
) -> StorageResult<()> {
    let bytes = sum_bytes(
        native_files.iter().map(|f| f.native_bytes.get()),
        "byte native size overflow",
    )?;
    if native_files.len() as u64 != state.native_files || bytes != state.native_bytes {
        return Err(invalid("byte exact native inventory totals"));
    }
    let (begin, native_root, recovery, run, file_count) = (
        state.begin.clone(),
        state.native_root.clone(),
        state.recovery.clone(),
        state.run.clone(),
        state.native_files,
    );
    let total = seal_pages(
        native_files,
        index,
        |i, chunk| ClientPage::new(i, &begin, chunk),
        |pages| {
            let (begin_sha256, descriptor_sha256) = codec::begin_digests(&begin)?;
            let root = ClientRoot {
                version: codec::CLIENT_VERSION,
                begin_sha256,
                descriptor_sha256,
                native_root,
                recovery,
                native_file_count: file_count,
                native_bytes: bytes,
                pages,
                run,
            };
            root.validate_for(&begin).map(|()| root)
        },
        |idx, plain| {
            let name = payload_name(idx);
            let prev = inner
                .store
                .read_raw_opt(&name, codec::CODEC_ENVELOPE_MAX_BYTES)?;
            let (obj, fresh) = codec::seal_stored_envelope(key, plain, prev).map_err(invalid)?;
            if let Some(b) = fresh {
                inner.store.write_raw(&name, &b)?;
            }
            save(inner, state, idx, (obj, Vec::new(), Some(name)))
        },
        |e| e,
    )?;
    (state.objects == total)
        .then_some(())
        .ok_or_else(|| invalid("byte frozen source object total differs"))
}

impl From<PlanError> for StorageError {
    fn from(error: PlanError) -> Self {
        invalid(&error.to_string())
    }
}
