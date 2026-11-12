//! Snapshot plan over frozen run files, source ownership, and client metadata.

use super::{
    codec, invalid, object_name, protocol, run_backup_destination, ClientPage, ClientPageReference,
    ClientRoot, Inner, ObjectRecord, PackRuntime, RunBackupHome, RunBackupProfile, State,
    StorageError, StorageResult, CODEC_PLAINTEXT_MAX_BYTES, HELD_NODE_FILE, MAX_OBJECTS,
};
use crate::storage::cloud_backup_plan::{
    new_transfer_input, plan_files, ClientNativeFile, PlanError, PlanFile, PlanSegment,
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
        let tail = path
            .strip_prefix(RUN_DIR)
            .map_err(|_| invalid("run backup unknown path"))?;
        self.main.reader(tail).map_err(map)
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
        let ignore_missing = |res: io::Result<()>| match res {
            Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        };
        for name in [HELD_NODE_FILE, "pair-ready", "pair-ready.pending"] {
            ignore_missing(fs::remove_file(root.join(name)))?;
        }
        ignore_missing(fs::remove_dir(root))?;
        if let Some(p) = root.parent().filter(|p| p.exists()) {
            File::open(p)?.sync_all()?;
        }
        Ok(())
    }
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

pub(super) fn build(inner: &Inner, state: &mut State, key: &[u8; 32]) -> StorageResult<()> {
    let source = inner
        .run
        .as_ref()
        .ok_or_else(|| invalid("byte retained source unavailable"))?;
    source.certify(key)?;
    let inv = &source.files;
    (state.native_files, state.native_bytes) = (inv.len() as u64, source.total_bytes()?);
    (state.metadata_pack_objects, state.multi_file_pack_objects) = (0, 0);
    inner.store.write("job.json", state)?;
    let mut files = Vec::with_capacity(inv.len());
    for (id, f) in inv.iter().enumerate() {
        let text = f
            .path
            .to_str()
            .ok_or_else(|| invalid("byte native UTF8 path"))?;
        let path = RelativePath::try_from(text.to_owned()).map_err(protocol)?;
        files.push(PlanFile {
            id,
            path,
            bytes: f.bytes,
            destination: run_backup_destination(text)?,
        });
    }
    let mut native_files = Vec::with_capacity(files.len());
    let scope = state.begin.job.stream.scope.clone();
    let totals = plan_files(
        &files,
        &scope,
        |file| source.reader(&inv[file.id].path),
        |index, p| {
            native_files.extend(p.native_files);
            save(inner, state, index, p.object, p.segments, None).map(|_| ())
        },
    )?;
    (state.metadata_pack_objects, state.multi_file_pack_objects) =
        (totals.packs, totals.multi_file_packs);
    finish_metadata(inner, state, key, totals.objects, &native_files)?;
    state.ready = true;
    state.validate()?;
    inner.store.write("job.json", state)
}

fn save(
    inner: &Inner,
    state: &mut State,
    index: u64,
    object: Object,
    segments: Vec<PlanSegment>,
    ciphertext: Option<String>,
) -> StorageResult<ObjectRecord> {
    if index >= MAX_OBJECTS {
        return Err(invalid("byte source object count bound"));
    }
    let name = object_name(index);
    let rec = if inner.store.exists(&name)? {
        let prior: ObjectRecord = inner.store.read(&name)?;
        prior.validate(state)?;
        if prior.input.object != object
            || prior.segments != segments
            || prior.ciphertext != ciphertext
        {
            return Err(invalid("byte frozen object plan differs"));
        }
        prior
    } else {
        let input = new_transfer_input(state.begin.job.clone(), object, None).map_err(protocol)?;
        ObjectRecord {
            input,
            segments,
            ciphertext,
            receipt: None,
        }
    };
    rec.validate(state)?;
    inner.store.write(&name, &rec)?;
    state.objects = state.objects.max(index + 1);
    inner.store.write("job.json", state).map(|()| rec)
}

fn finish_metadata(
    inner: &Inner,
    state: &mut State,
    key: &[u8; 32],
    mut index: u64,
    native_files: &[ClientNativeFile],
) -> StorageResult<()> {
    let bytes = native_files
        .iter()
        .try_fold(0u64, |n, f| n.checked_add(f.native_bytes.get()))
        .ok_or_else(|| invalid("byte native size overflow"))?;
    if native_files.len() as u64 != state.native_files || bytes != state.native_bytes {
        return Err(invalid("byte exact native inventory totals"));
    }
    let mut pages = Vec::new();
    for (i, chunk) in native_files.chunks(codec::MAX_PAGE_FILES).enumerate() {
        let page = ClientPage::new(i as u32, &state.begin, chunk.to_vec())?;
        let obj = freeze(inner, state, key, index, &codec::encode_metadata(&page)?)?;
        pages.push(ClientPageReference::for_object(page.page_index, &obj.input).map_err(protocol)?);
        index += 1;
    }
    let root = ClientRoot {
        version: codec::CLIENT_VERSION,
        begin_sha256: state.begin.input_sha256().map_err(protocol)?,
        descriptor_sha256: state.begin.descriptor.commitment().map_err(protocol)?,
        native_root: state.native_root.clone(),
        recovery: state.recovery.clone(),
        native_file_count: state.native_files,
        native_bytes: bytes,
        pages,
        run: state.run.clone(),
    };
    root.validate_for(&state.begin)?;
    freeze(inner, state, key, index, &codec::encode_metadata(&root)?)?;
    if state.objects != index + 1 {
        return Err(invalid("byte frozen source object total differs"));
    }
    Ok(())
}

fn freeze(
    inner: &Inner,
    state: &mut State,
    key: &[u8; 32],
    index: u64,
    plain: &[u8],
) -> StorageResult<ObjectRecord> {
    let name = format!("payload-{index:08}.enc");
    let prev = inner
        .store
        .exists(&name)?
        .then(|| {
            inner
                .store
                .read_bytes(&name, codec::CODEC_ENVELOPE_MAX_BYTES)
        })
        .transpose()?;
    let (object, bytes, fresh) = codec::seal_envelope(key, plain, prev).map_err(invalid)?;
    if fresh {
        inner.store.write_raw(&name, &bytes)?;
    }
    save(inner, state, index, object, Vec::new(), Some(name))
}

impl From<PlanError> for StorageError {
    fn from(error: PlanError) -> Self {
        invalid(&error.to_string())
    }
}
