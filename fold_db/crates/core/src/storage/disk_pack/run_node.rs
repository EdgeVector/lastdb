//! Node home creation, `O_NOFOLLOW` file helpers, format admission, and v2 run home opening.

use super::{
    cas_delete, kv, map, native_routing, physical, NativeReadView, PackNamespacedStore,
    PackRuntime, StorageError, StorageResult,
};
use crate::storage::content_key_proof;
use crate::storage::current_keys::{JOURNAL_RETIRED, META_PFX};
use crate::storage::disk_backup::accepted::Log;
use crate::storage::disk_cleanup::CleanupJob;
use crate::storage::format::metadata::{
    initial_local_author, validate_local_author, LOCAL_AUTHOR_KEY,
};
use crate::storage::format::{
    CAS_KEY_LABEL, CREATION_INTENT, CURRENT_KEY_FORMAT_KEY, CURRENT_KEY_FORMAT_VALUE,
    HOME_KEY_LABEL, MAIN_NAMESPACE, METADATA_NAMESPACE, NODE_MARKER, RECORDS_COLLECTION,
    RECORD_NODE_BYTES, RECORD_PROFILE, RETIRED_SHARED_DIR, RETIRED_SHARED_NODE_BYTES,
    STAGED_NODE_MARKER,
};
use lastdb_model::field_key_codec::current_key_format;
use lastdb_model::kind_partition::exact_prefix_bounds;
use laststore::disk_run::home::{HomeMutation, ReadBudget, RunHome, RunRead};
use laststore::space::SpacePolicy;
use std::fs::{self, DirBuilder, File, Metadata, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, RwLock};

pub use crate::storage::format::{CAS_DIR, RUN_DIR};

pub(in crate::storage) fn is_private_meta(meta: &Metadata) -> bool {
    meta.uid() == unsafe { libc::geteuid() } && meta.mode() & 0o077 == 0
}

pub(in crate::storage) fn private_dir_status(path: &Path) -> std::io::Result<Option<bool>> {
    match fs::symlink_metadata(path) {
        Ok(info) => Ok(Some(info.is_dir() && is_private_meta(&info))),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

pub(in crate::storage) fn create_private_dir(path: &Path) -> std::io::Result<()> {
    DirBuilder::new().mode(0o700).create(path)
}

pub(in crate::storage) fn open_nofollow_read(path: &Path) -> std::io::Result<File> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
}

pub(in crate::storage) fn open_private_write(
    path: &Path,
    create_new: bool,
) -> std::io::Result<File> {
    let mut opts = OpenOptions::new();
    opts.write(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    if create_new {
        opts.create_new(true);
    } else {
        opts.create(true).truncate(true);
    }
    opts.open(path)
}

fn bad_admission(text: &str) -> StorageError {
    StorageError::invalid(format!("node range admission: {text}"))
}

fn refuse_shared_home(root: &Path, marker: &[u8]) -> StorageResult<()> {
    let has_shared_dir =
        fs::symlink_metadata(root.join(RETIRED_SHARED_DIR)).is_ok_and(|meta| meta.is_dir());
    if marker == RETIRED_SHARED_NODE_BYTES || has_shared_dir {
        return Err(StorageError::RetiredFormat(
            "the dedicated shared home format is retired",
        ));
    }
    Ok(())
}

fn refuse_journal(main: &mut RunRead) -> StorageResult<()> {
    let prefixes = META_PFX.iter().map(|&prefix| (METADATA_NAMESPACE, prefix));
    for (namespace, prefix) in prefixes.chain([(MAIN_NAMESPACE, "ade:")]) {
        let (_, end) = exact_prefix_bounds(prefix);
        let collection = native_routing::collection_for_namespace(namespace);
        let mut range = main
            .administrative_range(collection, prefix.as_bytes(), end.as_bytes())
            .map_err(map)?;
        if !range.next_page(1).map_err(map)?.records.is_empty() {
            return Err(StorageError::RetiredFormat(JOURNAL_RETIRED));
        }
    }
    Ok(())
}

fn read_stopped_node_profile(root: &Path) -> StorageResult<u16> {
    let marker = open_nofollow_read(&root.join(NODE_MARKER))?;
    if !marker.metadata()?.is_file() {
        return Err(bad_admission("the node marker is not a regular file"));
    }
    let mut bytes = Vec::new();
    marker.take(129).read_to_end(&mut bytes)?;
    refuse_shared_home(root, &bytes)?;
    (bytes.as_slice() == RECORD_NODE_BYTES)
        .then_some(RECORD_PROFILE)
        .ok_or_else(|| bad_admission("the node range format is unsupported"))
}

pub(in crate::storage) fn admit_stopped_home(root: &Path, key: &[u8; 32]) -> StorageResult<u16> {
    let version = read_stopped_node_profile(root)?;
    let budget = ReadBudget::new(8 << 20, 64);
    let mut main =
        RunRead::open_existing(&root.join(RUN_DIR), &home_key(key), budget).map_err(map)?;
    RunRead::open_existing(&root.join(CAS_DIR), &cas_key(key), budget).map_err(map)?;
    let coords = [
        (RECORDS_COLLECTION, CURRENT_KEY_FORMAT_KEY.as_bytes()),
        (METADATA_NAMESPACE, LOCAL_AUTHOR_KEY.as_bytes()),
        (content_key_proof::NAMESPACE, content_key_proof::KEY),
    ]
    .map(|(collection, raw_key)| (collection.to_owned(), native_routing::encode(raw_key)));
    let rows = main
        .get_many_bounded(&coords, 8192)
        .map_err(map)?
        .into_iter()
        .collect::<laststore::Result<Vec<_>>>()
        .map_err(map)?;
    let [fmt, author, proof]: [Option<Vec<u8>>; 3] = rows
        .try_into()
        .map_err(|_| bad_admission("the account/format proof batch is incomplete"))?;
    let marker = fmt
        .as_deref()
        .ok_or_else(|| bad_admission("the logical format is absent"))?;
    if version != RECORD_PROFILE || !current_key_format(marker) {
        return Err(bad_admission(
            "the logical format differs from the node marker",
        ));
    }
    validate_local_author(author.as_deref())?;
    let stored_proof = proof
        .as_deref()
        .ok_or_else(|| bad_admission("the account proof is absent"))?;
    content_key_proof::verify(key, stored_proof)?;
    refuse_journal(&mut main).map(|()| version)
}

pub(in crate::storage) fn home_key(node_key: &[u8; 32]) -> zeroize::Zeroizing<[u8; 32]> {
    derive_key(node_key, HOME_KEY_LABEL)
}

pub(in crate::storage) fn cas_key(node_key: &[u8; 32]) -> zeroize::Zeroizing<[u8; 32]> {
    derive_key(node_key, CAS_KEY_LABEL)
}

fn derive_key(node_key: &[u8; 32], label: &[u8]) -> zeroize::Zeroizing<[u8; 32]> {
    let mut key = zeroize::Zeroizing::new([0u8; 32]);
    hkdf::Hkdf::<sha2::Sha256>::new(None, node_key)
        .expand(label, key.as_mut())
        .expect("HKDF");
    key
}

fn creation_stopped(path: &Path) -> StorageResult<bool> {
    Ok(path.join(CREATION_INTENT).try_exists()? && !path.join(NODE_MARKER).try_exists()?)
}

fn require_key(node_key: Option<&[u8; 32]>) -> StorageResult<&[u8; 32]> {
    node_key.ok_or_else(|| StorageError::invalid("a format v2 node home needs the encryption key"))
}

fn initialize_home(home: &RunHome, node_key: &[u8; 32]) -> StorageResult<()> {
    let author = initial_local_author(home.identity())?;
    let proof = content_key_proof::create(node_key)?;
    let items = [
        (
            MAIN_NAMESPACE,
            CURRENT_KEY_FORMAT_KEY.as_bytes(),
            CURRENT_KEY_FORMAT_VALUE.to_vec(),
        ),
        (content_key_proof::NAMESPACE, content_key_proof::KEY, proof),
        (METADATA_NAMESPACE, LOCAL_AUTHOR_KEY.as_bytes(), author),
    ];
    let ops = items
        .into_iter()
        .map(|(ns, k, body)| {
            let (col, key) = kv::coordinate(ns, k);
            HomeMutation::new(col, key, Some(body))
        })
        .collect();
    home.commit(ops).map_err(map)
}

fn clean_stopped_creation(path: &Path) -> StorageResult<()> {
    let mut partial = Vec::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let (kind, fname) = (entry.file_type()?, entry.file_name());
        let name = fname.to_str().unwrap_or_default();
        if kind.is_file() && name == CREATION_INTENT {
            continue;
        }
        let is_tmp = |home| {
            name.strip_prefix('.')
                .and_then(|rest| rest.strip_prefix(home))
                .is_some_and(|rest| rest.starts_with(".create-"))
        };
        let run_home = [RUN_DIR, CAS_DIR]
            .into_iter()
            .any(|h| name == h || is_tmp(h));
        if (kind.is_dir() && run_home) || (kind.is_file() && name == STAGED_NODE_MARKER) {
            partial.push((entry.path(), kind.is_dir()));
        } else {
            let lossy = fname.to_string_lossy();
            return Err(StorageError::invalid(format!(
                "creation stopped beside unexpected entry {lossy}; move it away and retry"
            )));
        }
    }
    for (entry_path, is_dir) in partial {
        if is_dir {
            fs::remove_dir_all(&entry_path)?;
        } else {
            fs::remove_file(&entry_path)?;
        }
    }
    Ok(())
}

impl PackNamespacedStore {
    pub(crate) fn open_keyed(
        path: &Path,
        key: Option<&[u8; 32]>,
        capture_accepted: bool,
    ) -> StorageResult<Self> {
        let is_new = match fs::read_dir(path) {
            Ok(mut dir) => dir.next().is_none(),
            Err(e) if e.kind() == ErrorKind::NotFound => true,
            Err(e) => return Err(e.into()),
        };
        if is_new || creation_stopped(path)? {
            return Self::create_run(path, require_key(key)?, capture_accepted);
        }
        Self::open_run(path, key, capture_accepted)
    }

    fn create_run(path: &Path, node_key: &[u8; 32], capture_accepted: bool) -> StorageResult<Self> {
        fs::create_dir_all(path)?;
        let folder = File::open(path)?;
        folder.try_lock().map_err(|e| {
            StorageError::invalid(format!(
                "another owner is creating this home or lock failed: {e}"
            ))
        })?;
        if !path.join(NODE_MARKER).try_exists()? {
            if creation_stopped(path)? {
                clean_stopped_creation(path)?;
            } else if fs::read_dir(path)?.next().is_some() {
                return Err(StorageError::invalid(
                    "format v2 creation requires an empty node home",
                ));
            }
            drop(File::create(path.join(CREATION_INTENT))?);
            folder.sync_all()?;
            let main = RunHome::create(&path.join(RUN_DIR), &home_key(node_key)).map_err(map)?;
            drop(RunHome::create(&path.join(CAS_DIR), &cas_key(node_key)).map_err(map)?);
            initialize_home(&main, node_key)?;
            drop(main);
            let staged = path.join(STAGED_NODE_MARKER);
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&staged)?;
            file.write_all(RECORD_NODE_BYTES)?;
            file.sync_all()?;
            fs::rename(&staged, path.join(NODE_MARKER))?;
            folder.sync_all()?;
        }
        let store = Self::open_run(path, Some(node_key), capture_accepted);
        drop(folder);
        store
    }

    fn open_run(
        path: &Path,
        node_key: Option<&[u8; 32]>,
        capture_accepted: bool,
    ) -> StorageResult<Self> {
        let node_key = require_key(node_key)?;
        admit_stopped_home(path, node_key)?;
        let mut space_policy = SpacePolicy::default();
        if let Some(raw) = std::env::var_os("LASTDB_DISK_CAP_BYTES") {
            let parsed = raw
                .to_str()
                .and_then(|s| s.parse::<u64>().ok())
                .filter(|&n| n > 0);
            space_policy.disk_cap_bytes = parsed
                .ok_or_else(|| StorageError::invalid("LASTDB_DISK_CAP_BYTES must be positive"))?;
        }
        let home = Arc::new(RunHome::open(&path.join(RUN_DIR), &home_key(node_key)).map_err(map)?);
        let cas = Arc::new(RunHome::open(&path.join(CAS_DIR), &cas_key(node_key)).map_err(map)?);
        let mut physical = physical::V2Home::new(home, cas);
        physical.cas_delete_recovery = cas_delete::recover(&physical)?;
        let current = physical.current_root();
        let cleanup_job = Arc::new(CleanupJob::open(&physical));
        let accepted = capture_accepted.then(|| Log::open(physical.path(), current.frontier()));
        let initializing = AtomicBool::new(path.join(CREATION_INTENT).try_exists()?);
        let view = NativeReadView {
            root: current,
            revision: 0,
        };
        let runtime = Arc::new(PackRuntime {
            physical,
            cleanup_job,
            accepted,
            local_file_refs_lock: Mutex::default(),
            view: Arc::new(RwLock::new(view)),
            prepare_gate: Arc::default(),
            write_gate: Arc::default(),
            queue: Mutex::default(),
            completed: tokio::sync::watch::channel(0).0,
            core: Arc::default(),
            failed: AtomicBool::new(false),
            serving: AtomicBool::new(false),
            initializing,
            failed_write: Mutex::new(None),
            space_policy,
        });
        runtime.spawn_merges();
        Ok(Self { runtime })
    }
}
