//! Node home creation, format admission, v2 run home opening, and warm-record admission.

use super::{
    cas_delete, kv, map, mutation_log, native_routing, physical, NativeReadView,
    PackNamespacedStore, PackRuntime, StorageError, StorageResult,
};
use crate::storage::content_key_proof;
use crate::storage::disk_cleanup::CleanupJob;
use crate::storage::format::control::{validate_frozen_replay_floor, DELETE_REPLAY_CUTOFF_KEY};
use crate::storage::format::metadata::{
    initial_local_author, validate_local_author, FILE_RETENTION_COMPLETE, FILE_RETENTION_KEY,
    LOCAL_AUTHOR_KEY,
};
use crate::storage::format::{
    CAS_KEY_LABEL, CREATION_INTENT, CURRENT_KEY_FORMAT_KEY, CURRENT_KEY_FORMAT_VALUE,
    HOME_KEY_LABEL, MAIN_NAMESPACE, METADATA_NAMESPACE, NODE_MARKER, RECORDS_COLLECTION,
    RECORD_NODE_BYTES, RECORD_PROFILE, STAGED_NODE_MARKER,
};
use lastdb_model::field_key_codec::current_key_format;
use laststore::disk_run::home::{HomeMutation, ReadBudget, RunHome, RunRead};
use laststore::space::SpacePolicy;
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, RwLock};

pub use crate::storage::format::{CAS_DIR, RUN_DIR};

pub(in crate::storage) fn profile(bytes: &[u8]) -> Option<u16> {
    (bytes == RECORD_NODE_BYTES).then_some(RECORD_PROFILE)
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

fn admit(root: &Path, key: &[u8; 32]) -> StorageResult<u16> {
    let bad = |t: &str| StorageError::invalid(format!("node range admission: {t}"));
    let flags = libc::O_NOFOLLOW | libc::O_NONBLOCK;
    let marker = OpenOptions::new()
        .read(true)
        .custom_flags(flags)
        .open(root.join(NODE_MARKER))?;
    if !marker.metadata()?.is_file() {
        return Err(bad("the node marker is not a regular file"));
    }
    let mut bytes = Vec::new();
    marker.take(129).read_to_end(&mut bytes)?;
    crate::storage::retired::refuse_shared_home(root, &bytes)?;
    let version = profile(&bytes).ok_or_else(|| bad("the node range format is unsupported"))?;
    let b = ReadBudget::new(16384, 65536, 8 << 20, 64);
    let mut main = RunRead::open_existing(&root.join(RUN_DIR), &home_key(key), b).map_err(map)?;
    RunRead::open_existing(&root.join(CAS_DIR), &cas_key(key), b).map_err(map)?;
    let coords = [
        (RECORDS_COLLECTION, CURRENT_KEY_FORMAT_KEY.as_bytes()),
        (RECORDS_COLLECTION, DELETE_REPLAY_CUTOFF_KEY.as_bytes()),
        (content_key_proof::NAMESPACE, content_key_proof::KEY),
        (METADATA_NAMESPACE, LOCAL_AUTHOR_KEY.as_bytes()),
    ]
    .map(|(c, k)| (c.to_owned(), native_routing::encode(k)));
    let rows = main
        .get_many_bounded(&coords, 8192)
        .map_err(map)?
        .into_iter()
        .collect::<laststore::Result<Vec<_>>>()
        .map_err(map)?;
    let [fmt, floor, proof, author]: [Option<Vec<u8>>; 4] = rows
        .try_into()
        .map_err(|_| bad("the account/format proof batch is incomplete"))?;
    let m = fmt
        .as_deref()
        .ok_or_else(|| bad("the logical format is absent"))?;
    if !current_key_format(m) {
        return Err(bad("the logical format differs from the node marker"));
    }
    validate_frozen_replay_floor(Some(m), floor.as_deref())?;
    validate_local_author(author.as_deref())?;
    let p = proof
        .as_deref()
        .ok_or_else(|| bad("the account proof is absent"))?;
    content_key_proof::verify(key, p)?;
    crate::storage::retired::refuse_journal(&mut main).map(|()| version)
}

fn creation_stopped(path: &Path) -> StorageResult<bool> {
    Ok(path.join(CREATION_INTENT).try_exists()? && !path.join(NODE_MARKER).try_exists()?)
}

fn require_key(node_key: Option<&[u8; 32]>) -> StorageResult<&[u8; 32]> {
    node_key.ok_or_else(|| StorageError::invalid("a format v2 node home needs the encryption key"))
}

fn initialize_home(home: &RunHome, node_key: &[u8; 32]) -> StorageResult<()> {
    let (author, proof) = (
        initial_local_author(home.identity())?,
        content_key_proof::create(node_key)?,
    );
    let (fmt_k, fmt_v) = (
        CURRENT_KEY_FORMAT_KEY.as_bytes(),
        CURRENT_KEY_FORMAT_VALUE.to_vec(),
    );
    let (ret_k, ret_v) = (
        FILE_RETENTION_KEY.as_bytes(),
        FILE_RETENTION_COMPLETE.to_vec(),
    );
    let items = [
        (MAIN_NAMESPACE, fmt_k, fmt_v),
        (content_key_proof::NAMESPACE, content_key_proof::KEY, proof),
        (METADATA_NAMESPACE, LOCAL_AUTHOR_KEY.as_bytes(), author),
        (METADATA_NAMESPACE, ret_k, ret_v),
    ];
    let to_op = |(ns, k, b)| {
        let (col, key) = kv::coordinate(ns, k);
        HomeMutation::new(col, key, Some(b))
    };
    home.commit(items.into_iter().map(to_op).collect())
        .map(|_| ())
        .map_err(map)
}

impl PackNamespacedStore {
    pub fn open_keyed(p: &Path, key: Option<&[u8; 32]>, cap: bool) -> StorageResult<Self> {
        let is_new = match fs::read_dir(p) {
            Ok(mut d) => d.next().is_none(),
            Err(e) if e.kind() == ErrorKind::NotFound => true,
            Err(e) => return Err(e.into()),
        };
        if is_new || creation_stopped(p)? {
            return Self::create_run(p, require_key(key)?, cap);
        }
        Self::open_run(p, key, cap)
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
                let mut partial = Vec::new();
                for entry in fs::read_dir(path)? {
                    let entry = entry?;
                    let (kind, fname) = (entry.file_type()?, entry.file_name());
                    let name = fname.to_str().unwrap_or_default();
                    if kind.is_file() && name == CREATION_INTENT {
                        continue;
                    }
                    let is_tmp = |h| {
                        name.strip_prefix('.')
                            .and_then(|r| r.strip_prefix(h))
                            .is_some_and(|r| r.starts_with(".create-"))
                    };
                    let run_home = [RUN_DIR, CAS_DIR]
                        .into_iter()
                        .any(|h| name == h || is_tmp(h));
                    if (kind.is_dir() && run_home) || (kind.is_file() && name == STAGED_NODE_MARKER)
                    {
                        partial.push((entry.path(), kind.is_dir()));
                    } else {
                        let lossy = fname.to_string_lossy();
                        return Err(StorageError::invalid(format!(
                            "creation stopped beside unexpected entry {lossy}; move it away and retry"
                        )));
                    }
                }
                for (p, dir) in partial {
                    if dir {
                        fs::remove_dir_all(&p)?;
                    } else {
                        fs::remove_file(&p)?;
                    }
                }
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

    fn open_run(path: &Path, node_key: Option<&[u8; 32]>, cap: bool) -> StorageResult<Self> {
        let node_key = require_key(node_key)?;
        admit(path, node_key)?;
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
        let mutation_log = mutation_log::MutationLog::new(&physical);
        let accepted = cap.then(|| {
            crate::storage::disk_backup::accepted::Log::open(physical.path(), current.frontier())
        });
        let initializing = AtomicBool::new(path.join(CREATION_INTENT).try_exists()?);
        let runtime = Arc::new(PackRuntime {
            physical,
            cleanup_job,
            mutation_log,
            accepted,
            view: Arc::new(RwLock::new(NativeReadView(current, 0))),
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

pub(crate) mod record_warm {
    pub(crate) use super::super::read::WarmRecordRead;
    use super::super::read::{unpoison, ReadContext};
    use super::super::transaction::PendingBatch;
    use super::*;
    use crate::resident::{core_warm_bytes, RecordCoordinate};
    use lastdb_model::record_value::StoredRecord;
    use std::collections::HashMap;
    use std::hash::{BuildHasher, BuildHasherDefault, DefaultHasher};

    pub(crate) struct WrittenRecord(RecordCoordinate, Arc<StoredRecord>, usize, u64);

    impl WrittenRecord {
        pub(crate) fn new(key: &str, record: Arc<StoredRecord>, val: &[u8]) -> Self {
            Self(
                kv::coordinate(MAIN_NAMESPACE, key.as_bytes()),
                record,
                val.len(),
                digest(val),
            )
        }
    }

    impl PackRuntime {
        pub(crate) fn warm_records<K: AsRef<str>>(
            &self,
            keys: &[K],
        ) -> StorageResult<Option<WarmRecordRead>> {
            if PendingBatch::current(self).is_some() {
                return Ok(None);
            }
            let view = self.view.read().expect("native record warm view");
            let ctx = ReadContext::captured(self);
            if ctx.as_ref().is_some_and(|c| c.2 != view.1) {
                return Ok(None);
            }
            let coords: Vec<_> = keys
                .iter()
                .map(|k| kv::coordinate(MAIN_NAMESPACE, k.as_ref().as_bytes()))
                .collect();
            let mut call = ctx.as_ref().map(|c| unpoison(&c.0));
            let found = self
                .core
                .lock()
                .expect("native record warm set")
                .record_lookup(&coords);
            if let (Some(ctx), Some(call)) = (&ctx, &mut call) {
                for (k, h) in keys.iter().zip(&found) {
                    if let Some((_, b)) = h {
                        ctx.account_memory_record(call, k.as_ref().len(), *b)?;
                    }
                }
            }
            let recs = found.into_iter().map(|h| h.map(|(r, _)| r)).collect();
            Ok(Some(WarmRecordRead(view.1, coords, recs)))
        }

        pub(crate) fn stage_written_records(&self, records: Vec<WrittenRecord>) {
            if let Some(pending) = PendingBatch::current(self) {
                let limit = core_warm_bytes() / 4;
                let mut staged = pending.warm_records.lock().expect("native warm records");
                for r in records {
                    let Some(b) = staged.1.checked_add(r.2).filter(|&b| b <= limit) else {
                        break;
                    };
                    staged.1 = b;
                    staged.0.push(r);
                }
            }
        }
    }

    pub(in crate::storage::disk_pack) fn committed_records(
        requests: &[Arc<PendingBatch>],
    ) -> Vec<(RecordCoordinate, Arc<StoredRecord>, usize)> {
        let mut latest: HashMap<RecordCoordinate, (Arc<StoredRecord>, usize)> = HashMap::new();
        for req in requests {
            let staged =
                std::mem::take(&mut *req.warm_records.lock().expect("native warm records")).0;
            if latest.is_empty() && staged.is_empty() {
                continue;
            }
            req.changed_coordinates()
                .iter()
                .for_each(|c| drop(latest.remove(c)));
            for WrittenRecord(coord, rec, bytes, dig) in staged {
                let ok = req.lookup(&coord.0, &coord.1).is_some_and(|p| {
                    p.body
                        .as_deref()
                        .is_some_and(|b| b.len() == bytes && digest(b) == dig)
                });
                if ok {
                    latest.insert(coord, (rec, bytes));
                }
            }
        }
        latest.into_iter().map(|(c, (r, b))| (c, r, b)).collect()
    }

    fn digest(value: &[u8]) -> u64 {
        BuildHasherDefault::<DefaultHasher>::default().hash_one(value)
    }
}
