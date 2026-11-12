//! Node home creation, format admission, and v2 run home opening.

use super::*;
use crate::storage::content_key_proof;
use crate::storage::format::control::{validate_frozen_replay_floor, DELETE_REPLAY_CUTOFF_KEY};
use crate::storage::format::metadata::{
    initial_local_author, validate_local_author, FILE_RETENTION_COMPLETE, FILE_RETENTION_KEY,
    LOCAL_AUTHOR_KEY,
};
use crate::storage::format::{
    CAS_KEY_LABEL, CREATION_INTENT as INTENT, CURRENT_KEY_FORMAT_KEY, CURRENT_KEY_FORMAT_VALUE,
    HOME_KEY_LABEL, MAIN_NAMESPACE, METADATA_NAMESPACE, RECORDS_COLLECTION, RECORD_PROFILE,
    STAGED_NODE_MARKER as STAGED_MARKER,
};
use lastdb_model::field_key_codec::current_key_format;
use laststore::disk_run::home::{ReadBudget, RunHome, RunRead};
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::os::unix::fs::OpenOptionsExt;

pub use crate::storage::format::{CAS_DIR, RUN_DIR};

pub(in crate::storage) fn profile(bytes: &[u8]) -> Option<u16> {
    (bytes == RECORD_NODE_BYTES).then_some(RECORD_PROFILE)
}

fn policy_from_env() -> StorageResult<SpacePolicy> {
    let mut policy = SpacePolicy::default();
    if let Some(raw) = std::env::var_os("LASTDB_DISK_CAP_BYTES") {
        policy.disk_cap_bytes = raw
            .to_str()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|&n| n > 0)
            .ok_or_else(|| {
                StorageError::InvalidOperation(
                    "LASTDB_DISK_CAP_BYTES must be a positive integer byte count".into(),
                )
            })?;
    }
    Ok(policy)
}

fn admit(root: &Path, key: &[u8; 32]) -> StorageResult<u16> {
    let marker = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(root.join(NODE_MARKER))?;
    if !marker.metadata()?.is_file() {
        return Err(admission_invalid("the node marker is not a regular file"));
    }
    let mut bytes = Vec::new();
    marker.take(129).read_to_end(&mut bytes)?;
    crate::storage::retired::refuse_shared_home(root, &bytes)?;
    let version =
        profile(&bytes).ok_or_else(|| admission_invalid("the node range format is unsupported"))?;
    let b = ReadBudget {
        pages: 16384,
        blocks: 65536,
        bytes: 8 << 20,
        keys: 64,
    };
    let mut main = RunRead::open_existing(&root.join(RUN_DIR), &home_key(key), b).map_err(map)?;
    RunRead::open_existing(&root.join(CAS_DIR), &cas_key(key), b).map_err(map)?;
    verify_proofs(&mut main, key)?;
    crate::storage::retired::refuse_journal(&mut main)?;
    Ok(version)
}

fn verify_proofs(main: &mut RunRead, key: &[u8; 32]) -> StorageResult<()> {
    let enc = native_routing::encode;
    let coords = [
        (RECORDS_COLLECTION.into(), CURRENT_KEY_FORMAT_KEY.into()),
        (RECORDS_COLLECTION.into(), DELETE_REPLAY_CUTOFF_KEY.into()),
        (
            content_key_proof::NAMESPACE.into(),
            enc(content_key_proof::KEY),
        ),
        (METADATA_NAMESPACE.into(), enc(LOCAL_AUTHOR_KEY.as_bytes())),
    ];
    let [fmt, floor, proof, author]: [Option<Vec<u8>>; 4] = main
        .get_many_bounded(&coords, 8192)
        .map_err(map)?
        .into_iter()
        .collect::<laststore::Result<Vec<_>>>()
        .map_err(map)?
        .try_into()
        .map_err(|_| admission_invalid("the account/format proof batch is incomplete"))?;
    let marker = fmt
        .as_deref()
        .ok_or_else(|| admission_invalid("the logical format is absent"))?;
    if !current_key_format(marker) {
        return Err(admission_invalid(
            "the logical format differs from the node marker",
        ));
    }
    validate_frozen_replay_floor(Some(marker), floor.as_deref())?;
    validate_local_author(author.as_deref())?;
    let p = proof
        .as_deref()
        .ok_or_else(|| admission_invalid("the account proof is absent"))?;
    content_key_proof::verify(key, p)
}

fn admission_invalid(text: &str) -> StorageError {
    StorageError::InvalidOperation(format!("node range admission: {text}"))
}

fn is_new_home(path: &Path) -> StorageResult<bool> {
    match fs::read_dir(path) {
        Ok(mut d) => Ok(d.next().is_none()),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(true),
        Err(e) => Err(e.into()),
    }
}

fn creation_stopped(path: &Path) -> StorageResult<bool> {
    Ok(path.join(INTENT).try_exists()? && !path.join(NODE_MARKER).try_exists()?)
}

fn clear_stopped_creation(path: &Path) -> StorageResult<()> {
    let mut partial = Vec::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let (kind, file_name) = (entry.file_type()?, entry.file_name());
        let name = file_name.to_str().unwrap_or_default();
        if kind.is_file() && name == INTENT {
            continue;
        }
        let run_home = [RUN_DIR, CAS_DIR].into_iter().any(|h| {
            name == h
                || name
                    .strip_prefix('.')
                    .and_then(|r| r.strip_prefix(h))
                    .is_some_and(|r| r.starts_with(".create-"))
        });
        if (kind.is_dir() && run_home) || (kind.is_file() && name == STAGED_MARKER) {
            partial.push((entry.path(), kind.is_dir()));
        } else {
            return Err(StorageError::InvalidOperation(format!(
                "a format v2 node home creation stopped beside the unexpected entry {}; the home \
                 holds no data, so move that entry away and start the node again",
                file_name.to_string_lossy()
            )));
        }
    }
    for (p, is_dir) in partial {
        if is_dir {
            fs::remove_dir_all(&p)?;
        } else {
            fs::remove_file(&p)?;
        }
    }
    Ok(())
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
        .expect("32 bytes is a valid HKDF-SHA256 output length");
    key
}

fn require_key(node_key: Option<&[u8; 32]>) -> StorageResult<&[u8; 32]> {
    node_key.ok_or_else(|| {
        StorageError::InvalidOperation(
            "a format v2 node home needs the node encryption key; every new node home is v2".into(),
        )
    })
}

fn initialize_home(home: &RunHome, node_key: &[u8; 32]) -> StorageResult<()> {
    let ops = [
        (
            MAIN_NAMESPACE,
            CURRENT_KEY_FORMAT_KEY.as_bytes(),
            CURRENT_KEY_FORMAT_VALUE.to_vec(),
        ),
        (
            content_key_proof::NAMESPACE,
            content_key_proof::KEY,
            content_key_proof::create(node_key)?,
        ),
        (
            METADATA_NAMESPACE,
            LOCAL_AUTHOR_KEY.as_bytes(),
            initial_local_author(home.identity())?,
        ),
        (
            METADATA_NAMESPACE,
            FILE_RETENTION_KEY.as_bytes(),
            FILE_RETENTION_COMPLETE.to_vec(),
        ),
    ]
    .into_iter()
    .map(|(ns, k, body)| {
        let (collection, key) = kv::coordinate(ns, k);
        HomeMutation {
            collection,
            key,
            body: Some(body),
        }
    })
    .collect();
    home.commit(ops).map(|_| ()).map_err(map)
}

impl PackRuntime {
    pub(in crate::storage::disk_pack) async fn ensure_namespace(
        self: &Arc<Self>,
        name: &str,
    ) -> StorageResult<()> {
        let key = format!("{NAMESPACES}{name}");
        let ctx = read::ReadContext::current(self).await?;
        let pending = transaction::PendingBatch::current(self);
        let coords = [(REGISTRY.to_string(), key.clone())];
        if self
            .raw_many(&ctx, &coords, pending.as_ref())?
            .pop()
            .flatten()
            .is_none()
        {
            self.mutate(vec![HomeMutation {
                collection: REGISTRY.into(),
                key,
                body: Some(vec![1]),
            }])
            .await?;
        }
        Ok(())
    }
}

impl PackNamespacedStore {
    pub fn open_keyed(
        path: &Path,
        node_key: Option<&[u8; 32]>,
        capture_accepted: bool,
    ) -> StorageResult<Self> {
        if is_new_home(path)? || creation_stopped(path)? {
            return Self::create_run(path, require_key(node_key)?, capture_accepted);
        }
        Self::open_run(path, node_key, capture_accepted)
    }

    pub(crate) async fn open_namespace(&self, name: &str) -> StorageResult<PackKvStore> {
        if name.len() > 65531 || name == REGISTRY {
            return Err(StorageError::InvalidOperation(
                "namespace exceeds format limit or uses the reserved registry identity".into(),
            ));
        }
        if read::ReadContext::captured(&self.runtime).is_none() {
            self.runtime.ensure_namespace(name).await?;
        }
        Ok(PackKvStore {
            runtime: Arc::clone(&self.runtime),
            namespace: name.to_string(),
        })
    }

    fn create_run(path: &Path, node_key: &[u8; 32], capture_accepted: bool) -> StorageResult<Self> {
        fs::create_dir_all(path)?;
        let folder = File::open(path)?;
        folder.try_lock().map_err(|e| {
            StorageError::InvalidOperation(format!(
                "another owner is creating this format v2 node home or its lock failed: {e}"
            ))
        })?;
        if !path.join(NODE_MARKER).try_exists()? {
            if creation_stopped(path)? {
                clear_stopped_creation(path)?;
            } else if fs::read_dir(path)?.next().is_some() {
                return Err(StorageError::InvalidOperation(
                    "format v2 creation requires an empty node home".into(),
                ));
            }
            drop(File::create(path.join(INTENT))?);
            folder.sync_all()?;
            let main = RunHome::create(&path.join(RUN_DIR), &home_key(node_key)).map_err(map)?;
            drop(RunHome::create(&path.join(CAS_DIR), &cas_key(node_key)).map_err(map)?);
            initialize_home(&main, node_key)?;
            drop(main);
            let staged = path.join(STAGED_MARKER);
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
        admit(path, node_key)?;
        let home = Arc::new(RunHome::open(&path.join(RUN_DIR), &home_key(node_key)).map_err(map)?);
        let cas = Arc::new(RunHome::open(&path.join(CAS_DIR), &cas_key(node_key)).map_err(map)?);
        let mut physical = physical::V2Home::new(home, cas);
        physical.cas_delete_recovery = super::cas_delete::recover(&physical)?;
        let current = physical.current_root();
        let cleanup_job = Arc::new(CleanupJob::open(&physical));
        let mutation_log = mutation_log::MutationLog::new(&physical);
        let accepted = capture_accepted.then(|| {
            crate::storage::disk_backup::accepted::Log::open(physical.path(), current.frontier())
        });
        let runtime = Arc::new(PackRuntime {
            physical,
            cleanup_job,
            mutation_log,
            accepted,
            view: Arc::new(RwLock::new(view::NativeReadView {
                current,
                revision: 0,
            })),
            prepare_gate: Arc::default(),
            write_gate: Arc::default(),
            queue: Mutex::new(queue::WriteQueue::default()),
            completed: tokio::sync::watch::channel(0).0,
            core: Arc::new(Mutex::new(CoreWarmSet::default())),
            failed: AtomicBool::new(false),
            serving: AtomicBool::new(false),
            initializing: AtomicBool::new(path.join(INTENT).try_exists()?),
            failed_write: Mutex::new(None),
            space_policy: policy_from_env()?,
        });
        runtime.resume_merges();
        Ok(Self { runtime })
    }
}
