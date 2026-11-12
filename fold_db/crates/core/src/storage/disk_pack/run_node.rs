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
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::os::unix::fs::OpenOptionsExt;

pub use crate::storage::format::{CAS_DIR, RUN_DIR};

/// The backup profile version of a node marker. Only the current marker has one.
pub(in crate::storage) fn profile(bytes: &[u8]) -> Option<u16> {
    (bytes == RECORD_NODE_BYTES).then_some(RECORD_PROFILE)
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
    let admit_budget = ReadBudget {
        pages: 16384,
        blocks: 65536,
        bytes: 8 * 1024 * 1024,
        keys: 64,
    };
    let mut main =
        RunRead::open_existing(&root.join(RUN_DIR), &home_key(key), admit_budget).map_err(map)?;
    let _cas =
        RunRead::open_existing(&root.join(CAS_DIR), &cas_key(key), admit_budget).map_err(map)?;
    verify_proofs(&mut main, key)?;
    crate::storage::retired::refuse_journal(&mut main)?;
    Ok(version)
}

fn verify_proofs(main: &mut RunRead, key: &[u8; 32]) -> StorageResult<()> {
    let coordinates = vec![
        (RECORDS_COLLECTION.into(), CURRENT_KEY_FORMAT_KEY.into()),
        (RECORDS_COLLECTION.into(), DELETE_REPLAY_CUTOFF_KEY.into()),
        (
            content_key_proof::NAMESPACE.into(),
            native_routing::encode(content_key_proof::KEY),
        ),
        (
            METADATA_NAMESPACE.into(),
            native_routing::encode(LOCAL_AUTHOR_KEY.as_bytes()),
        ),
    ];
    let values = main
        .get_many_bounded(&coordinates, 8192)
        .map_err(map)?
        .into_iter()
        .collect::<laststore::Result<Vec<_>>>()
        .map_err(map)?;
    if values.len() != coordinates.len() {
        return Err(admission_invalid(
            "the account/format proof batch is incomplete",
        ));
    }
    let marker = values[0]
        .as_deref()
        .ok_or_else(|| admission_invalid("the logical format is absent"))?;
    if !current_key_format(marker) {
        return Err(admission_invalid(
            "the logical format differs from the node marker",
        ));
    }
    validate_frozen_replay_floor(Some(marker), values[1].as_deref())?;
    validate_local_author(values[3].as_deref())?;
    let proof = values[2]
        .as_deref()
        .ok_or_else(|| admission_invalid("the account proof is absent"))?;
    content_key_proof::verify(key, proof)
}

fn admission_invalid(text: &str) -> StorageError {
    StorageError::InvalidOperation(format!("node range admission: {text}"))
}

pub(in crate::storage::disk_pack) fn is_new_home(path: &Path) -> StorageResult<bool> {
    match std::fs::read_dir(path) {
        Ok(mut entries) => Ok(entries.next().is_none()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(error) => Err(error.into()),
    }
}

pub(in crate::storage::disk_pack) fn creation_stopped(path: &Path) -> StorageResult<bool> {
    Ok(path.join(INTENT).try_exists()? && !path.join(NODE_MARKER).try_exists()?)
}

fn clear_stopped_creation(path: &Path) -> StorageResult<()> {
    let mut partial = Vec::new();
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        let file_name = entry.file_name();
        let name = file_name.to_str().unwrap_or_default();
        if kind.is_file() && name == INTENT {
            continue;
        }
        let run_home = [RUN_DIR, CAS_DIR].into_iter().any(|home| {
            name == home
                || name
                    .strip_prefix('.')
                    .and_then(|r| r.strip_prefix(home))
                    .is_some_and(|r| r.starts_with(".create-"))
        });
        if (kind.is_dir() && run_home) || (kind.is_file() && name == STAGED_MARKER) {
            partial.push((entry.path(), kind.is_dir()));
            continue;
        }
        return Err(StorageError::InvalidOperation(format!(
            "a format v2 node home creation stopped beside the unexpected entry {}; the home \
             holds no data, so move that entry away and start the node again",
            file_name.to_string_lossy()
        )));
    }
    for (entry, is_dir) in partial {
        if is_dir {
            std::fs::remove_dir_all(&entry)?;
        } else {
            std::fs::remove_file(&entry)?;
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
    let hkdf = hkdf::Hkdf::<sha2::Sha256>::new(None, node_key);
    let mut key = zeroize::Zeroizing::new([0u8; 32]);
    hkdf.expand(label, key.as_mut())
        .expect("32 bytes is a valid HKDF-SHA256 output length");
    key
}

pub(in crate::storage::disk_pack) fn require_key(
    node_key: Option<&[u8; 32]>,
) -> StorageResult<&[u8; 32]> {
    node_key.ok_or_else(|| {
        StorageError::InvalidOperation(
            "a format v2 node home needs the node encryption key; every new node home is v2".into(),
        )
    })
}

fn initialize_home(home: &RunHome, node_key: &[u8; 32]) -> StorageResult<&'static [u8]> {
    let row = |ns: &str, k: &[u8], body: Vec<u8>| {
        let (collection, key) = kv::coordinate(ns, k);
        HomeMutation {
            collection,
            key,
            body: Some(body),
        }
    };
    let rows = vec![
        row(
            MAIN_NAMESPACE,
            CURRENT_KEY_FORMAT_KEY.as_bytes(),
            CURRENT_KEY_FORMAT_VALUE.to_vec(),
        ),
        row(
            content_key_proof::NAMESPACE,
            content_key_proof::KEY,
            content_key_proof::create(node_key)?,
        ),
        row(
            METADATA_NAMESPACE,
            LOCAL_AUTHOR_KEY.as_bytes(),
            initial_local_author(home.identity())?,
        ),
        row(
            METADATA_NAMESPACE,
            FILE_RETENTION_KEY.as_bytes(),
            FILE_RETENTION_COMPLETE.to_vec(),
        ),
    ];
    home.commit(rows).map_err(map)?;
    Ok(RECORD_NODE_BYTES)
}

impl PackNamespacedStore {
    pub fn create_run(
        path: &Path,
        node_key: &[u8; 32],
        capture_accepted: bool,
    ) -> StorageResult<Self> {
        std::fs::create_dir_all(path)?;
        let folder = std::fs::File::open(path)?;
        folder.try_lock().map_err(|error| {
            StorageError::InvalidOperation(format!(
                "another owner is creating this format v2 node home or its lock failed: {error}"
            ))
        })?;
        if !path.join(NODE_MARKER).try_exists()? {
            if creation_stopped(path)? {
                clear_stopped_creation(path)?;
            } else if std::fs::read_dir(path)?.next().is_some() {
                return Err(StorageError::InvalidOperation(
                    "format v2 creation requires an empty node home".into(),
                ));
            }
            drop(std::fs::File::create(path.join(INTENT))?);
            folder.sync_all()?;
            let main = RunHome::create(&path.join(RUN_DIR), &home_key(node_key)).map_err(map)?;
            drop(RunHome::create(&path.join(CAS_DIR), &cas_key(node_key)).map_err(map)?);
            let marker = initialize_home(&main, node_key)?;
            drop(main);
            let staged = path.join(STAGED_MARKER);
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&staged)?;
            file.write_all(marker)?;
            file.sync_all()?;
            std::fs::rename(&staged, path.join(NODE_MARKER))?;
            folder.sync_all()?;
        }
        let store = Self::open_run(path, Some(node_key), capture_accepted);
        drop(folder);
        store
    }

    pub(in crate::storage::disk_pack) fn open_run(
        path: &Path,
        node_key: Option<&[u8; 32]>,
        capture_accepted: bool,
    ) -> StorageResult<Self> {
        let node_key = require_key(node_key)?;
        admit(path, node_key)?;
        let home = Arc::new(RunHome::open(&path.join(RUN_DIR), &home_key(node_key)).map_err(map)?);
        let cas = Arc::new(RunHome::open(&path.join(CAS_DIR), &cas_key(node_key)).map_err(map)?);
        let mut v2 = physical::V2Home::new(home, cas);
        v2.cas_delete_recovery = super::cas_delete::recover(&v2)?;
        let policy = space::policy_from_env()?;
        let root = v2.current_root();
        let store = Self::assemble(
            v2,
            root,
            path.join(INTENT).try_exists()?,
            policy,
            capture_accepted,
        );
        store.runtime.resume_merges();
        Ok(store)
    }
}

pub(in crate::storage::disk_pack) fn complete_initialization(run_home: &Path) -> StorageResult<()> {
    let node = run_home.parent().ok_or_else(|| {
        StorageError::InvalidOperation("a format v2 run home has no node home".into())
    })?;
    match std::fs::remove_file(node.join(INTENT)) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    std::fs::File::open(node)?.sync_all()?;
    Ok(())
}
