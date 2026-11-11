//! A node home in disk format v2: the node marker and two run homes.

use super::shared_layout::{Scope, SharedHome};
use super::*;
use crate::storage::content_key_proof;
use crate::storage::format::metadata::{
    initial_local_author, FILE_RETENTION_COMPLETE, FILE_RETENTION_KEY, LOCAL_AUTHOR_KEY,
    NATIVE_SHARED_SCOPE_KEY,
};
use crate::storage::format::{
    CAS_KEY_LABEL, CREATION_INTENT as INTENT, CURRENT_KEY_FORMAT_KEY, CURRENT_KEY_FORMAT_VALUE,
    HOME_KEY_LABEL, MAIN_NAMESPACE, METADATA_NAMESPACE, PRIVATE_KEY_LABEL,
    STAGED_NODE_MARKER as STAGED_MARKER,
};
use laststore::disk_run::home::RunHome;
use std::io::Write;

pub(in crate::storage) use crate::storage::format::PRIVATE_DIR;
pub use crate::storage::format::{CAS_DIR, RUN_DIR};

/// True when `path` is absent or empty: a new node home, which is v2.
pub(in crate::storage::disk_pack) fn is_new_home(path: &Path) -> StorageResult<bool> {
    let entries = match std::fs::read_dir(path) {
        Ok(entries) => Some(entries),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    Ok(entries.is_none_or(|mut entries| entries.next().is_none()))
}

/// True when a v2 home creation stopped before its node marker.
pub(in crate::storage::disk_pack) fn creation_stopped(path: &Path) -> StorageResult<bool> {
    Ok(path.join(INTENT).try_exists()? && !path.join(NODE_MARKER).try_exists()?)
}

fn clear_stopped_creation(path: &Path) -> StorageResult<()> {
    let staging = |name: &str, home: &str| {
        name.strip_prefix('.')
            .and_then(|rest| rest.strip_prefix(home))
            .is_some_and(|rest| rest.starts_with(".create-"))
    };
    let mut partial = Vec::new();
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        let file_name = entry.file_name();
        let name = file_name.to_str().unwrap_or_default();
        if kind.is_file() && name == INTENT {
            continue;
        }
        let run_home = [RUN_DIR, CAS_DIR, PRIVATE_DIR]
            .into_iter()
            .any(|home| name == home || staging(name, home));
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

pub(in crate::storage) fn private_key(node_key: &[u8; 32]) -> zeroize::Zeroizing<[u8; 32]> {
    derive_key(node_key, PRIVATE_KEY_LABEL)
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

fn initialize_home(
    home: &RunHome,
    node_key: &[u8; 32],
    scope: Option<&Scope>,
) -> StorageResult<&'static [u8]> {
    let (collection, key) = kv::coordinate(MAIN_NAMESPACE, CURRENT_KEY_FORMAT_KEY.as_bytes());
    let format = HomeMutation {
        collection,
        key,
        body: Some(CURRENT_KEY_FORMAT_VALUE.to_vec()),
    };
    let proof = HomeMutation {
        collection: content_key_proof::NAMESPACE.into(),
        key: super::native_routing::encode(content_key_proof::KEY),
        body: Some(content_key_proof::create(node_key)?),
    };
    let mut rows = vec![format, proof];
    let marker = if let Some(scope) = scope {
        rows.push(HomeMutation {
            collection: METADATA_NAMESPACE.into(),
            key: NATIVE_SHARED_SCOPE_KEY.into(),
            body: Some(scope.encode()?),
        });
        SHARED_NODE_BYTES
    } else {
        rows.extend(local_rows(home.identity())?);
        RECORD_NODE_BYTES
    };
    home.commit(rows).map_err(map)?;
    Ok(marker)
}

fn local_rows(stream: uuid::Uuid) -> StorageResult<Vec<HomeMutation>> {
    let (collection, key) = kv::coordinate(METADATA_NAMESPACE, LOCAL_AUTHOR_KEY.as_bytes());
    let author = HomeMutation {
        collection,
        key,
        body: Some(initial_local_author(stream)?),
    };
    let (collection, key) = kv::coordinate(METADATA_NAMESPACE, FILE_RETENTION_KEY.as_bytes());
    let file_retention = HomeMutation {
        collection,
        key,
        body: Some(FILE_RETENTION_COMPLETE.to_vec()),
    };
    Ok(vec![author, file_retention])
}

impl PackNamespacedStore {
    pub fn create_run(path: &Path, node_key: &[u8; 32]) -> StorageResult<Self> {
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
            let scope = Scope::requested()?;
            if scope.is_some() {
                create_private(path, node_key)?;
            }
            let marker = initialize_home(&main, node_key, scope.as_ref())?;
            drop(main);
            let staged = path.join(STAGED_MARKER);
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&staged)?;
            file.write_all(marker)?;
            file.sync_all()?;
            std::fs::rename(&staged, path.join(NODE_MARKER))?;
            folder.sync_all()?;
        }
        let store = Self::open_run(path, Some(node_key));
        drop(folder);
        store
    }

    pub(in crate::storage::disk_pack) fn open_run(
        path: &Path,
        node_key: Option<&[u8; 32]>,
    ) -> StorageResult<Self> {
        let node_key = require_key(node_key)?;
        let version = super::run_admission::admit(path, node_key)?;
        let home = Arc::new(RunHome::open(&path.join(RUN_DIR), &home_key(node_key)).map_err(map)?);
        let cas = Arc::new(RunHome::open(&path.join(CAS_DIR), &cas_key(node_key)).map_err(map)?);
        let mut v2 = physical::V2Home::new(home, cas);
        if version == super::run_admission::SHARED_RECORD_PROFILE {
            open_shared(path, node_key, &mut v2)?;
        }
        v2.cas_delete_recovery = super::cas_delete::recover(&v2)?;
        let policy = space::policy_from_env()?;
        let root = v2.current_root();
        let initializing = path.join(INTENT).try_exists()?;
        let store = Self::assemble(v2, root, initializing, policy);
        store.runtime.resume_merges();
        Ok(store)
    }
}

/// Create the device-private disk home after authenticating a downloaded shared snapshot.
pub fn prepare_shared_restored_run(path: &Path, key: &[u8; 32]) -> StorageResult<()> {
    let version = super::run_admission::admit(path, key)?;
    if version == super::run_admission::SHARED_RECORD_PROFILE {
        create_private(path, key)?;
    }
    Ok(())
}

fn create_private(path: &Path, key: &[u8; 32]) -> StorageResult<()> {
    let private = RunHome::create(&path.join(PRIVATE_DIR), &private_key(key)).map_err(map)?;
    private
        .commit(local_rows(private.identity())?)
        .map_err(map)?;
    Ok(())
}

fn open_shared(path: &Path, key: &[u8; 32], v2: &mut physical::V2Home) -> StorageResult<()> {
    let private = Arc::new(RunHome::open(&path.join(PRIVATE_DIR), &private_key(key)).map_err(map)?);
    shared_layout::journal::recover(v2, &private, &private_key(key))?;
    let mut main = v2.home.read_root(&v2.home.current_root(), budget());
    let scope = Scope::load(&mut main)?;
    verify_private_author(&mut private.read_root(&private.current_root(), budget()))?;
    v2.shared = Some(SharedHome {
        private,
        scope: Arc::new(RwLock::new(Arc::new(scope))),
        journal_key: private_key(key),
    });
    Ok(())
}

pub(in crate::storage) fn verify_private_author(
    local: &mut laststore::disk_run::home::RunRead,
) -> StorageResult<()> {
    let (collection, author_key) = kv::coordinate(METADATA_NAMESPACE, LOCAL_AUTHOR_KEY.as_bytes());
    let author = local
        .get_bounded(&collection, &author_key, 8192)
        .map_err(map)?;
    crate::storage::format::metadata::validate_local_author(author.as_deref())
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
