//! Open, create, and remove owned sibling stores. The owner store is not handled here.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use fold_db::crypto::E2eKeys;
use fold_db::fold_db_core::FoldDB;
use fold_db::security::Ed25519KeyPair;
use fold_db::storage::config::DatabaseConfig;
use fold_db::storage::disk_pack::NODE_MARKER;
use hkdf::Hkdf;
use sha2::Sha256;
use zeroize::Zeroizing;

use super::{
    ensure_free, registry, snapshot, validate_create, Entry, Kind, OwnedError, Resolved, Tables,
};
use crate::profile::PublicationSpec;

/// Domain for owned-database content keys. Personal uses the node seed through
/// `E2eKeys::from_ed25519_seed`. Publication homes use a different salt.
const OWNED_CONTENT_DOMAIN: &[u8] = b"lastdb:owned-database:content:v1";

impl Tables {
    pub(crate) async fn create(
        &self,
        home: &Path,
        signer: Arc<Ed25519KeyPair>,
        name: &str,
        kind: Kind,
        spec: Option<PublicationSpec>,
        requested_id: Option<uuid::Uuid>,
    ) -> Result<Entry, OwnedError> {
        if kind == Kind::Personal && spec.is_some() {
            return Err(OwnedError::InvalidSpec);
        }
        validate_create(name, spec.as_ref())?;
        let _creating = self.creating.lock().await;
        let id = match requested_id {
            Some(id) => id.to_string(),
            None => self.allocate(name)?,
        };
        let entry = Entry {
            id,
            name: Some(name.to_owned()),
            kind,
            spec,
        };
        if requested_id.is_some() {
            let state = self.read()?;
            if let Some(existing) = state.owned.iter().find(|row| row.id == entry.id) {
                return if existing == &entry {
                    Ok(entry)
                } else {
                    Err(OwnedError::AlreadyRegistered)
                };
            }
            ensure_free(&state, name)?;
            if state.registry_owner.id == entry.id {
                return Err(OwnedError::AlreadyRegistered);
            }
        }
        self.create_registered(home, signer, entry).await
    }

    /// Register the cloud id in an isolated fresh recovery home.
    pub(crate) async fn register_recovered(
        &self,
        home: &Path,
        signer: Arc<Ed25519KeyPair>,
        name: &str,
        id: &cloud_backup_protocol::Id,
    ) -> Result<Entry, OwnedError> {
        validate_create(name, None)?;
        if !matches!(
            self.registered_store(id.as_str())?,
            (Resolved::Absent, None)
        ) {
            return Err(OwnedError::AlreadyRegistered);
        }
        self.create_registered(
            home,
            signer,
            Entry {
                id: id.as_str().to_owned(),
                name: Some(name.to_owned()),
                kind: Kind::Personal,
                spec: None,
            },
        )
        .await
    }

    async fn create_registered(
        &self,
        home: &Path,
        signer: Arc<Ed25519KeyPair>,
        entry: Entry,
    ) -> Result<Entry, OwnedError> {
        let id = &entry.id;
        let store = open(home, &entry, signer, true)
            .await
            .map_err(|_| OwnedError::Unavailable)?;
        if let Err(error) = self.commit_create(home, entry.clone(), Arc::clone(&store)) {
            if !error.renamed {
                let _ = store.shutdown().await;
                let _ = remove(home, id);
            }
            return Err(error.kind);
        }
        Ok(entry)
    }

    /// Point the personal row at a restored cloud id. The store stays `<home>/data`.
    pub(crate) fn adopt_owner_id(&self, home: &Path, database_id: &str) -> Result<(), OwnedError> {
        if cloud_backup_protocol::Id::try_from(database_id.to_owned()).is_err() {
            return Err(OwnedError::InvalidName);
        }
        let mut state = self.write()?;
        if state.registry_owner.kind != Kind::Personal
            || state.registry_owner.name.as_deref() != Some("personal")
            || state.registry_owner.spec.is_some()
        {
            return Err(OwnedError::Unavailable);
        }
        if state.registry_owner.id == database_id {
            return Ok(());
        }
        if state.stores.contains_key(database_id)
            || state.owned.iter().any(|entry| entry.id == database_id)
        {
            return Err(OwnedError::AlreadyRegistered);
        }
        let previous = state.registry_owner.id.clone();
        state.registry_owner.id = database_id.to_owned();
        if let Err(error) = registry::write(home, &snapshot(&state)) {
            if !error.renamed {
                state.registry_owner.id = previous;
            }
            return Err(OwnedError::Unavailable);
        }
        Ok(())
    }
}

pub(super) async fn open_all(
    home: &Path,
    owned: &[Entry],
    signer: &Arc<Ed25519KeyPair>,
) -> Result<HashMap<String, Arc<FoldDB>>, String> {
    let mut stores = HashMap::new();
    for entry in owned {
        let store = open(home, entry, Arc::clone(signer), false).await?;
        stores.insert(entry.id.clone(), store);
    }
    Ok(stores)
}

pub(super) async fn open(
    home: &Path,
    entry: &Entry,
    signer: Arc<Ed25519KeyPair>,
    create: bool,
) -> Result<Arc<FoldDB>, String> {
    let id = &entry.id;
    let root = home.join("databases").join(id);
    let data = root.join("data");
    if !create && !data.join(NODE_MARKER).is_file() {
        return Err(format!("owned database {id} store is missing"));
    }
    let node_seed = Zeroizing::new(signer.secret_key_bytes());
    let keys = content_keys(&node_seed, id)?;
    std::fs::create_dir_all(&data)
        .map_err(|error| format!("create owned database dir: {error}"))?;
    let inbox = root.join("search-inbox");
    std::fs::create_dir_all(&inbox)
        .map_err(|error| format!("create owned search inbox: {error}"))?;
    fold_db::db_operations::search_index::queue_policy::prepare(&inbox)
        .map_err(|error| format!("owned search queue preparation failed: {error}"))?;
    fold_db::fold_db_core::factory::create_fold_db_with_options(
        &DatabaseConfig::local(data),
        &keys,
        signer,
        fold_db::fold_db_core::factory::FoldDbOptions {
            offline_maintenance: false,
            search_outbox_inbox: Some(inbox),
            // Owned siblings are shareable. Capture starts here, before cloud enrollment.
            capture_accepted: true,
            capture_initial_snapshot: false,
            track_shared_record_revisions: entry.whole_database(),
        },
    )
    .await
    .map_err(|error| format!("open owned database {id}: {error}"))
}

fn content_keys(node_seed: &[u8; 32], id: &str) -> Result<E2eKeys, String> {
    let mut seed = Zeroizing::new([0u8; 32]);
    Hkdf::<Sha256>::new(Some(OWNED_CONTENT_DOMAIN), node_seed)
        .expand(id.as_bytes(), seed.as_mut())
        .map_err(|_| "cannot derive owned database content key")?;
    E2eKeys::from_ed25519_seed(&seed).map_err(|_| "cannot derive owned database content key".into())
}

pub(super) fn content_encryption_key(
    node_seed: &[u8; 32],
    id: &str,
) -> Result<Zeroizing<[u8; 32]>, String> {
    Ok(Zeroizing::new(
        content_keys(node_seed, id)?.encryption_key(),
    ))
}

fn remove(home: &Path, id: &str) -> Result<(), String> {
    let root = home.join("databases").join(id);
    if root.is_dir() {
        std::fs::remove_dir_all(&root)
            .map_err(|error| format!("remove owned database {id}: {error}"))?;
    }
    Ok(())
}
