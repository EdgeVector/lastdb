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

use super::{ensure_free, validate_create, Entry, Kind, OwnedError, Tables};
use crate::profile::PublicationSpec;

/// Domain for owned-database content keys. Personal uses the node seed through
/// `E2eKeys::from_ed25519_seed`. Publication homes use a different salt.
const OWNED_CONTENT_DOMAIN: &[u8] = b"lastdb:owned-database:content:v1";

/// Inputs for [`Tables::create`] besides the home, signer, and birth mint.
pub(crate) struct CreateArgs<'a> {
    pub name: &'a str,
    pub kind: Kind,
    pub spec: Option<PublicationSpec>,
    pub requested_id: Option<String>,
}

impl Tables {
    pub(crate) async fn lock_creating(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.creating.lock().await
    }

    pub(crate) fn claims(&self, id: &str, name: Option<&str>) -> Result<bool, OwnedError> {
        let state = self.read()?;
        let claimed = |entry: &Entry| {
            entry.id == id || name.is_some_and(|wanted| entry.name.as_deref() == Some(wanted))
        };
        Ok(claimed(&state.registry_owner) || state.owned.iter().any(claimed))
    }

    pub(crate) async fn create(
        &self,
        _creating: &tokio::sync::MutexGuard<'_, ()>,
        home: &Path,
        signer: Arc<Ed25519KeyPair>,
        args: CreateArgs<'_>,
        mint_birth: impl FnOnce(&str, &[u8; 32]) -> Result<(), String>,
    ) -> Result<Entry, OwnedError> {
        if args.kind == Kind::Personal && args.spec.is_some() {
            return Err(OwnedError::InvalidSpec);
        }
        validate_create(args.name, args.spec.as_ref())?;
        let (id, birth_seed) = if let Some(ref id) = args.requested_id {
            mutation_log::DatabaseId::from_hex(id).map_err(|_| OwnedError::InvalidName)?;
            (id.clone(), None)
        } else {
            let (id, seed) = self.allocate(args.name)?;
            (id, Some(seed))
        };
        let entry = Entry {
            id,
            name: Some(args.name.to_owned()),
            kind: args.kind,
            spec: args.spec,
            lease_length_ms: None,
        };
        if args.requested_id.is_some() {
            let state = self.read()?;
            if let Some(existing) = state.owned.iter().find(|row| row.id == entry.id) {
                let mut same = entry.clone();
                same.lease_length_ms = existing.lease_length_ms;
                return if existing == &same {
                    Ok(same)
                } else {
                    Err(OwnedError::AlreadyRegistered)
                };
            }
            ensure_free(&state, args.name)?;
            if state.registry_owner.id == entry.id {
                return Err(OwnedError::AlreadyRegistered);
            }
        }
        ensure_not_received(home, &entry.id, args.name)?;
        if let (Kind::Shareable, Some(seed)) = (entry.kind, &birth_seed) {
            mint_birth(&entry.id, seed).map_err(|_| OwnedError::Unavailable)?;
        }
        self.create_registered(home, signer, entry).await
    }

    async fn create_registered(
        &self,
        home: &Path,
        signer: Arc<Ed25519KeyPair>,
        entry: Entry,
    ) -> Result<Entry, OwnedError> {
        let id = &entry.id;
        let Ok(store) = open(home, &entry, &signer, true).await else {
            let _ = remove(home, id);
            return Err(OwnedError::Unavailable);
        };
        if let Err(error) = self.commit_create(home, entry.clone(), Arc::clone(&store)) {
            if !error.renamed {
                let _ = store.shutdown().await;
                let _ = remove(home, id);
            }
            return Err(error.kind);
        }
        Ok(entry)
    }
}

fn ensure_not_received(home: &Path, id: &str, name: &str) -> Result<(), OwnedError> {
    let shares = crate::registered_share::list(home).map_err(|_| OwnedError::Unavailable)?;
    if shares
        .iter()
        .any(|share| share.id.as_str() == id || share.name.as_deref() == Some(name))
    {
        return Err(OwnedError::AlreadyRegistered);
    }
    Ok(())
}

pub(super) async fn open_all(
    home: &Path,
    owned: &[Entry],
    signer: &Arc<Ed25519KeyPair>,
) -> Result<HashMap<String, Arc<FoldDB>>, String> {
    let mut stores = HashMap::new();
    for entry in owned {
        let store = open(home, entry, signer, false).await?;
        stores.insert(entry.id.clone(), store);
    }
    Ok(stores)
}

pub(super) async fn open(
    home: &Path,
    entry: &Entry,
    signer: &Ed25519KeyPair,
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
    fold_db::fold_db_core::factory::create_fold_db_with_options(
        &DatabaseConfig::local(data),
        &keys,
        signer,
        fold_db::fold_db_core::factory::FoldDbOptions {
            // An owned sibling is signed from its history (`log_sync::mirror`),
            // and nothing sends a spool for it, so it captures nothing.
            capture_accepted: false,
            capture_author: None,
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
    let mut first_err = None;
    for root in [
        home.join("databases").join(id),
        home.join("log-sync").join(id),
    ] {
        if root.is_dir() {
            if let Err(error) = std::fs::remove_dir_all(&root) {
                if first_err.is_none() {
                    first_err = Some(format!("remove owned database {id}: {error}"));
                }
            }
        }
    }
    first_err.map_or(Ok(()), Err)
}
