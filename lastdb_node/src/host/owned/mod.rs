//! Owned databases beside the booted owner store.
//!
//! The registry file is `<home>/databases/registry.json`. Each sibling store is
//! `<home>/databases/<id>/data`. The owner store stays `<home>/data`. Cloud
//! backup is not enrolled here.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, RwLock};

use tokio::sync::watch;

use fold_db::fold_db_core::FoldDB;
use fold_db::security::Ed25519KeyPair;
use serde::{Deserialize, Serialize};

use super::Host;

mod lookup;
mod registry;
mod store;

pub(crate) use lookup::{find_shareable, registry_claims};
pub(crate) use registry::validate_create;

/// AES content key for one owned sibling. Callers must not treat this as a
/// signing seed.
pub(crate) fn content_encryption_key(
    node_seed: &[u8; 32],
    id: &str,
) -> Result<zeroize::Zeroizing<[u8; 32]>, String> {
    store::content_encryption_key(node_seed, id)
}

tokio::task_local! {
    static ACTIVE_STORE: Arc<FoldDB>;
    static ACTIVE_BINDING: database_policy::SpecBinding;
}

pub(crate) fn active_store(personal: &Arc<FoldDB>) -> Arc<FoldDB> {
    ACTIVE_STORE
        .try_with(Arc::clone)
        .unwrap_or_else(|_| Arc::clone(personal))
}

/// Spec binding of the shareable database whose store is active on this task.
pub(crate) fn active_binding() -> Option<database_policy::SpecBinding> {
    ACTIVE_BINDING.try_with(Clone::clone).ok()
}

pub(crate) async fn with_store<T>(
    store: Arc<FoldDB>,
    future: impl std::future::Future<Output = T>,
) -> T {
    ACTIVE_STORE.scope(store, future).await
}

pub(crate) async fn with_store_binding<T>(
    store: Arc<FoldDB>,
    binding: Option<database_policy::SpecBinding>,
    future: impl std::future::Future<Output = T>,
) -> T {
    match binding {
        Some(binding) => {
            ACTIVE_STORE
                .scope(store, ACTIVE_BINDING.scope(binding, future))
                .await
        }
        None => with_store(store, future).await,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Kind {
    Personal,
    Shareable,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SpecBinding {
    pub name: String,
    pub version: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Entry {
    pub id: String,
    pub name: Option<String>,
    pub kind: Kind,
    pub spec: Option<SpecBinding>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Resolved {
    Owner,
    Owned,
    Absent,
}

pub(crate) enum OwnedError {
    InvalidName,
    InvalidSpec,
    AlreadyRegistered,
    ReservedUnprovisioned,
    ReservedSpec,
    NotOwned,
    Unavailable,
}

impl OwnedError {
    pub(crate) fn status(self) -> (u16, &'static str) {
        match self {
            Self::InvalidName => (400, "invalid database name"),
            Self::InvalidSpec => (400, "invalid spec binding"),
            Self::AlreadyRegistered => (409, "database name is already registered"),
            Self::ReservedUnprovisioned => (409, "database name is reserved and not provisioned"),
            Self::ReservedSpec => (409, "reserved database name requires its spec"),
            Self::NotOwned => (404, "database is not registered"),
            Self::Unavailable => (503, "database registry is unavailable"),
        }
    }
}

struct State {
    registry_owner: Entry,
    owned: Vec<Entry>,
    stores: HashMap<String, Arc<FoldDB>>,
}

pub(crate) struct Tables {
    state: RwLock<State>,
    changed: watch::Sender<u64>,
}

struct CommitError {
    renamed: bool,
    kind: OwnedError,
}

impl CommitError {
    fn plain(kind: OwnedError) -> Self {
        Self {
            renamed: false,
            kind,
        }
    }
}

impl Tables {
    pub(crate) async fn open(home: &Path, signer: Arc<Ed25519KeyPair>) -> Result<Self, String> {
        let file = registry::load(home)?;
        let stores = store::open_all(home, &file.owned, &signer).await?;
        if registry::read(home)?.is_none() {
            registry::write(home, &file).map_err(|error| error.to_string())?;
        }
        let (changed, _) = watch::channel(0);
        Ok(Self {
            state: RwLock::new(State {
                registry_owner: file.owner,
                owned: file.owned,
                stores,
            }),
            changed,
        })
    }

    /// Wakes after a sibling store is opened. The payload is only a counter.
    pub(crate) fn subscribe_stores(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }

    pub(crate) fn stores(&self) -> Result<Vec<(String, Arc<FoldDB>)>, OwnedError> {
        let state = self.read()?;
        Ok(state
            .stores
            .iter()
            .map(|(id, db)| (id.clone(), Arc::clone(db)))
            .collect())
    }

    pub(crate) fn owner(&self) -> Result<Entry, OwnedError> {
        Ok(self.read()?.registry_owner.clone())
    }

    pub(crate) fn owned_entries(&self) -> Result<Vec<Entry>, OwnedError> {
        Ok(self.read()?.owned.clone())
    }

    pub(crate) fn resolve(
        &self,
        selector: &str,
    ) -> Result<(Resolved, Option<Arc<FoldDB>>), OwnedError> {
        let state = self.read()?;
        if selector == state.registry_owner.id || owner_name_hit(&state, selector) {
            return Ok((Resolved::Owner, None));
        }
        if let Some(store) = state.stores.get(selector) {
            return Ok((Resolved::Owned, Some(Arc::clone(store))));
        }
        let Some(entry) = state
            .owned
            .iter()
            .find(|entry| entry.name.as_deref() == Some(selector))
        else {
            return Ok((Resolved::Absent, None));
        };
        let store = state
            .stores
            .get(&entry.id)
            .cloned()
            .ok_or(OwnedError::Unavailable)?;
        Ok((Resolved::Owned, Some(store)))
    }

    /// Catalog binding for this open shareable store. Personal has none.
    fn policy_binding(&self, store: &Arc<FoldDB>) -> Option<database_policy::SpecBinding> {
        let state = self.read().ok()?;
        let id = state
            .stores
            .iter()
            .find_map(|(id, open)| Arc::ptr_eq(open, store).then(|| id.clone()))?;
        let entry = state.owned.iter().find(|entry| entry.id == id)?;
        if entry.kind != Kind::Shareable {
            return None;
        }
        let spec = entry.spec.as_ref()?;
        database_policy::SpecBinding::admitted(&spec.name, spec.version)
            .or_else(|_| database_policy::SpecBinding::new(&spec.name, spec.version))
            .ok()
    }

    pub(crate) async fn create(
        &self,
        home: &Path,
        signer: Arc<Ed25519KeyPair>,
        name: &str,
        spec: Option<SpecBinding>,
    ) -> Result<Entry, OwnedError> {
        validate_create(name, spec.as_ref())?;
        let id = self.allocate(name)?;
        let store = store::open(home, &id, signer, true)
            .await
            .map_err(|_| OwnedError::Unavailable)?;
        let entry = Entry {
            id: id.clone(),
            name: Some(name.to_owned()),
            kind: Kind::Shareable,
            spec,
        };
        if let Err(error) = self.commit_create(home, entry.clone(), Arc::clone(&store)) {
            if !error.renamed {
                let _ = store.shutdown().await;
                let _ = store::remove(home, &id);
            }
            return Err(error.kind);
        }
        self.changed.send_modify(|generation| {
            *generation = generation.saturating_add(1);
        });
        Ok(entry)
    }

    pub(crate) fn bind_spec(
        &self,
        home: &Path,
        id: &str,
        spec: Option<SpecBinding>,
    ) -> Result<Entry, OwnedError> {
        let mut state = self.write()?;
        let index = state
            .owned
            .iter()
            .position(|entry| entry.id == id)
            .ok_or(OwnedError::NotOwned)?;
        let name = state.owned[index]
            .name
            .clone()
            .ok_or(OwnedError::Unavailable)?;
        validate_binding(&name, spec.as_ref())?;
        let previous = state.owned[index].spec.clone();
        state.owned[index].spec = spec;
        let file = snapshot(&state);
        if let Err(error) = registry::write(home, &file) {
            if !error.renamed {
                state.owned[index].spec = previous;
            }
            tracing::warn!(error = %error, "owned database spec binding was not saved");
            return Err(OwnedError::Unavailable);
        }
        Ok(state.owned[index].clone())
    }

    pub(crate) async fn shutdown(&self) -> Result<(), String> {
        let stores = {
            let mut state = match self.state.write() {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
            state
                .stores
                .drain()
                .map(|(_, store)| store)
                .collect::<Vec<_>>()
        };
        let mut errors = Vec::new();
        for store in stores {
            if let Err(error) = store.shutdown().await {
                errors.push(error.to_string());
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }

    fn allocate(&self, name: &str) -> Result<String, OwnedError> {
        let state = self.read()?;
        ensure_free(&state, name)?;
        registry::fresh_id(&registry::used_ids(&state.registry_owner, &state.owned))
    }

    fn commit_create(
        &self,
        home: &Path,
        entry: Entry,
        store: Arc<FoldDB>,
    ) -> Result<(), CommitError> {
        let mut state = self
            .write()
            .map_err(|_| CommitError::plain(OwnedError::Unavailable))?;
        let name = entry
            .name
            .as_deref()
            .ok_or(OwnedError::Unavailable)
            .map_err(CommitError::plain)?;
        ensure_free(&state, name).map_err(CommitError::plain)?;
        if state.stores.contains_key(&entry.id)
            || registry::used_ids(&state.registry_owner, &state.owned).contains(&entry.id)
        {
            return Err(CommitError::plain(OwnedError::AlreadyRegistered));
        }
        let id = entry.id.clone();
        state.owned.push(entry);
        if let Err(error) = registry::write(home, &snapshot(&state)) {
            if error.renamed {
                state.stores.insert(id, store);
            } else {
                state.owned.pop();
            }
            tracing::warn!(error = %error, "owned database registry write failed");
            return Err(CommitError {
                renamed: error.renamed,
                kind: OwnedError::Unavailable,
            });
        }
        state.stores.insert(id, store);
        Ok(())
    }

    fn read(&self) -> Result<std::sync::RwLockReadGuard<'_, State>, OwnedError> {
        self.state.read().map_err(|_| OwnedError::Unavailable)
    }

    fn write(&self) -> Result<std::sync::RwLockWriteGuard<'_, State>, OwnedError> {
        self.state.write().map_err(|_| OwnedError::Unavailable)
    }
}

impl Host {
    pub(crate) fn records(&self) -> Arc<FoldDB> {
        active_store(&self.db)
    }

    pub(crate) async fn with_records<T>(
        &self,
        store: Arc<FoldDB>,
        future: impl std::future::Future<Output = T>,
    ) -> T {
        let binding = self.owned.policy_binding(&store);
        with_store_binding(store, binding, future).await
    }

    /// Flush sibling stores. The daemon binary calls this before the owner store.
    pub async fn shutdown_owned_databases(&self) -> Result<(), String> {
        self.owned.shutdown().await
    }
}

fn owner_name_hit(state: &State, selector: &str) -> bool {
    state.registry_owner.name.as_deref() == Some("personal") && selector == "personal"
}

fn ensure_free(state: &State, name: &str) -> Result<(), OwnedError> {
    let taken = state.registry_owner.name.as_deref() == Some(name)
        || state
            .owned
            .iter()
            .any(|entry| entry.name.as_deref() == Some(name));
    if taken {
        Err(OwnedError::AlreadyRegistered)
    } else {
        Ok(())
    }
}

fn snapshot(state: &State) -> registry::RegistryFile {
    registry::RegistryFile {
        format: registry::FORMAT,
        owner: state.registry_owner.clone(),
        owned: state.owned.clone(),
    }
}

fn validate_binding(name: &str, spec: Option<&SpecBinding>) -> Result<(), OwnedError> {
    registry::validate_binding(name, spec)
}
