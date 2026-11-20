//! Owned databases beside the booted owner store.
//!
//! The registry file is `<home>/databases/registry.json`. Each sibling store is
//! `<home>/databases/<id>/data`. The owner store stays `<home>/data`. Cloud
//! backup is not enrolled here.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, RwLock};

use fold_db::fold_db_core::FoldDB;
use fold_db::security::Ed25519KeyPair;
use serde::{Deserialize, Serialize};

use super::Host;
use crate::profile::PublicationSpec;

mod registry;
mod store;

pub(crate) use registry::{find_shareable, registered_databases, registry_claims, validate_create};

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
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Kind {
    Personal,
    Shareable,
}

impl Kind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Personal => "personal",
            Self::Shareable => "shareable",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Entry {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    pub kind: Kind,
    #[serde(default)]
    pub spec: Option<PublicationSpec>,
}

impl Entry {
    pub(crate) fn whole_database(&self) -> bool {
        self.spec
            .as_ref()
            .is_some_and(PublicationSpec::whole_database)
    }
}

#[derive(Clone)]
pub(crate) enum ResolvedStore {
    Owner,
    Owned(Arc<FoldDB>),
    Absent,
}

pub(crate) enum OwnedError {
    InvalidName,
    InvalidSpec,
    AlreadyRegistered,
    NotOwned,
    Unavailable,
}

impl OwnedError {
    pub(crate) fn status(self) -> (u16, &'static str) {
        match self {
            Self::InvalidName => (400, "invalid database name"),
            Self::InvalidSpec => (400, "invalid spec binding"),
            Self::AlreadyRegistered => (409, "database name is already registered"),
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
    creating: tokio::sync::Mutex<()>,
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
        Ok(Self {
            state: RwLock::new(State {
                registry_owner: file.owner,
                owned: file.owned,
                stores,
            }),
            creating: tokio::sync::Mutex::new(()),
        })
    }

    pub(crate) fn owner(&self) -> Result<Entry, OwnedError> {
        Ok(self.read()?.registry_owner.clone())
    }

    pub(crate) fn owned_entries(&self) -> Result<Vec<Entry>, OwnedError> {
        Ok(self.read()?.owned.clone())
    }

    pub(crate) fn resolve(&self, selector: &str) -> Result<ResolvedStore, OwnedError> {
        let state = self.read()?;
        if selector == state.registry_owner.id
            || state.registry_owner.name.as_deref() == Some(selector)
        {
            return Ok(ResolvedStore::Owner);
        }
        if let Some(store) = state.stores.get(selector) {
            return Ok(ResolvedStore::Owned(Arc::clone(store)));
        }
        let Some(entry) = state
            .owned
            .iter()
            .find(|entry| entry.name.as_deref() == Some(selector))
        else {
            return Ok(ResolvedStore::Absent);
        };
        let store = state
            .stores
            .get(&entry.id)
            .cloned()
            .ok_or(OwnedError::Unavailable)?;
        Ok(ResolvedStore::Owned(store))
    }

    pub(crate) fn bind_spec(
        &self,
        home: &Path,
        id: &str,
        spec: Option<PublicationSpec>,
    ) -> Result<Entry, OwnedError> {
        let mut state = self.write()?;
        let index = state
            .owned
            .iter()
            .position(|entry| entry.id == id)
            .ok_or(OwnedError::NotOwned)?;
        if state.owned[index].kind == Kind::Personal && spec.is_some() {
            return Err(OwnedError::InvalidSpec);
        }
        if state.owned[index].spec.as_ref().is_some_and(|binding| {
            database_policy::SpecBinding::is_whole_database_name(&binding.name)
        }) {
            return Err(OwnedError::InvalidSpec);
        }
        registry::validate_binding(spec.as_ref())?;
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
        ACTIVE_STORE
            .try_with(Arc::clone)
            .unwrap_or_else(|_| Arc::clone(&self.db))
    }

    pub(crate) async fn with_records<T>(
        &self,
        store: Arc<FoldDB>,
        future: impl std::future::Future<Output = T>,
    ) -> T {
        ACTIVE_STORE.scope(store, future).await
    }

    /// Flush sibling stores. The daemon binary calls this before the owner store.
    pub async fn shutdown_owned_databases(&self) -> Result<(), String> {
        self.owned.shutdown().await
    }
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
