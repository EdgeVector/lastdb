//! One grant per app id. The app socket names the app by that id.
//!
//! Grants live in `app-grants.json` in the data directory, beside the sockets.
//! The file is not part of the database. A missing or unreadable file means
//! the node has no grants.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use serde::{Deserialize, Serialize};

use crate::profile::private_fs;

/// File name beside `folddb.sock` and `apps.sock`.
pub(crate) const GRANTS_FILE: &str = "app-grants.json";

/// File grammar. Stays 1. `"*"` does not deserialize as a name list, so an
/// older node rejects a file that contains it. It does not read that side as
/// an empty list. A higher version would make that node reject every file
/// this node writes, including grants that only name schemas.
const VERSION: u32 = 1;
const MAX_APPS: usize = 256;
const MAX_NAMES: usize = 256;
const MAX_SCHEMA_NAME: usize = 128;
const MAX_EXTENSION_NAME: usize = 40;
const MAX_DATA_LEN: usize = 4096;
const MAX_FILE_BYTES: usize = 1024 * 1024;

/// Schemas one side of a grant allows. `All` is the registration value `"*"`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SchemaGrant {
    Names(BTreeSet<String>),
    All,
}

impl SchemaGrant {
    pub(crate) fn allows(&self, schema: &str) -> bool {
        match self {
            Self::All => true,
            Self::Names(names) => names.contains(schema),
        }
    }

    /// `None` when this side allows every schema.
    pub(crate) fn names(&self) -> Option<&BTreeSet<String>> {
        match self {
            Self::All => None,
            Self::Names(names) => Some(names),
        }
    }
}

/// What one app may do on the app socket: the grants it registered, and its
/// data folder when the owner named one.
#[derive(Debug, Clone)]
pub(crate) struct AppGrant {
    pub(crate) app: String,
    pub(crate) read: SchemaGrant,
    pub(crate) write: SchemaGrant,
    pub(crate) extensions: BTreeSet<String>,
    pub(crate) data_dir: Option<PathBuf>,
}

impl AppGrant {
    pub(crate) fn new(
        app: String,
        read: SchemaGrant,
        write: SchemaGrant,
        extensions: BTreeSet<String>,
        data_dir: Option<PathBuf>,
    ) -> Self {
        Self {
            app,
            read,
            write,
            extensions,
            data_dir,
        }
    }
}

pub(crate) enum RegisterError {
    Invalid,
    Limit,
    Store,
}

/// A name list, or the string `"*"`. Untagged, so a list serializes as a list.
/// An older node expects a list and rejects the string instead of seeing no names.
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum StoredSchemas {
    Names(BTreeSet<String>),
    All(String),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredGrant {
    read: StoredSchemas,
    write: StoredSchemas,
    extensions: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    data: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Catalog {
    version: u32,
    apps: BTreeMap<String, StoredGrant>,
}

struct Store {
    grants: BTreeMap<String, Arc<AppGrant>>,
    path: PathBuf,
}

/// The grants the node is serving. Keyed by app id.
pub(crate) struct AppGrants(Mutex<Store>);

impl AppGrants {
    /// Load grants from `data_dir`. A missing or unreadable file is no grants.
    pub(crate) fn open(data_dir: &Path) -> Self {
        let path = data_dir.join(GRANTS_FILE);
        let grants = load(&path);
        Self(Mutex::new(Store { grants, path }))
    }

    /// Replace the grant for this app. The same app id keeps one grant.
    ///
    /// A grant equal to the one already held answers success and does not
    /// write the file. Equal compares the read side, the write side, the
    /// extensions, and the data folder. A side is equal when both are `"*"`
    /// or both are the same set of schema names.
    pub(crate) fn register(&self, grant: AppGrant) -> Result<(), RegisterError> {
        if !grant_ok(&grant) {
            return Err(RegisterError::Invalid);
        }
        let mut store = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if store
            .grants
            .get(&grant.app)
            .is_some_and(|held| same_grant(held, &grant))
        {
            return Ok(());
        }
        let new_app = !store.grants.contains_key(&grant.app);
        if new_app && store.grants.len() >= MAX_APPS {
            return Err(RegisterError::Limit);
        }
        let mut next = store.grants.clone();
        next.insert(grant.app.clone(), Arc::new(grant));
        let bytes = encode(&next).ok_or(RegisterError::Store)?;
        store_next(&mut store, next, &bytes)
    }

    /// Drop the grant for this app.
    ///
    /// No stored grant answers success and does not write the file. A later
    /// `register` of the same app id creates a new grant.
    pub(crate) fn unregister(&self, app: &str) -> Result<bool, RegisterError> {
        if !app_identity_crypto::valid_app_id(app) {
            return Err(RegisterError::Invalid);
        }
        let mut store = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if !store.grants.contains_key(app) {
            return Ok(false);
        }
        let mut next = store.grants.clone();
        next.remove(app);
        let bytes = encode(&next).ok_or(RegisterError::Store)?;
        store_next(&mut store, next, &bytes)?;
        Ok(true)
    }

    pub(crate) fn get(&self, app_id: &str) -> Option<Arc<AppGrant>> {
        if !app_identity_crypto::valid_app_id(app_id) {
            return None;
        }
        let store = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        store.grants.get(app_id).cloned()
    }
}

fn store_next(
    store: &mut Store,
    next: BTreeMap<String, Arc<AppGrant>>,
    bytes: &[u8],
) -> Result<(), RegisterError> {
    if bytes.len() > MAX_FILE_BYTES {
        return Err(RegisterError::Limit);
    }
    if let Err(error) = private_fs::write_private(&store.path, bytes) {
        tracing::warn!(
            target: "lastdb_node::app_grants",
            "could not store app grants: {}",
            error.kind()
        );
        return Err(RegisterError::Store);
    }
    store.grants = next;
    Ok(())
}

fn same_grant(held: &AppGrant, grant: &AppGrant) -> bool {
    held.read == grant.read
        && held.write == grant.write
        && held.extensions == grant.extensions
        && held.data_dir == grant.data_dir
}

fn grant_ok(grant: &AppGrant) -> bool {
    app_identity_crypto::valid_app_id(&grant.app)
        && schemas_ok(&grant.read)
        && schemas_ok(&grant.write)
        && names_ok(&grant.extensions, extension_name)
        && data_ok(grant.data_dir.as_deref())
}

fn schemas_ok(schemas: &SchemaGrant) -> bool {
    match schemas {
        SchemaGrant::All => true,
        SchemaGrant::Names(names) => names_ok(names, schema_name),
    }
}

fn names_ok(names: &BTreeSet<String>, accept: fn(&str) -> bool) -> bool {
    names.len() <= MAX_NAMES && names.iter().all(|name| accept(name))
}

fn schema_name(value: &str) -> bool {
    (1..=MAX_SCHEMA_NAME).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'/' | b'-'))
}

fn extension_name(value: &str) -> bool {
    (1..=MAX_EXTENSION_NAME).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn data_ok(data: Option<&Path>) -> bool {
    let Some(path) = data else {
        return true;
    };
    path.to_str()
        .is_some_and(|text| (1..=MAX_DATA_LEN).contains(&text.len()) && path.is_absolute())
}

fn encode(grants: &BTreeMap<String, Arc<AppGrant>>) -> Option<Vec<u8>> {
    let apps = grants
        .iter()
        .map(|(app, grant)| (app.clone(), stored_from(grant)))
        .collect();
    serde_json::to_vec(&Catalog {
        version: VERSION,
        apps,
    })
    .ok()
}

fn stored_from(grant: &AppGrant) -> StoredGrant {
    StoredGrant {
        read: stored_schemas(&grant.read),
        write: stored_schemas(&grant.write),
        extensions: grant.extensions.clone(),
        data: grant
            .data_dir
            .as_ref()
            .and_then(|path| path.to_str().map(str::to_owned)),
    }
}

fn stored_schemas(schemas: &SchemaGrant) -> StoredSchemas {
    match schemas {
        SchemaGrant::All => StoredSchemas::All(app_identity_crypto::ALL_SCHEMAS.to_owned()),
        SchemaGrant::Names(names) => StoredSchemas::Names(names.clone()),
    }
}

fn load(path: &Path) -> BTreeMap<String, Arc<AppGrant>> {
    let bytes = match private_fs::read_file_limited(path, MAX_FILE_BYTES, true) {
        Ok(None) => return BTreeMap::new(),
        Ok(Some(bytes)) => bytes,
        Err(error) => {
            tracing::warn!(
                target: "lastdb_node::app_grants",
                "app grants file is unreadable: {}",
                error.kind()
            );
            return BTreeMap::new();
        }
    };
    decode(&bytes).unwrap_or_else(|| {
        tracing::warn!(
            target: "lastdb_node::app_grants",
            "app grants file is unreadable"
        );
        BTreeMap::new()
    })
}

fn decode(bytes: &[u8]) -> Option<BTreeMap<String, Arc<AppGrant>>> {
    let catalog: Catalog = serde_json::from_slice(bytes).ok()?;
    if catalog.version != VERSION || catalog.apps.len() > MAX_APPS {
        return None;
    }
    let mut grants = BTreeMap::new();
    for (app, stored) in catalog.apps {
        let grant = accept_stored(app.clone(), stored)?;
        grants.insert(app, Arc::new(grant));
    }
    Some(grants)
}

fn accept_stored(app: String, stored: StoredGrant) -> Option<AppGrant> {
    let grant = AppGrant {
        app,
        read: schemas_from(stored.read)?,
        write: schemas_from(stored.write)?,
        extensions: stored.extensions,
        data_dir: stored.data.map(PathBuf::from),
    };
    grant_ok(&grant).then_some(grant)
}

fn schemas_from(stored: StoredSchemas) -> Option<SchemaGrant> {
    match stored {
        StoredSchemas::All(marker) if marker == app_identity_crypto::ALL_SCHEMAS => {
            Some(SchemaGrant::All)
        }
        StoredSchemas::All(_) => None,
        StoredSchemas::Names(names) => Some(SchemaGrant::Names(names)),
    }
}
