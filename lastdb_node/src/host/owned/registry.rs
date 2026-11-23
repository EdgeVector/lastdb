//! Durable registry of owned database identity, separate from each store.

use std::collections::HashSet;
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::{Entry, Kind, OwnedError};
use crate::profile::PublicationSpec;

pub(super) const FORMAT: u32 = 1;
const REGISTRY_DIR: &str = "databases";
const REGISTRY_FILE: &str = "registry.json";
const REGISTRY_LIMIT: usize = 65_536;

/// One database this node owns. `native` is an owned sibling, not the owner row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RegisteredDatabase {
    pub id: String,
    pub name: Option<String>,
    pub kind: &'static str,
    pub spec: Option<PublicationSpec>,
    pub native: bool,
    pub lease_length_ms: Option<u64>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RegistryFile {
    pub format: u32,
    pub owner: Entry,
    #[serde(default)]
    pub owned: Vec<Entry>,
}

/// The registry of `home`, or a fresh one. The personal database of a home is
/// the one its node seed names, so a registry whose owner row is another
/// database is refused. A home that was made before that rule is not converted.
pub(super) fn load(home: &Path, node_seed: &[u8; 32]) -> Result<RegistryFile, String> {
    let owner = crate::personal_identity::database_id(node_seed);
    match read(home)? {
        Some(file) => check(file, &owner),
        None => Ok(fresh(owner)),
    }
}

/// Owner row, then each usable owned sibling.
pub(crate) fn registered_databases(home: &Path) -> Vec<RegisteredDatabase> {
    let Some(file) = read(home).ok().flatten() else {
        return Vec::new();
    };
    if file.format != FORMAT {
        return Vec::new();
    }
    let Some(owner) = accept_registered(&file.owner, false) else {
        return Vec::new();
    };
    let mut rows = vec![owner];
    for entry in &file.owned {
        let Some(row) = accept_registered(entry, true) else {
            continue;
        };
        if rows.iter().any(|existing| existing.id == row.id) {
            continue;
        }
        rows.push(row);
    }
    rows
}

fn accept_registered(row: &Entry, native: bool) -> Option<RegisteredDatabase> {
    require_id(&row.id).ok()?;
    let spec = match &row.spec {
        Some(spec) if validate_binding(Some(spec)).is_ok() => Some(spec.clone()),
        Some(_) if native => return None,
        None | Some(_) => None,
    };
    Some(RegisteredDatabase {
        id: row.id.clone(),
        name: row.name.clone(),
        kind: row.kind.as_str(),
        spec,
        native,
        lease_length_ms: row.lease_length_ms,
    })
}

/// Resolve one shareable sibling by id or exact name. Share creation also
/// requires a spec binding.
pub(crate) fn find_shareable(home: &Path, selector: &str) -> Result<Entry, String> {
    let entry = find_shareable_database(home, selector)?;
    if entry.spec.is_none() {
        return Err("share creation requires a spec binding".into());
    }
    Ok(entry)
}

/// Resolve one shareable sibling by id or exact name, with or without a spec.
pub(crate) fn find_shareable_database(home: &Path, selector: &str) -> Result<Entry, String> {
    if !crate::profile::valid_label_64(selector) {
        return Err("invalid database selector".into());
    }
    let file = read(home)
        .map_err(|_| "database registry is unavailable".to_owned())?
        .ok_or("database is not registered on this node")?;
    if matches_entry(&file.owner, selector) {
        return Err("personal database is not shareable".into());
    }
    let mut found = None;
    for entry in &file.owned {
        if !matches_entry(entry, selector) {
            continue;
        }
        if found.is_some() {
            return Err("database registry is unavailable".into());
        }
        found = Some(entry.clone());
    }
    let Some(entry) = found else {
        return Err("database is not registered on this node".into());
    };
    if entry.kind != Kind::Shareable {
        return Err("database is not shareable".into());
    }
    require_id(&entry.id).map_err(|_| "database registry is unavailable".to_owned())?;
    Ok(entry)
}

/// True when this id or name is already the owner or an owned sibling.
pub(crate) fn registry_claims(home: &Path, id: &str, name: Option<&str>) -> Result<bool, ()> {
    let Some(file) = read(home).map_err(|_| ())? else {
        return Ok(false);
    };
    let claimed = |entry: &Entry| {
        entry.id == id || name.is_some_and(|name| entry.name.as_deref() == Some(name))
    };
    Ok(claimed(&file.owner) || file.owned.iter().any(claimed))
}

fn matches_entry(entry: &Entry, selector: &str) -> bool {
    entry.id == selector || entry.name.as_deref() == Some(selector)
}

pub(super) fn read(home: &Path) -> Result<Option<RegistryFile>, String> {
    let Some(bytes) =
        crate::profile::private_fs::read_limited(&registry_path(home), REGISTRY_LIMIT)
            .map_err(|error| format!("read database registry: {error}"))?
    else {
        return Ok(None);
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| format!("invalid database registry: {error}"))
}

pub(super) struct WriteError {
    pub renamed: bool,
    message: String,
}

impl std::fmt::Display for WriteError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

pub(super) fn write(home: &Path, registry: &RegistryFile) -> Result<(), WriteError> {
    let dir = home.join(REGISTRY_DIR);
    std::fs::create_dir_all(&dir)
        .map_err(|error| WriteError::unchanged(format!("create database registry dir: {error}")))?;
    let path = dir.join(REGISTRY_FILE);
    let temporary = dir.join(format!("registry.json.tmp.{}", std::process::id()));
    let bytes = serde_json::to_vec_pretty(registry)
        .map_err(|error| WriteError::unchanged(format!("encode database registry: {error}")))?;
    if let Err(error) = write_temp(&temporary, &bytes) {
        let _ = std::fs::remove_file(&temporary);
        return Err(WriteError::unchanged(error));
    }
    if let Err(error) = std::fs::rename(&temporary, &path) {
        let _ = std::fs::remove_file(&temporary);
        return Err(WriteError::unchanged(format!(
            "rename database registry: {error}"
        )));
    }
    std::fs::File::open(&dir)
        .and_then(|file| file.sync_all())
        .map_err(|error| WriteError::renamed(format!("sync database registry dir: {error}")))?;
    Ok(())
}

fn write_temp(temporary: &Path, bytes: &[u8]) -> Result<(), String> {
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    opts.mode(0o600);
    let mut file = opts
        .open(temporary)
        .map_err(|error| format!("write database registry: {error}"))?;
    file.write_all(bytes)
        .map_err(|error| format!("write database registry: {error}"))?;
    file.sync_all()
        .map_err(|error| format!("sync database registry: {error}"))
}

impl WriteError {
    fn unchanged(message: String) -> Self {
        Self {
            renamed: false,
            message,
        }
    }

    fn renamed(message: String) -> Self {
        Self {
            renamed: true,
            message,
        }
    }
}

pub(super) fn used_ids(owner: &Entry, owned: &[Entry]) -> HashSet<String> {
    let mut ids = HashSet::from([owner.id.clone()]);
    ids.extend(owned.iter().map(|entry| entry.id.clone()));
    ids
}

/// A fresh database id and the seed of the key whose verifying key is that id.
/// Only a database that signs its own birth keeps the seed, and only until then.
pub(super) fn fresh_db_key(
    used: &HashSet<String>,
) -> Result<(String, zeroize::Zeroizing<[u8; 32]>), OwnedError> {
    for _ in 0..8 {
        let mut seed = zeroize::Zeroizing::new([0u8; 32]);
        rand::RngCore::fill_bytes(&mut rand::thread_rng(), seed.as_mut());
        let key = app_identity_crypto::SigningKey::from_bytes(&seed);
        let id = app_identity_crypto::hex_lower(&key.verifying_key().to_bytes());
        if require_id(&id).is_ok() && !used.contains(&id) {
            return Ok((id, seed));
        }
    }
    Err(OwnedError::Unavailable)
}

pub(crate) fn validate_create(
    name: &str,
    spec: Option<&PublicationSpec>,
) -> Result<(), OwnedError> {
    if !crate::profile::valid_database_name(name) {
        return Err(OwnedError::InvalidName);
    }
    validate_binding(spec)
}

pub(super) fn validate_binding(spec: Option<&PublicationSpec>) -> Result<(), OwnedError> {
    if let Some(spec) = spec {
        if !crate::profile::valid_label_64(&spec.name) || spec.version == 0 {
            return Err(OwnedError::InvalidSpec);
        }
    }
    Ok(())
}

fn fresh(owner_id: String) -> RegistryFile {
    let owner = Entry {
        id: owner_id,
        name: Some("personal".to_owned()),
        kind: Kind::Personal,
        spec: None,
        lease_length_ms: None,
    };
    RegistryFile {
        format: FORMAT,
        owner,
        owned: Vec::new(),
    }
}

fn check(file: RegistryFile, owner_id: &str) -> Result<RegistryFile, String> {
    if file.format != FORMAT {
        return Err("unsupported database registry format".into());
    }
    require_id(&file.owner.id)?;
    if file.owner.id != owner_id {
        return Err("the personal database of this home does not come from its identity key; start a fresh home and copy the data once".into());
    }
    if file.owner.kind != Kind::Personal
        || file.owner.name.as_deref() != Some("personal")
        || file.owner.spec.is_some()
    {
        return Err("database registry owner is not the personal database".into());
    }
    let mut names = HashSet::from(["personal".to_owned()]);
    let mut ids = HashSet::from([file.owner.id.clone()]);
    for entry in &file.owned {
        accept_sibling(entry, &mut ids, &mut names)?;
    }
    Ok(file)
}

fn accept_sibling(
    entry: &Entry,
    ids: &mut HashSet<String>,
    names: &mut HashSet<String>,
) -> Result<(), String> {
    require_id(&entry.id)?;
    let name = entry
        .name
        .clone()
        .ok_or_else(|| format!("owned database {} has no name", entry.id))?;
    if (entry.kind == Kind::Personal && entry.spec.is_some())
        || !ids.insert(entry.id.clone())
        || !names.insert(name.clone())
    {
        return Err(format!(
            "owned database {} has a duplicate identity or an invalid spec",
            entry.id
        ));
    }
    validate_create(&name, entry.spec.as_ref())
        .map_err(|_| format!("owned database {name} failed its name or spec binding check"))
}

fn require_id(id: &str) -> Result<(), String> {
    mutation_log::DatabaseId::from_hex(id)
        .map(|_| ())
        .map_err(|_| format!("database id {id} is not a 64-hex database id"))
}

fn registry_path(home: &Path) -> PathBuf {
    home.join(REGISTRY_DIR).join(REGISTRY_FILE)
}
