//! Durable registry of owned database identity, separate from each store.

use std::collections::HashSet;
use std::io::Write;
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
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RegistryFile {
    pub format: u32,
    pub owner: Entry,
    #[serde(default)]
    pub owned: Vec<Entry>,
}

pub(super) fn load(home: &Path) -> Result<RegistryFile, String> {
    match read(home)? {
        Some(file) => check(file),
        None => fresh(),
    }
}

pub(crate) fn prepare_snapshot_restore(
    home: &Path,
    database_id: String,
    owned_name: Option<&str>,
) -> Result<(), String> {
    let mut file = load(home)?;
    if let Some(name) = owned_name {
        validate_create(name, None).map_err(|err| err.status().1)?;
        file.owned.push(Entry {
            id: database_id,
            name: Some(name.to_owned()),
            kind: Kind::Personal,
            spec: None,
        });
    } else {
        file.owner.id = database_id;
    }
    write(home, &file).map_err(|e| e.to_string())
}

/// Owner row, then each usable owned sibling.
pub(crate) fn registered_databases(home: &Path) -> Vec<RegisteredDatabase> {
    let Some(bytes) =
        crate::profile::private_fs::read_limited(&registry_path(home), REGISTRY_LIMIT)
            .ok()
            .flatten()
    else {
        return Vec::new();
    };
    let Ok(file) = serde_json::from_slice::<RegistryFile>(&bytes) else {
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
    })
}

/// Resolve one shareable sibling by id or exact name.
pub(crate) fn find_shareable(home: &Path, selector: &str) -> Result<Entry, String> {
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
    if entry.spec.is_none() {
        return Err("share creation requires a spec binding".into());
    }
    if cloud_backup_protocol::Id::try_from(entry.id.clone()).is_err() {
        return Err("database registry is unavailable".into());
    }
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
    let path = registry_path(home);
    if !path.is_file() {
        return Ok(None);
    }
    let bytes = std::fs::read(&path).map_err(|error| format!("read database registry: {error}"))?;
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
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
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

pub(super) fn fresh_id(used: &HashSet<String>) -> Result<String, OwnedError> {
    for _ in 0..8 {
        let id = uuid::Uuid::new_v4().to_string();
        if cloud_backup_protocol::Id::try_from(id.clone()).is_ok() && !used.contains(&id) {
            return Ok(id);
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

fn fresh() -> Result<RegistryFile, String> {
    let owner = Entry {
        id: fresh_id(&HashSet::new()).map_err(|_| "could not allocate a database id")?,
        name: Some("personal".to_owned()),
        kind: Kind::Personal,
        spec: None,
    };
    Ok(RegistryFile {
        format: FORMAT,
        owner,
        owned: Vec::new(),
    })
}

fn check(file: RegistryFile) -> Result<RegistryFile, String> {
    if file.format != FORMAT {
        return Err("unsupported database registry format".into());
    }
    require_id(&file.owner.id)?;
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
    cloud_backup_protocol::Id::try_from(id.to_owned())
        .map(|_| ())
        .map_err(|_| format!("database id {id} is not a UUID"))
}

fn registry_path(home: &Path) -> PathBuf {
    home.join(REGISTRY_DIR).join(REGISTRY_FILE)
}
