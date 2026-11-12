//! Durable registry of owned database identity, separate from each store.

use std::collections::HashSet;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::{Entry, Kind, OwnedError, SpecBinding};

pub(super) const FORMAT: u32 = 1;
const REGISTRY_DIR: &str = "databases";
const REGISTRY_FILE: &str = "registry.json";
/// Same file name as the publication marker. The host layer does not call the backup module.
const PUBLICATION_MARKER: &str = "share-publication.json";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RegistryFile {
    pub format: u32,
    pub owner: Entry,
    pub owned: Vec<Entry>,
}

pub(super) fn load(home: &Path) -> Result<RegistryFile, String> {
    let publication = publication_id(home)?;
    match read(home)? {
        Some(file) => check(file, publication.as_deref()),
        None => fresh(publication),
    }
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
    /// The temp file was renamed onto `registry.json`. The previous file is gone.
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
        .map_err(|error| format!("sync database registry: {error}"))?;
    Ok(())
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

pub(crate) fn validate_create(name: &str, spec: Option<&SpecBinding>) -> Result<(), OwnedError> {
    if !valid_database_name(name) {
        return Err(OwnedError::InvalidName);
    }
    validate_binding(spec)
}

pub(super) fn validate_binding(spec: Option<&SpecBinding>) -> Result<(), OwnedError> {
    if let Some(spec) = spec {
        if !valid_label(&spec.name) || spec.version == 0 {
            return Err(OwnedError::InvalidSpec);
        }
    }
    Ok(())
}

fn fresh(publication: Option<String>) -> Result<RegistryFile, String> {
    let owner = if let Some(id) = publication {
        Entry {
            id,
            name: None,
            kind: Kind::Shareable,
            spec: None,
        }
    } else {
        Entry {
            id: fresh_id(&HashSet::new()).map_err(|_| "could not allocate a database id")?,
            name: Some("personal".to_owned()),
            kind: Kind::Personal,
            spec: None,
        }
    };
    Ok(RegistryFile {
        format: FORMAT,
        owner,
        owned: Vec::new(),
    })
}

fn check(file: RegistryFile, publication: Option<&str>) -> Result<RegistryFile, String> {
    if file.format != FORMAT {
        return Err("unsupported database registry format".into());
    }
    require_id(&file.owner.id)?;
    owner_matches(&file.owner, publication)?;
    let mut names = HashSet::new();
    let mut ids = HashSet::from([file.owner.id.clone()]);
    if let Some(name) = file.owner.name.clone() {
        names.insert(name);
    }
    for entry in &file.owned {
        accept_sibling(entry, &mut ids, &mut names)?;
    }
    Ok(file)
}

fn owner_matches(owner: &Entry, publication: Option<&str>) -> Result<(), String> {
    match publication {
        Some(id)
            if owner.id != id
                || owner.name.is_some()
                || owner.kind != Kind::Shareable
                || owner.spec.is_some() =>
        {
            Err("database registry does not match the publication id".into())
        }
        None if owner.kind != Kind::Personal
            || owner.name.as_deref() != Some("personal")
            || owner.spec.is_some() =>
        {
            Err("database registry owner is not the personal database".into())
        }
        _ => Ok(()),
    }
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
        .map_err(|_| format!("owned database {name} failed its name or spec binding check"))?;
    Ok(())
}

fn valid_database_name(name: &str) -> bool {
    valid_label(name)
        && !name.eq_ignore_ascii_case("personal")
        && uuid::Uuid::parse_str(name).is_err()
}

fn valid_label(name: &str) -> bool {
    (1..=64).contains(&name.len())
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn require_id(id: &str) -> Result<(), String> {
    cloud_backup_protocol::Id::try_from(id.to_owned())
        .map(|_| ())
        .map_err(|_| format!("database id {id} is not a UUID"))
}

fn publication_id(home: &Path) -> Result<Option<String>, String> {
    let path = home.join(PUBLICATION_MARKER);
    if !path.is_file() {
        return Ok(None);
    }
    #[derive(Deserialize)]
    struct Marker {
        version: u8,
        id: String,
    }
    let marker: Marker = serde_json::from_slice(
        &std::fs::read(&path).map_err(|error| format!("read publication marker: {error}"))?,
    )
    .map_err(|_| "invalid share publication marker".to_owned())?;
    if marker.version != 1 {
        return Err("unsupported share publication marker version".into());
    }
    require_id(&marker.id)?;
    Ok(Some(marker.id))
}

fn registry_path(home: &Path) -> PathBuf {
    home.join(REGISTRY_DIR).join(REGISTRY_FILE)
}
