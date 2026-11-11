//! Read A's registry. This lane does not write it.
//!
//! The file is `<home>/databases/registry.json`. The owner row is the existing
//! database. Each `owned` row is a named database this node created. Received
//! shares are not in this file.

use std::path::Path;

use serde::Deserialize;

use super::disk;

const REGISTRY_LIMIT: usize = 65_536;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Spec {
    pub name: String,
    pub version: u32,
}

/// One database this node owns. `native` is an owned sibling, not the owner row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Database {
    pub id: String,
    pub kind: String,
    pub spec: Option<Spec>,
    pub native: bool,
}

#[derive(Deserialize)]
struct RegistryFile {
    format: u64,
    owner: EntryRow,
    #[serde(default)]
    owned: Vec<EntryRow>,
}

#[derive(Deserialize)]
struct EntryRow {
    id: String,
    kind: String,
    #[serde(default)]
    spec: Option<SpecRow>,
}

#[derive(Deserialize)]
struct SpecRow {
    name: String,
    version: u32,
}

/// Owner row, then each usable owned sibling.
pub(crate) fn databases(home: &Path) -> Vec<Database> {
    let Some(file) = read_registry(home) else {
        return Vec::new();
    };
    if file.format != 1 {
        return Vec::new();
    }
    let Some(owner) = accept(&file.owner, false) else {
        return Vec::new();
    };
    let mut rows = vec![owner];
    for entry in &file.owned {
        let Some(row) = accept(entry, true) else {
            continue;
        };
        if rows.iter().any(|existing| existing.id == row.id) {
            continue;
        }
        rows.push(row);
    }
    rows
}

/// Owner database. Byte backup still follows this store only.
pub(crate) fn owner(home: &Path) -> Option<Database> {
    databases(home).into_iter().find(|row| !row.native)
}

/// Database whose store is `data_dir`. The owner store is `<home>/data`.
/// An owned sibling store is `<home>/databases/<id>/data`.
pub(crate) fn database_at(home: &Path, data_dir: &Path) -> Option<Database> {
    let rows = databases(home);
    if data_dir == home.join("data") {
        return rows.into_iter().find(|row| !row.native);
    }
    let id = sibling_id(home, data_dir)?;
    rows.into_iter().find(|row| row.native && row.id == id)
}

fn read_registry(home: &Path) -> Option<RegistryFile> {
    let path = home.join("databases").join("registry.json");
    let bytes = disk::read_limited(&path, REGISTRY_LIMIT).ok()??;
    serde_json::from_slice(&bytes).ok()
}

fn accept(row: &EntryRow, native: bool) -> Option<Database> {
    if !valid_id(&row.id) || !valid_kind(&row.kind) {
        return None;
    }
    if native && row.kind != "shareable" {
        return None;
    }
    let spec = match &row.spec {
        None => None,
        Some(spec) if valid_spec(spec) => Some(Spec {
            name: spec.name.clone(),
            version: spec.version,
        }),
        Some(_) if native => return None,
        Some(_) => None,
    };
    Some(Database {
        id: row.id.clone(),
        kind: row.kind.clone(),
        spec,
        native,
    })
}

fn sibling_id(home: &Path, data_dir: &Path) -> Option<String> {
    let relative = data_dir.strip_prefix(home.join("databases")).ok()?;
    let mut parts = relative.components();
    let id = parts.next()?.as_os_str().to_str()?;
    if parts.next()?.as_os_str() != "data" || parts.next().is_some() {
        return None;
    }
    Some(id.to_owned())
}

fn valid_spec(spec: &SpecRow) -> bool {
    spec.version >= 1
        && (1..=64).contains(&spec.name.len())
        && spec
            .name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn valid_kind(kind: &str) -> bool {
    kind == "personal" || kind == "shareable"
}

fn valid_id(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()
            }
        })
        && value.bytes().any(|byte| byte != b'0' && byte != b'-')
}
