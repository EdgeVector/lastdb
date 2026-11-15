//! Offline registry lookup for a shareable named database.
//!
//! This reads `databases/registry.json` only. It does not create a registry
//! and it does not open a store.

use std::path::Path;

use super::{registry, Entry, Kind};

/// Resolve one shareable sibling by id or exact name.
pub(crate) fn find_shareable(home: &Path, selector: &str) -> Result<Entry, String> {
    if !crate::profile::valid_label_64(selector) {
        return Err("invalid database selector".into());
    }
    let file = registry::read(home)
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
///
/// A missing registry is not a claim. An unreadable registry is an error.
pub(crate) fn registry_claims(home: &Path, id: &str, name: Option<&str>) -> Result<bool, ()> {
    let Some(file) = registry::read(home).map_err(|_| ())? else {
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
