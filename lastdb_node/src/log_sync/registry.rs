//! Read A's registry owner row. This lane does not write the registry.

use std::path::Path;

use serde::Deserialize;

use super::disk;

const REGISTRY_LIMIT: usize = 65_536;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Owner {
    pub id: String,
    pub kind: String,
}

#[derive(Deserialize)]
struct RegistryFile {
    format: u64,
    owner: OwnerRow,
}

#[derive(Deserialize)]
struct OwnerRow {
    id: String,
    kind: String,
}

/// Owner database for this home, when the registry file is present and usable.
pub(crate) fn owner(home: &Path) -> Option<Owner> {
    let path = home.join("databases").join("registry.json");
    let bytes = disk::read_limited(&path, REGISTRY_LIMIT).ok()??;
    let file: RegistryFile = serde_json::from_slice(&bytes).ok()?;
    if file.format != 1 || !valid_id(&file.owner.id) || !valid_kind(&file.owner.kind) {
        return None;
    }
    Some(Owner {
        id: file.owner.id,
        kind: file.owner.kind,
    })
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
