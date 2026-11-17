//! The schemas and extensions named in an app manifest.
//!
//! This is the manifest block. It is not the signed listing, which pins each
//! schema name to a definition ID under `read` and `write`.

use std::collections::HashSet;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::valid_app_id;

/// Lists from `lastdb-app.json` `access`. Absent on a manifest that has no block.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestAccess {
    #[serde(default)]
    pub reads: Vec<String>,
    #[serde(default)]
    pub writes: Vec<String>,
    #[serde(default)]
    pub extensions: Vec<String>,
}

impl ManifestAccess {
    /// `None` when the manifest has no `access` key.
    pub(super) fn from_manifest(manifest: &Value) -> Result<Option<Self>, String> {
        manifest.get("access").map(Self::parse_block).transpose()
    }

    /// Parse one `access` object. Publish uses this before it pins definition IDs.
    pub(crate) fn parse_block(block: &Value) -> Result<Self, String> {
        let access = Self::deserialize(block).map_err(|e| format!("manifest access: {e}"))?;
        access.check()?;
        Ok(access)
    }

    fn check(&self) -> Result<(), String> {
        check_names("reads", &self.reads, schema_ref, "must be <app id>/<Name>")?;
        check_names(
            "writes",
            &self.writes,
            schema_ref,
            "must be <app id>/<Name>",
        )?;
        check_names(
            "extensions",
            &self.extensions,
            extension_word,
            "must be a lowercase word ([a-z][a-z0-9_]*, at most 40 characters)",
        )?;
        Ok(())
    }
}

/// When the manifest names extensions, the node must list each one.
/// `GET /api/version` with no `extensions` field means the node has none.
pub(super) fn require_declared_extensions(
    home: &Path,
    app: &str,
    access: Option<&ManifestAccess>,
) -> Result<(), String> {
    let Some(access) = access.filter(|a| !a.extensions.is_empty()) else {
        return Ok(());
    };
    let have = node_extensions(home)?;
    let known: HashSet<&str> = have.iter().map(String::as_str).collect();
    let missing: Vec<&str> = access
        .extensions
        .iter()
        .map(String::as_str)
        .filter(|name| !known.contains(name))
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    Err(format!(
        "app '{app}' needs the app API extension {}, which this LastDB does not have; upgrade LastDB",
        missing.join(", ")
    ))
}

fn node_extensions(home: &Path) -> Result<Vec<String>, String> {
    let sock = lastdb_node::lastdb_uds::socket_path(&home.join("data"));
    let raw =
        crate::uds_client::get_path(&sock, "/api/version", std::time::Duration::from_secs(2))?;
    let value = crate::uds_client::parse_json_response(&raw, "version")?;
    let Some(field) = value.get("extensions") else {
        return Ok(Vec::new());
    };
    let list = field
        .as_array()
        .ok_or("GET /api/version extensions is not a list")?;
    list.iter()
        .map(|item| {
            item.as_str()
                .map(str::to_owned)
                .ok_or_else(|| "GET /api/version extensions must be strings".to_string())
        })
        .collect()
}

fn check_names(
    field: &str,
    names: &[String],
    valid: fn(&str) -> bool,
    why: &str,
) -> Result<(), String> {
    let mut seen = HashSet::new();
    for name in names {
        if name.is_empty() {
            return Err(format!("manifest access.{field} contains an empty name"));
        }
        if !valid(name) {
            return Err(format!("manifest access.{field} entry '{name}' {why}"));
        }
        if !seen.insert(name.as_str()) {
            return Err(format!(
                "manifest access.{field} lists '{name}' more than once"
            ));
        }
    }
    Ok(())
}

/// `<app id>/<Name>`. The app id uses [`valid_app_id`]. The whole name stays
/// inside the listing's 128-character schema-name bound.
fn schema_ref(value: &str) -> bool {
    if !(1..=128).contains(&value.len()) {
        return false;
    }
    let Some((app, name)) = value.split_once('/') else {
        return false;
    };
    !name.is_empty() && !name.contains('/') && valid_app_id(app) && schema_word(name)
}

fn schema_word(name: &str) -> bool {
    let mut bytes = name.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    first.is_ascii_alphabetic()
        && bytes.all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
}

/// `[a-z][a-z0-9_]*`, capped at 40 so a listed extension name still accepts it.
fn extension_word(value: &str) -> bool {
    let mut bytes = value.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    (1..=40).contains(&value.len())
        && first.is_ascii_lowercase()
        && bytes.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}
