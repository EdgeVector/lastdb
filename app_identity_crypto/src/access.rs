//! The LastDB access a listed app declares.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// The app API extensions this build can grant to a listed app.
pub const KNOWN_EXTENSIONS: &[&str] = &["changes", "databases"];

/// The access value that means every schema, in place of a name list or a map.
pub const ALL_SCHEMAS: &str = "*";

/// First API version whose registry lists may carry [`ALL_SCHEMAS`].
///
/// An older build that cannot parse that value fails the whole list for that
/// channel and API version. Lists below this version stay maps only.
pub const ALL_SCHEMAS_API_VERSION: u32 = 38;

const MAX_ENTRIES: usize = 256;

/// One side of an access block.
///
/// `Named` serializes as the inner map. This enum is untagged, so that case
/// has the same JSON bytes as a `BTreeMap<String, String>` field. `All`
/// serializes as a JSON string. `well_formed` accepts only [`ALL_SCHEMAS`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SchemaSet {
    /// Schema name to the definition id of the one version the app was built against.
    Named(BTreeMap<String, String>),
    /// A string in place of the map. Only [`ALL_SCHEMAS`] is well formed.
    All(String),
}

impl Default for SchemaSet {
    fn default() -> Self {
        Self::Named(BTreeMap::new())
    }
}

impl SchemaSet {
    fn entries_ok(&self) -> bool {
        match self {
            Self::All(marker) => marker == ALL_SCHEMAS,
            Self::Named(map) => {
                map.len() <= MAX_ENTRIES
                    && map
                        .iter()
                        .all(|(name, id)| schema_name(name) && definition_id(id))
            }
        }
    }

    /// The name-to-definition map, or `None` when this side is a string.
    #[must_use]
    pub fn named(&self) -> Option<&BTreeMap<String, String>> {
        match self {
            Self::Named(map) => Some(map),
            Self::All(_) => None,
        }
    }

    /// Registration value: `"*"` or the schema names, without definition ids.
    #[must_use]
    pub fn registration_value(&self) -> Value {
        match self {
            Self::Named(map) => json!(map.keys().collect::<Vec<_>>()),
            Self::All(marker) => json!(marker),
        }
    }
}

/// What a listed app asks to do on a node. The publisher signs it with the
/// rest of the listing, so the registry cannot widen it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Access {
    /// Schemas the app reads: a name-to-definition map, or `"*"`.
    pub read: SchemaSet,
    /// Schemas the app writes, in the same form.
    pub write: SchemaSet,
    /// App API extensions the app calls, by name (`POST /v1/x/<name>`).
    pub extensions: Vec<String>,
}

impl Access {
    /// True when every schema name, definition ID and extension name is well formed.
    /// A string other than [`ALL_SCHEMAS`] is not well formed.
    #[must_use]
    pub fn well_formed(&self) -> bool {
        self.read.entries_ok()
            && self.write.entries_ok()
            && self.extensions.len() <= MAX_ENTRIES
            && self.extensions.iter().all(|name| extension_name(name))
    }

    /// The declared extensions this build cannot grant.
    #[must_use]
    pub fn unknown_extensions(&self) -> Vec<&str> {
        self.extensions
            .iter()
            .map(String::as_str)
            .filter(|name| !KNOWN_EXTENSIONS.contains(name))
            .collect()
    }
}

fn schema_name(value: &str) -> bool {
    (1..=128).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'/' | b'-'))
}

fn definition_id(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn extension_name(value: &str) -> bool {
    (1..=40).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}
