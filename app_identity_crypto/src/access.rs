//! The LastDB access a listed app declares.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// The LastDB features this build can grant to a listed app.
pub const KNOWN_FEATURES: &[&str] = &["changes"];

const MAX_ENTRIES: usize = 256;

/// What a listed app asks to do on a node. The publisher signs it with the
/// rest of the listing, so the registry cannot widen it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Access {
    /// Schemas the app reads: the schema name, and the definition ID of the
    /// one version the app was built against.
    pub read: BTreeMap<String, String>,
    /// Schemas the app writes, in the same form.
    pub write: BTreeMap<String, String>,
    /// Named LastDB features beyond schema reads and writes.
    pub features: Vec<String>,
}

impl Access {
    /// True when every schema name, definition ID and feature name is well formed.
    #[must_use]
    pub fn well_formed(&self) -> bool {
        self.read.len() <= MAX_ENTRIES
            && self.write.len() <= MAX_ENTRIES
            && self.features.len() <= MAX_ENTRIES
            && self
                .read
                .iter()
                .chain(self.write.iter())
                .all(|(name, id)| schema_name(name) && definition_id(id))
            && self.features.iter().all(|name| feature_name(name))
    }

    /// The declared features this build cannot grant.
    #[must_use]
    pub fn unknown_features(&self) -> Vec<&str> {
        self.features
            .iter()
            .map(String::as_str)
            .filter(|name| !KNOWN_FEATURES.contains(name))
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

fn feature_name(value: &str) -> bool {
    (1..=40).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}
