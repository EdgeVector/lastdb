//! Match canonical field identities and describe member key layouts.

use crate::schema::types::key_config::KeyConfig;
use crate::schema::types::DeclarativeSchemaDefinition;
use std::collections::HashMap;

/// Fingerprint of a schema's key layout for equality (not product names).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KeyLayoutFingerprint {
    pub hash_field: Option<String>,
    pub range_field: Option<String>,
}

impl KeyLayoutFingerprint {
    #[must_use]
    pub fn from_key(key: Option<&KeyConfig>) -> Self {
        match key {
            None => Self {
                hash_field: None,
                range_field: None,
            },
            Some(k) => Self {
                hash_field: k.hash_field.clone(),
                range_field: k.range_field.clone(),
            },
        }
    }

    #[must_use]
    pub fn from_schema(schema: &DeclarativeSchemaDefinition) -> Self {
        Self::from_key(schema.key.as_ref())
    }
}

/// One field two sibling schemas agree on.
///
/// The two local names are carried separately because matching is on
/// **identity**, not on the local field name — a schema calling the field
/// `name` binds with one calling it `title`. For every legacy pair the two are
/// equal, because a v1 identity hashes the field name and so cannot match
/// across a rename.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedFieldIdentity {
    /// Local field name in the first schema.
    pub a_field: String,
    /// Local field name in the second schema.
    pub b_field: String,
    pub field_hash: String,
}

/// Fields carried by both schemas under the same identity.
///
/// Matching is on identity **alone**. The previous form additionally required
/// `b.field_hashes[field] == hash` — the same *name* as well as the same
/// identity — which silently made renames unbindable. That extra condition was
/// invisible for v1 identities (which hash the name, so a rename already
/// changes the identity) and only became wrong once a declared identity could
/// outlive a local rename.
#[must_use]
pub fn shared_field_identities(
    a: &DeclarativeSchemaDefinition,
    b: &DeclarativeSchemaDefinition,
) -> Vec<SharedFieldIdentity> {
    // identity -> b's local field name. A well-formed schema carries an
    // identity once; if it somehow repeats, take the lexicographically first
    // name so repeated loads bind the same way rather than by hash order.
    let mut b_by_identity: HashMap<&str, &str> = HashMap::new();
    for (field, hash) in &b.field_hashes {
        if hash.is_empty() {
            continue;
        }
        b_by_identity
            .entry(hash.as_str())
            .and_modify(|existing| {
                if field.as_str() < *existing {
                    *existing = field.as_str();
                }
            })
            .or_insert(field.as_str());
    }

    let mut shared: Vec<SharedFieldIdentity> = a
        .field_hashes
        .iter()
        .filter(|(_, h)| !h.is_empty())
        .filter_map(|(field, h)| {
            b_by_identity
                .get(h.as_str())
                .map(|b_field| SharedFieldIdentity {
                    a_field: field.clone(),
                    b_field: (*b_field).to_string(),
                    field_hash: h.clone(),
                })
        })
        .collect();
    // Stable order so repeated loads bind in the same sequence.
    shared.sort_by(|x, y| x.a_field.cmp(&y.a_field));
    shared
}
