use super::DeclarativeSchemaDefinition;
use crate::KeyConfig;
use sha2::{Digest, Sha256};

pub const IDENTITY_HASH_ALGO_VERSION: u32 = 4;
const RECORD_KEYS_IDENTITY_HASH_ALGO_VERSION: u32 = 3;
const FIELD_DERIVED_IDENTITY_HASH_ALGO_VERSION: u32 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityRecompute {
    Recomputed,
    RefusedDowngrade { stored: u32, binary: u32 },
}

impl IdentityRecompute {
    #[must_use]
    pub fn refused(self) -> bool {
        matches!(self, Self::RefusedDowngrade { .. })
    }
}

#[must_use]
pub fn refuses_identity_downgrade(
    stored_hash: Option<&str>,
    stored_version: Option<u32>,
) -> Option<u32> {
    stored_version.filter(|stored| {
        stored_hash.is_some_and(|h| !h.is_empty()) && *stored > IDENTITY_HASH_ALGO_VERSION
    })
}

#[must_use]
pub fn compute_identity_hash_parts<'a, I>(
    owner_app_id: Option<&str>,
    descriptive_name: Option<&str>,
    name: &str,
    fields: I,
    key: Option<&KeyConfig>,
) -> String
where
    I: IntoIterator<Item = &'a str>,
{
    let mut field_names: Vec<&str> = fields.into_iter().collect();
    field_names.sort_unstable();
    field_names.dedup();
    let combined = field_names.join(",");
    let mut hasher = Sha256::new();

    if let Some(app_id) = owner_app_id.filter(|s| !s.is_empty()) {
        hasher.update(b"app:");
        hasher.update(app_id.as_bytes());
        hasher.update(b":");
    }

    let readable_name = descriptive_name.filter(|s| !s.is_empty()).unwrap_or(name);
    hasher.update(readable_name.as_bytes());
    hasher.update(b":");
    hasher.update(combined.as_bytes());

    if let Some(key) = key {
        let hf = key
            .hash_field
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or_default();
        let rf = key
            .range_field
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or_default();
        if !hf.is_empty() || !rf.is_empty() {
            hasher.update(b":key:");
            hasher.update(hf.as_bytes());
            hasher.update(b":");
            hasher.update(rf.as_bytes());
        }
    }

    format!("{:x}", hasher.finalize())
}

#[must_use]
pub fn parse_canonical_name(canonical: &str) -> (Option<String>, &str) {
    match canonical.split_once('/') {
        Some((app_id, name)) if !app_id.is_empty() => (Some(app_id.to_string()), name),
        _ => (None, canonical),
    }
}

impl DeclarativeSchemaDefinition {
    pub fn compute_identity_hash(&mut self) {
        let base = compute_identity_hash_parts(
            self.owner_app_id.as_deref(),
            self.descriptive_name.as_deref(),
            &self.name,
            self.fields
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(String::as_str),
            self.key.as_ref(),
        );
        if let Some(keys) = &self.record_keys {
            let mut hash = Sha256::new();
            hash.update(b"lastdb:record-schema:v1\0");
            hash.update(base.as_bytes());
            hash.update(serde_json::to_vec(keys).expect("record key definitions serialize"));
            if self.file_required {
                hash.update(b"\0file-required");
            }
            self.identity_hash = Some(format!("{:x}", hash.finalize()));
            self.identity_hash_algo_version = Some(if self.file_required {
                IDENTITY_HASH_ALGO_VERSION
            } else {
                RECORD_KEYS_IDENTITY_HASH_ALGO_VERSION
            });
        } else {
            self.identity_hash = Some(base);
            self.identity_hash_algo_version = Some(FIELD_DERIVED_IDENTITY_HASH_ALGO_VERSION);
        }
    }

    #[must_use]
    pub fn identity_is_newer_than_binary(&self) -> bool {
        refuses_identity_downgrade(
            self.identity_hash.as_deref(),
            self.identity_hash_algo_version,
        )
        .is_some()
    }

    #[must_use = "a refused downgrade must be surfaced, not dropped"]
    pub fn recompute_identity_hash_unless_newer(&mut self) -> IdentityRecompute {
        if let Some(stored) = refuses_identity_downgrade(
            self.identity_hash.as_deref(),
            self.identity_hash_algo_version,
        ) {
            return IdentityRecompute::RefusedDowngrade {
                stored,
                binary: IDENTITY_HASH_ALGO_VERSION,
            };
        }
        self.compute_identity_hash();
        IdentityRecompute::Recomputed
    }

    #[must_use]
    pub fn parse_canonical_name(canonical: &str) -> (Option<String>, &str) {
        parse_canonical_name(canonical)
    }

    #[must_use]
    pub fn get_identity_hash(&self) -> Option<&String> {
        self.identity_hash.as_ref()
    }
}
