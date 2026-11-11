//! Schema registry, retention policies, and core behaviors over `lastdb_model::schema`.

pub mod core;
pub mod manifest_retention;
pub mod schema_types;
pub mod types;

pub use core::SchemaCore;
pub use lastdb_model::schema::logical_rules::{require_logical_filter, require_logical_layout};
pub use manifest_retention::{
    min_manifest_ttl_seconds, ManifestRetention, ManifestRetentionOutcome, ManifestRetentionPlan,
    ManifestRetentionStatus, MAX_MANIFEST_TTL_SECONDS, MIN_MANIFEST_TTL_ENV,
    MIN_MANIFEST_TTL_SECONDS,
};
pub use schema_types::{
    RetentionSource, SchemaListEntry, SchemaRetentionPolicy, SchemaState, SchemaWithState,
};
pub use types::{Schema, SchemaError};

pub mod mutation_author {
    use crate::schema::types::author_clock::mutation_author_canonical_bytes;
    use crate::schema::types::Mutation;
    use crate::security::{Ed25519KeyPair, KeyUtils};

    #[must_use]
    pub fn sign_mutation_author_clock(mutation: &Mutation, keypair: &Ed25519KeyPair) -> String {
        let signature = keypair.sign(&mutation_author_canonical_bytes(mutation));
        KeyUtils::signature_to_base64(&signature)
    }
}
