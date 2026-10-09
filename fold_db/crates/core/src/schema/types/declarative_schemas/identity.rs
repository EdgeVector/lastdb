//! Schema identity for `fold_db`'s schema type.
//!
//! There is no algorithm in this file. It lives once, in the leaf crate
//! `schema_types` (`compute_identity_hash_parts`), which `fold_db_core`
//! already depends on. This file is accessor glue that hands `fold_db`'s
//! fields to it.
//!
//! This used to be a byte-identical *copy* of the service's function, kept in
//! sync by a doc comment reading "Must match Schema Service". The comments
//! drifted first; the logic drifted later. A node binary that re-hashed
//! without the `:key:` segment after loading a catalog-keyed schema stored
//! `name = catalog_hash` while recomputing a different `identity_hash` —
//! keyed-readable / scan-invisible for any keyed local-declared type
//! (Papercut 2026-08-06), and 43 rows on the primary still carry that stale
//! hash. The compiler now enforces what the comment was asking for.

use super::DeclarativeSchemaDefinition;
use schema_types::{
    canonical_name_parts, compute_identity_hash_parts, refuses_identity_downgrade,
    IdentityRecompute, IDENTITY_HASH_ALGO_VERSION, LEGACY_IDENTITY_HASH_ALGO_VERSION,
};

impl DeclarativeSchemaDefinition {
    /// Compute and store this schema's identity hash, stamping it with the
    /// algorithm version that produced it.
    ///
    /// The algorithm — readable name + sorted, deduplicated field names + key
    /// layout, optionally namespaced by `owner_app_id` — is documented on
    /// [`schema_types::compute_identity_hash_parts`], which this calls.
    ///
    /// This is the **ungated** recompute: it always overwrites. Paths that may
    /// be handed an identity minted elsewhere — chiefly
    /// `SchemaCore::load_schema_internal` — must call
    /// [`Self::recompute_identity_hash_unless_newer`] instead.
    pub fn compute_identity_hash(&mut self) {
        self.identity_hash = Some(compute_identity_hash_parts(
            self.owner_app_id.as_deref(),
            self.descriptive_name.as_deref(),
            &self.name,
            self.fields
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(String::as_str),
            self.key.as_ref(),
        ));
        self.identity_hash_algo_version = Some(IDENTITY_HASH_ALGO_VERSION);
    }

    /// The algorithm version that minted this schema's `identity_hash`.
    ///
    /// An unstamped row reports [`LEGACY_IDENTITY_HASH_ALGO_VERSION`].
    #[must_use]
    pub fn identity_hash_algo_version(&self) -> u32 {
        self.identity_hash_algo_version
            .unwrap_or(LEGACY_IDENTITY_HASH_ALGO_VERSION)
    }

    /// True when this schema's identity was minted by an algorithm newer than
    /// this binary implements — i.e. the node is older than its own data.
    #[must_use]
    pub fn identity_is_newer_than_binary(&self) -> bool {
        refuses_identity_downgrade(
            self.identity_hash.as_deref(),
            self.identity_hash_algo_version,
        )
        .is_some()
    }

    /// Recompute the identity hash **unless** the one already carried was
    /// minted by a newer algorithm, in which case it is left untouched.
    ///
    /// Thin accessor glue over [`schema_types::refuses_identity_downgrade`],
    /// which holds the rule.
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

    /// The app-namespaced canonical name for this schema.
    ///
    /// - `owner_app_id == Some("fbrain")`, `name == "Concept"` -> `"fbrain/Concept"`.
    /// - `owner_app_id == None` -> the bare `name` (legacy behavior — schemas
    ///   without an owning app keep their un-namespaced name).
    ///
    /// Inverse of [`Self::parse_canonical_name`]. See app_identity v3.1, Lane B2a.
    #[must_use]
    pub fn canonical_name(&self) -> String {
        canonical_name_parts(self.owner_app_id.as_deref(), &self.name)
    }

    /// Split a canonical name into `(owner_app_id, name)`.
    ///
    /// - `"fbrain/Concept"` -> `(Some("fbrain"), "Concept")`.
    /// - `"Concept"` (no slash) -> `(None, "Concept")` — a legacy, un-namespaced name.
    ///
    /// Splits on the **first** `/` only, so a schema name that itself
    /// contains slashes stays intact in the returned `name`. Inverse of
    /// [`DeclarativeSchemaDefinition::canonical_name`].
    #[must_use]
    pub fn parse_canonical_name(canonical: &str) -> (Option<String>, &str) {
        schema_types::parse_canonical_name(canonical)
    }

    /// Deduplicate the fields list in-place, preserving order.
    /// Used by schema_service when materializing/normalizing schemas.
    pub fn dedup_fields(&mut self) {
        if let Some(ref mut fields) = self.fields {
            let mut seen = std::collections::HashSet::new();
            fields.retain(|f| seen.insert(f.clone()));
        }
    }

    /// Get the identity hash
    #[must_use]
    pub fn get_identity_hash(&self) -> Option<&String> {
        self.identity_hash.as_ref()
    }
}
