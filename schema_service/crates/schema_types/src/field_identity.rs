//! Schema Service field identity: stable hash over name + description + type + version.
//!
//! Local nodes read the catalog `field_hash` and interpret:
//! - same hash + same key layout → mapped / shared payload molecule
//! - same hash + different key layout → one protein per field (fold coherence)
//!
//! See brain `design-lastdb-field-hash-auto-protein`.

use crate::FieldValueType;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Inputs Schema Service mints from. Owner/app is intentionally **not** part of
/// the hash — field identity is catalog-global across apps.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldIdentityInputs {
    pub name: String,
    pub description: String,
    pub field_type: FieldValueType,
    /// Bump when meaning or type changes incompatibly. Default 1.
    pub version: u32,
}

impl FieldIdentityInputs {
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        field_type: FieldValueType,
        version: u32,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            field_type,
            version,
        }
    }

    /// SHA-256 hex of canonical JSON for the four identity inputs.
    #[must_use]
    pub fn hash(&self) -> String {
        compute_field_hash(
            &self.name,
            &self.description,
            &self.field_type,
            self.version,
        )
    }
}

/// Canonical field hash used on catalog schemas and local runtime metadata.
///
/// Serialization is stable: sorted JSON object with fixed keys. Description is
/// trimmed; name is trimmed (not lowercased — case is part of identity).
#[must_use]
pub fn compute_field_hash(
    name: &str,
    description: &str,
    field_type: &FieldValueType,
    version: u32,
) -> String {
    let type_json = serde_json::to_string(field_type).unwrap_or_else(|_| "\"Any\"".to_string());
    // Fixed key order — do not use HashMap.
    let payload = format!(
        r#"{{"description":{},"name":{},"type":{},"version":{}}}"#,
        serde_json::to_string(description.trim()).unwrap_or_else(|_| "\"\"".into()),
        serde_json::to_string(name.trim()).unwrap_or_else(|_| "\"\"".into()),
        type_json,
        version
    );
    let digest = Sha256::digest(payload.as_bytes());
    crate::hex::hex_lower(&digest)
}

// ===========================================================================
// Declared-field identity (v2) — scoped to an SS-minted declaration id.
// ===========================================================================

/// Version of the **declared**-field identity algorithm.
///
/// v1 is [`compute_field_hash`] above: network-global, keyed on
/// name + description + type + version. That scope is right for answering
/// *what does this field mean* (search, sensitivity, classification) and wrong
/// for answering *should a write here land there*. Measured on the live
/// primary: `created_at` / `"RFC 3339 timestamp"` / `String` is byte-identical
/// across **46 unrelated schemas**.
///
/// v2 scopes identity to a declaration instead of to meaning. Two fields are
/// the same field only when someone passed the same handle.
pub const DECLARED_FIELD_IDENTITY_ALGO_VERSION: u32 = 2;

/// Declared-field identity — the value a schema stamps into `field_hashes`,
/// and (per `design-lastdb-declared-fields`) the protein id itself.
///
/// `declaration_id` is minted by Schema Service when an app declares a field
/// and is immutable for the life of that declaration. Everything else here is
/// content the declarer supplied.
///
/// **Deliberately excluded from the identity:**
///
/// - `owner_app_id` — 949 of 1,141 live schemas carry none, so an app-keyed
///   identity would serve a minority; and ownership is *mutable* while identity
///   is not. Transferring a declaration to another app must leave every field
///   identity byte-identical, or a permission change would cascade into a
///   re-identification of every protein and every stamped schema. Ownership is
///   an attribute of the declaration, never an input to the hash.
/// - `description` — free text that gets corrected. Hashing it would mean a
///   typo fix silently re-identifies the field and unbinds every schema that
///   already stamped it. `version` is the deliberate signal for an
///   incompatible change; prose is not.
///
/// Because the formula is derivable, a node holding a schema that already
/// carries a `declaration_id` can recompute and verify the identity **offline**
/// and get exactly what Schema Service got. Only *creating* a declaration needs
/// SS — which is what stops two offline nodes from minting conflicting ids for
/// the same handle.
///
/// Serialization is stable: a JSON object with fixed key order (not a HashMap),
/// so the digest is reproducible across languages and releases.
#[must_use]
pub fn compute_declared_field_identity(
    declaration_id: &str,
    field_name: &str,
    field_type: &FieldValueType,
    version: u32,
) -> String {
    let type_json = serde_json::to_string(field_type).unwrap_or_else(|_| "\"Any\"".to_string());
    // Fixed key order — do not use HashMap. `algo` is inside the digest so a
    // future version cannot collide with this one even on identical inputs.
    let payload = format!(
        r#"{{"algo":{},"declaration":{},"name":{},"type":{},"version":{}}}"#,
        DECLARED_FIELD_IDENTITY_ALGO_VERSION,
        serde_json::to_string(declaration_id.trim()).unwrap_or_else(|_| "\"\"".into()),
        serde_json::to_string(field_name.trim()).unwrap_or_else(|_| "\"\"".into()),
        type_json,
        version
    );
    let digest = Sha256::digest(payload.as_bytes());
    crate::hex::hex_lower(&digest)
}
