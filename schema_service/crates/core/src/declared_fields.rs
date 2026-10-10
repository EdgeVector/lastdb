//! Declared fields — an app declares a field, gets a handle, and reuses it.
//!
//! > You declare a field. You get a handle. Use it in any schema.
//! > Two schemas using the same handle hold the same field, so their data
//! > stays in step.
//!
//! Design: brain `design-lastdb-declared-fields`. Nothing here is inferred:
//! there is no threshold, no similarity matching, and no fallback path.
//!
//! # Why this exists at all
//!
//! Schema Service already had a canonical-field registry, and it is *not* the
//! broken part. That registry answers **what does this field mean** — search,
//! sensitivity, classification — and it is deliberately network-global:
//! `builtin_canonical_fields` plus LLM classification normalize semantically
//! equivalent fields across every schema on the network.
//!
//! Coherence asks a different question: **should a write here land there?**
//! Answering it with a meaning-scoped identifier is what made
//! `created_at` / `"RFC 3339 timestamp"` / `String` byte-identical across 46
//! unrelated schemas on the live primary. This module adds the identifier for
//! the second question and leaves the first one exactly as it was.
//!
//! # What is minted, and what is derived
//!
//! Declaring mints one immutable thing: a **declaration id**. Everything else
//! is derived from it by [`schema_types::compute_declared_field_identity`],
//! which lives in the leaf crate `schema_types` precisely so a node can
//! recompute — and therefore verify — an identity offline and get exactly what
//! Schema Service got. Only *creating* a declaration needs SS, which is what
//! stops two offline nodes minting conflicting ids for the same handle.
//!
//! # Ownership is an attribute, never an input
//!
//! `owner_app_id` is not part of any identity. 949 of 1,141 live schemas carry
//! none, so an app-keyed identity would serve a minority — and ownership is
//! mutable while identity is not. Transferring a declaration leaves every field
//! identity byte-identical; a permission change must never cascade into
//! re-identifying every protein and every stamped schema.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use schema_types::{
    compute_declared_field_identity, FieldValueType, DECLARED_FIELD_IDENTITY_ALGO_VERSION,
};

/// Prefix on every minted declaration id, so the id is recognisable in a
/// schema's `field_hashes` provenance and in logs.
pub const DECLARATION_ID_PREFIX: &str = "fd_";

/// Maximum fields in one declaration. A declaration groups fields for humans;
/// it is not a schema, and an unbounded list is a denial-of-service surface.
pub const MAX_FIELDS_PER_DECLARATION: usize = 256;

/// One field inside a declaration, as stored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeclaredField {
    /// Field name as the declarer wrote it. Trimmed; case is significant.
    pub name: String,
    /// Strongly typed value type from the shared registry.
    pub field_type: FieldValueType,
    /// Human-readable description. **Not** part of the identity — correcting
    /// prose must never unbind schemas that already stamped this field.
    #[serde(default)]
    pub description: String,
    /// Bumped by the declarer when meaning or type changes incompatibly.
    /// A bump mints a new identity, hence a new protein.
    pub version: u32,
    /// The derived identity. Stored so reads never recompute, but it is a
    /// pure function of `(declaration_id, name, field_type, version)`.
    pub identity: String,
}

/// A declaration: an immutable id, a set of fields, and mutable ownership and
/// permissions hanging off it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeclaredFieldRecord {
    /// Minted, globally unique, immutable. The only input to field identity.
    pub declaration_id: String,
    /// Human-readable handle, e.g. `card/title`. Groups things for humans; the
    /// slash means nothing to the database. **A typo mints a new declaration,
    /// which fails safe.**
    pub handle: String,
    /// The app that owns this declaration. Mutable (see
    /// [`DeclaredFieldRegistry::transfer_declaration`]) and never hashed.
    pub owner_app_id: String,
    /// Apps other than the owner that may reference these identities. The
    /// owner is always permitted and is not listed here.
    #[serde(default)]
    pub readers: Vec<String>,
    /// Fields in this declaration, keyed by name for stable lookup.
    pub fields: Vec<DeclaredField>,
    /// The identity algorithm that minted `fields[].identity`.
    pub algo_version: u32,
    /// RFC 3339 timestamp of first declaration.
    pub declared_at: String,
}

impl DeclaredFieldRecord {
    /// Whether `app_id` may reference this declaration's identities.
    ///
    /// The owner always may. Everyone else needs an explicit grant — sharing a
    /// field is a permission, not a second identity scheme.
    #[must_use]
    pub fn permits(&self, app_id: &str) -> bool {
        self.owner_app_id == app_id || self.readers.iter().any(|r| r == app_id)
    }
}

/// Why a declare or reference was refused. Every variant carries a reason the
/// caller can act on — "refused, with a reason" is part of the contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeclaredFieldError {
    /// No `owner_app_id`, or it failed charset validation. **No verified
    /// identity → no owned field**: an un-namespaced submission is a proposal,
    /// which is precisely *not yours*.
    OwnerRequired(String),
    /// The handle was empty or malformed.
    InvalidHandle(String),
    /// A field entry was malformed (empty name, duplicate, too many).
    InvalidField(String),
    /// The declaration exists and is owned by a different app.
    NotOwner {
        declaration_id: String,
        owner_app_id: String,
    },
    /// The named declaration does not exist.
    UnknownDeclaration(String),
    /// The caller may not reference this declaration's identities.
    NotPermitted {
        declaration_id: String,
        app_id: String,
    },
    /// Internal failure (lock poisoned, persistence error).
    Internal(String),
}

impl DeclaredFieldError {
    /// `(status, body)` for the HTTP layer. Shapes match the app-identity
    /// handlers: a machine-readable `reason` plus a human `detail`.
    #[must_use]
    pub fn to_http(&self) -> (u16, Value) {
        match self {
            Self::OwnerRequired(detail) => {
                (400, json!({ "reason": "owner_required", "detail": detail }))
            }
            Self::InvalidHandle(detail) => {
                (400, json!({ "reason": "invalid_handle", "detail": detail }))
            }
            Self::InvalidField(detail) => {
                (400, json!({ "reason": "invalid_field", "detail": detail }))
            }
            Self::NotOwner {
                declaration_id,
                owner_app_id,
            } => (
                403,
                json!({
                    "reason": "not_owner",
                    "detail": format!(
                        "declaration {declaration_id} is owned by {owner_app_id}"
                    ),
                }),
            ),
            Self::UnknownDeclaration(id) => (
                404,
                json!({
                    "reason": "unknown_declaration",
                    "detail": format!("no declaration {id}"),
                }),
            ),
            Self::NotPermitted {
                declaration_id,
                app_id,
            } => (
                403,
                json!({
                    "reason": "not_permitted",
                    "detail": format!(
                        "app {app_id} may not reference declaration {declaration_id}; \
                         the owner must grant it"
                    ),
                }),
            ),
            Self::Internal(detail) => (500, json!({ "reason": "internal", "detail": detail })),
        }
    }
}

/// One field as submitted to `POST /v1/fields/declare`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DeclareFieldInput {
    pub name: String,
    /// Defaults to `Any` when omitted — `FieldValueType` has no `Default`
    /// impl, and inventing one crate-wide to serve this field would change
    /// behaviour elsewhere.
    #[serde(default = "default_field_type")]
    pub field_type: FieldValueType,
    #[serde(default)]
    pub description: String,
    #[serde(default = "default_field_version")]
    pub version: u32,
}

fn default_field_type() -> FieldValueType {
    FieldValueType::Any
}

const fn default_field_version() -> u32 {
    1
}

/// The declare request body. `owner_app_id` is what the DevCert is checked
/// against — it is never taken on trust.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DeclareFieldRequest {
    pub owner_app_id: String,
    pub handle: String,
    pub fields: Vec<DeclareFieldInput>,
    /// Apps granted reference permission at declare time. Additive on
    /// re-declare; never removes an existing grant.
    #[serde(default)]
    pub readers: Vec<String>,
}

/// In-memory registry of declarations.
///
/// Two indexes: the declaration id (immutable, the identity input) and
/// `(owner_app_id, handle)` (mutable, how a declarer finds their own
/// declaration again). Transfer moves the second without touching the first —
/// which is exactly why identities survive it.
#[derive(Debug, Default)]
pub struct DeclaredFieldRegistry {
    by_id: HashMap<String, DeclaredFieldRecord>,
    by_owner_handle: HashMap<(String, String), String>,
}

impl DeclaredFieldRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Rebuild both indexes from persisted rows at startup.
    pub fn load(&mut self, records: impl IntoIterator<Item = DeclaredFieldRecord>) {
        for record in records {
            self.by_owner_handle.insert(
                (record.owner_app_id.clone(), record.handle.clone()),
                record.declaration_id.clone(),
            );
            self.by_id.insert(record.declaration_id.clone(), record);
        }
    }

    #[must_use]
    pub fn get(&self, declaration_id: &str) -> Option<&DeclaredFieldRecord> {
        self.by_id.get(declaration_id)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    /// Every declaration, for snapshot export. Sorted by id for determinism.
    #[must_use]
    pub fn all(&self) -> Vec<DeclaredFieldRecord> {
        let mut out: Vec<DeclaredFieldRecord> = self.by_id.values().cloned().collect();
        out.sort_by(|a, b| a.declaration_id.cmp(&b.declaration_id));
        out
    }

    /// Declare, or re-declare, a handle.
    ///
    /// **Idempotent by construction.** A second declare of the same
    /// `(owner_app_id, handle)` reuses the existing declaration id, so every
    /// already-issued identity is returned byte-identical. New field names are
    /// added; existing ones keep their identity unless the declarer bumps
    /// `version`, which is the deliberate signal for an incompatible change.
    ///
    /// `mint_id` is injected so tests get deterministic ids; production passes
    /// a UUID-backed generator.
    pub fn declare(
        &mut self,
        request: &DeclareFieldRequest,
        mint_id: impl FnOnce() -> String,
    ) -> Result<DeclaredFieldRecord, DeclaredFieldError> {
        let owner = request.owner_app_id.trim();
        if owner.is_empty() {
            return Err(DeclaredFieldError::OwnerRequired(
                "owner_app_id is required — a field with no verified owner is a \
                 proposal, not a declaration"
                    .to_string(),
            ));
        }
        let handle = request.handle.trim();
        if handle.is_empty() {
            return Err(DeclaredFieldError::InvalidHandle(
                "handle must be non-empty".to_string(),
            ));
        }
        if request.fields.is_empty() {
            return Err(DeclaredFieldError::InvalidField(
                "declare at least one field".to_string(),
            ));
        }
        if request.fields.len() > MAX_FIELDS_PER_DECLARATION {
            return Err(DeclaredFieldError::InvalidField(format!(
                "at most {MAX_FIELDS_PER_DECLARATION} fields per declaration"
            )));
        }
        let mut seen = HashSet::new();
        for field in &request.fields {
            let name = field.name.trim();
            if name.is_empty() {
                return Err(DeclaredFieldError::InvalidField(
                    "field name must be non-empty".to_string(),
                ));
            }
            if !seen.insert(name) {
                return Err(DeclaredFieldError::InvalidField(format!(
                    "duplicate field name `{name}` in one declaration"
                )));
            }
        }

        let key = (owner.to_string(), handle.to_string());
        let declaration_id = match self.by_owner_handle.get(&key) {
            Some(existing) => existing.clone(),
            None => mint_id(),
        };

        let mut record =
            self.by_id
                .get(&declaration_id)
                .cloned()
                .unwrap_or_else(|| DeclaredFieldRecord {
                    declaration_id: declaration_id.clone(),
                    handle: handle.to_string(),
                    owner_app_id: owner.to_string(),
                    readers: Vec::new(),
                    fields: Vec::new(),
                    algo_version: DECLARED_FIELD_IDENTITY_ALGO_VERSION,
                    declared_at: String::new(),
                });

        // Re-declaring someone else's declaration is not a declare, it is a
        // takeover attempt.
        if record.owner_app_id != owner {
            return Err(DeclaredFieldError::NotOwner {
                declaration_id: record.declaration_id.clone(),
                owner_app_id: record.owner_app_id.clone(),
            });
        }

        for input in &request.fields {
            let name = input.name.trim().to_string();
            let identity = compute_declared_field_identity(
                &record.declaration_id,
                &name,
                &input.field_type,
                input.version,
            );
            let field = DeclaredField {
                name: name.clone(),
                field_type: input.field_type.clone(),
                description: input.description.trim().to_string(),
                version: input.version,
                identity,
            };
            match record.fields.iter_mut().find(|f| f.name == name) {
                // Description edits are free; identity is unaffected by them.
                Some(existing) => *existing = field,
                None => record.fields.push(field),
            }
        }
        record.fields.sort_by(|a, b| a.name.cmp(&b.name));

        // Grants are additive. Removing one is a separate, deliberate act.
        for reader in &request.readers {
            let reader = reader.trim();
            if !reader.is_empty() && reader != owner && !record.readers.iter().any(|r| r == reader)
            {
                record.readers.push(reader.to_string());
            }
        }
        record.readers.sort();

        self.by_owner_handle
            .insert(key, record.declaration_id.clone());
        self.by_id
            .insert(record.declaration_id.clone(), record.clone());
        Ok(record)
    }

    /// Move a declaration to a new owner.
    ///
    /// The declaration id does not change, so **every field identity is
    /// byte-identical afterwards**. That is the whole reason ownership is not
    /// hashed.
    pub fn transfer_declaration(
        &mut self,
        declaration_id: &str,
        current_owner: &str,
        new_owner: &str,
    ) -> Result<DeclaredFieldRecord, DeclaredFieldError> {
        let new_owner = new_owner.trim();
        if new_owner.is_empty() {
            return Err(DeclaredFieldError::OwnerRequired(
                "new owner_app_id is required".to_string(),
            ));
        }
        let record = self
            .by_id
            .get_mut(declaration_id)
            .ok_or_else(|| DeclaredFieldError::UnknownDeclaration(declaration_id.to_string()))?;
        if record.owner_app_id != current_owner {
            return Err(DeclaredFieldError::NotOwner {
                declaration_id: declaration_id.to_string(),
                owner_app_id: record.owner_app_id.clone(),
            });
        }
        let old_key = (record.owner_app_id.clone(), record.handle.clone());
        record.owner_app_id = new_owner.to_string();
        record.readers.retain(|r| r != new_owner);
        let new_key = (new_owner.to_string(), record.handle.clone());
        let updated = record.clone();
        self.by_owner_handle.remove(&old_key);
        self.by_owner_handle
            .insert(new_key, declaration_id.to_string());
        Ok(updated)
    }

    /// Refuse a reference to any identity the submitting app is not permitted
    /// to use.
    ///
    /// `identities` are the values a schema is trying to stamp into
    /// `field_hashes`. An identity belonging to no known declaration is *not*
    /// refused: the overwhelming majority of live schemas carry locally-minted
    /// v1 field hashes, and those must keep working untouched.
    pub fn authorize_references<'a>(
        &self,
        app_id: Option<&str>,
        identities: impl IntoIterator<Item = &'a str>,
    ) -> Result<(), DeclaredFieldError> {
        let mut owned: HashMap<&str, &DeclaredFieldRecord> = HashMap::new();
        for record in self.by_id.values() {
            for field in &record.fields {
                owned.insert(field.identity.as_str(), record);
            }
        }
        for identity in identities {
            let Some(record) = owned.get(identity) else {
                continue; // not a declared identity — legacy/local, untouched
            };
            let permitted = app_id.is_some_and(|a| record.permits(a));
            if !permitted {
                return Err(DeclaredFieldError::NotPermitted {
                    declaration_id: record.declaration_id.clone(),
                    app_id: app_id.unwrap_or("<none>").to_string(),
                });
            }
        }
        Ok(())
    }
}

/// Mint a fresh declaration id. Prefixed so it is recognisable on sight.
#[must_use]
pub fn mint_declaration_id() -> String {
    format!("{DECLARATION_ID_PREFIX}{}", uuid::Uuid::new_v4().simple())
}

// ===========================================================================
// Service wiring
// ===========================================================================

use crate::state::{SchemaServiceState, SchemaStorage};

impl SchemaServiceState {
    /// Declare (or re-declare) a field handle and persist the result.
    ///
    /// The caller must already have passed
    /// [`SchemaServiceState::authorize_field_declare`] — this method trusts
    /// `request.owner_app_id` and does not re-verify the cert. Keeping the two
    /// separate is deliberate: the HTTP layer owns credential extraction, and
    /// this owns the registry invariants.
    pub async fn declare_field(
        &self,
        request: &DeclareFieldRequest,
    ) -> Result<DeclaredFieldRecord, DeclaredFieldError> {
        self.record_schema_write();
        let record = {
            let mut registry = self
                .declared_fields
                .write()
                .map_err(|e| DeclaredFieldError::Internal(e.to_string()))?;
            let mut record = registry.declare(request, mint_declaration_id)?;
            if record.declared_at.is_empty() {
                record.declared_at =
                    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
                // Re-insert so the stamped timestamp is what we persist and
                // what a later read returns.
                registry.load(vec![record.clone()]);
            }
            record
        };

        match &self.storage {
            SchemaStorage::External(backend) => {
                backend
                    .save_declared_field(&record)
                    .await
                    .map_err(|e| DeclaredFieldError::Internal(e.to_string()))?;
            }
        }
        self.bump_state_version();
        tracing::info!(
            target: "schema_service::declared_fields",
            declaration_id = %record.declaration_id,
            handle = %record.handle,
            owner_app_id = %record.owner_app_id,
            fields = record.fields.len(),
            "declared field handle",
        );
        Ok(record)
    }

    /// Look up one declaration.
    pub fn get_declared_field(
        &self,
        declaration_id: &str,
    ) -> Result<DeclaredFieldRecord, DeclaredFieldError> {
        let registry = self
            .declared_fields
            .read()
            .map_err(|e| DeclaredFieldError::Internal(e.to_string()))?;
        registry
            .get(declaration_id)
            .cloned()
            .ok_or_else(|| DeclaredFieldError::UnknownDeclaration(declaration_id.to_string()))
    }

    /// Refuse a schema registration that stamps a declared identity the
    /// submitting app has no permission to reference.
    ///
    /// Called from `add_schema`. Identities belonging to no declaration pass
    /// straight through — every one of the 1,141 live schemas carries
    /// locally-minted v1 field hashes, and this feature is forward-only.
    pub fn authorize_declared_field_references(
        &self,
        owner_app_id: Option<&str>,
        field_hashes: &HashMap<String, String>,
    ) -> Result<(), DeclaredFieldError> {
        if field_hashes.is_empty() {
            return Ok(());
        }
        let registry = self
            .declared_fields
            .read()
            .map_err(|e| DeclaredFieldError::Internal(e.to_string()))?;
        if registry.is_empty() {
            return Ok(());
        }
        registry.authorize_references(owner_app_id, field_hashes.values().map(String::as_str))
    }
}
