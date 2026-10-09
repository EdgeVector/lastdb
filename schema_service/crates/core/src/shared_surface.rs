//! Shared-surface contract for schema service.
//!
//! Schema Service is the authority for every schema identity. App-private
//! schemas are still registered; this module adds governance for contracts
//! that are intentionally shared (see `docs/shared_surface_schema_service.md`).
//!
//! This module defines:
//! - the explicit publish/attach request shape and validation;
//! - inventory classification of existing registry rows;
//! - the shared-only projection used by resolver packs;
//! - observe-mode telemetry for legacy callers that still use
//!   `POST /v1/schemas` without a full shared-surface envelope.
//!
//! Enforcement of "reject non-shared registrations" is **not** flipped on
//! here — PR 0 lands the contract and measurement. Later PRs activate
//! rejection after caller migration.

use schema_types::{Schema, SchemaSource};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

/// Visibility of a shared-surface contract.
///
/// Only `Shared` is accepted on the publish/attach path today. Private
/// schemas never reach this module.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SharedSurfaceVisibility {
    Shared,
}

impl SharedSurfaceVisibility {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Shared => "shared",
        }
    }
}

/// Why the owner is exposing a schema through a shared surface.
///
/// Similarity, install count, or same-app multi-device use never imply
/// sharing intent — one of these purposes must be asserted explicitly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SharedSurfacePurpose {
    /// Another app may read records under this contract.
    CrossAppRead,
    /// Another app may write records under this contract.
    CrossAppWrite,
    /// Cross-app linking / foreign keys against this contract.
    CrossAppLink,
    /// Published or externally discoverable data slice.
    PublishedDataSlice,
    /// Public or importable protocol/contract.
    PublicProtocol,
    /// Coordinated compatibility, migration, or lifecycle management.
    CoordinatedLifecycle,
}

impl SharedSurfacePurpose {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CrossAppRead => "cross_app_read",
            Self::CrossAppWrite => "cross_app_write",
            Self::CrossAppLink => "cross_app_link",
            Self::PublishedDataSlice => "published_data_slice",
            Self::PublicProtocol => "public_protocol",
            Self::CoordinatedLifecycle => "coordinated_lifecycle",
        }
    }
}

/// Compatibility promise for a shared contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SharedSurfaceCompatibility {
    /// New fields may be added; existing field meanings stay stable.
    BackwardCompatible,
    /// Breaking changes allowed only via a new shared identity.
    Strict,
    /// Explicitly unstable; consumers must pin identity hashes.
    Experimental,
}

impl SharedSurfaceCompatibility {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BackwardCompatible => "backward_compatible",
            Self::Strict => "strict",
            Self::Experimental => "experimental",
        }
    }
}

/// Provenance for an explicit shared publish/attach.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedSurfaceProvenance {
    /// Free-form origin label (e.g. `notes-app@1.2.0`, `operator-import`).
    /// Must be non-empty after trim; high-cardinality PII is forbidden.
    pub origin: String,
    /// Optional content-addressed declaration the node already holds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_identity_hash: Option<String>,
    /// Optional caller-supplied notes for audit (not used for matching).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
}

/// Governance metadata required on every shared-surface operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedSurfaceMetadata {
    pub visibility: SharedSurfaceVisibility,
    pub purpose: SharedSurfacePurpose,
    /// Owning app id (reverse-DNS or Mini namespace). Required, non-empty.
    pub owner_app_id: String,
    /// Human-facing shared contract name (not the local private name).
    pub contract_name: String,
    pub compatibility: SharedSurfaceCompatibility,
    pub provenance: SharedSurfaceProvenance,
}

/// Explicit shared-surface publish or attach request.
///
/// This is intentionally separate from Mini's ordinary registered
/// `POST /api/schemas/declare` body. Registration does not imply sharing, and
/// an ordinary declaration must not be deserializable into this governance
/// type by accident (different required fields).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedSurfacePublishAttachRequest {
    /// App-facing alias for a registered catalog identity
    /// (`{namespace}/{local_name}`).
    pub local_schema_id: String,
    /// Sharing intent + governance metadata.
    pub surface: SharedSurfaceMetadata,
    /// Optional proposal shape when the caller wants the service (or local
    /// resolver) to match against existing shared contracts before create.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proposal: Option<SharedSurfaceProposal>,
}

/// Proposal payload carried with a shared publish when matching is needed.
///
/// Does not create a schema by itself. Canonical creation remains a live
/// service decision after local resolution fails or is disabled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedSurfaceProposal {
    pub descriptive_name: String,
    pub fields: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub field_descriptions: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub purpose_statement: Option<String>,
}

/// Outcome of validating a shared-surface request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SharedSurfaceValidationError {
    EmptyLocalSchemaId,
    LocalSchemaIdMustBeNamespaced,
    EmptyOwnerAppId,
    OwnerAppIdMustNotContainSlash,
    EmptyContractName,
    EmptyProvenanceOrigin,
    EmptyProposalDescriptiveName,
    EmptyProposalFields,
    VisibilityNotShared,
}

impl fmt::Display for SharedSurfaceValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyLocalSchemaId => write!(f, "local_schema_id must be non-empty"),
            Self::LocalSchemaIdMustBeNamespaced => write!(
                f,
                "local_schema_id must be app-namespaced ({{namespace}}/{{local_name}})"
            ),
            Self::EmptyOwnerAppId => write!(f, "surface.owner_app_id must be non-empty"),
            Self::OwnerAppIdMustNotContainSlash => {
                write!(f, "surface.owner_app_id must not contain '/'")
            }
            Self::EmptyContractName => write!(f, "surface.contract_name must be non-empty"),
            Self::EmptyProvenanceOrigin => {
                write!(f, "surface.provenance.origin must be non-empty")
            }
            Self::EmptyProposalDescriptiveName => {
                write!(
                    f,
                    "proposal.descriptive_name must be non-empty when proposal is set"
                )
            }
            Self::EmptyProposalFields => {
                write!(f, "proposal.fields must be non-empty when proposal is set")
            }
            Self::VisibilityNotShared => {
                write!(f, "surface.visibility must be 'shared'")
            }
        }
    }
}

impl std::error::Error for SharedSurfaceValidationError {}

/// Validate an explicit shared publish/attach request.
///
/// Rejects missing ownership, purpose metadata, and non-namespaced local
/// ids. Does **not** talk to storage or the network.
pub fn validate_shared_surface_request(
    request: &SharedSurfacePublishAttachRequest,
) -> Result<(), SharedSurfaceValidationError> {
    let local_id = request.local_schema_id.trim();
    if local_id.is_empty() {
        return Err(SharedSurfaceValidationError::EmptyLocalSchemaId);
    }
    match local_id.split_once('/') {
        Some((ns, local)) if !ns.is_empty() && !local.is_empty() && !local.contains('/') => {}
        _ => return Err(SharedSurfaceValidationError::LocalSchemaIdMustBeNamespaced),
    }

    // Visibility is a closed enum; keep an explicit check so a future
    // variant cannot silently pass validation.
    if request.surface.visibility != SharedSurfaceVisibility::Shared {
        return Err(SharedSurfaceValidationError::VisibilityNotShared);
    }

    validate_surface_metadata(&request.surface)?;

    if let Some(proposal) = &request.proposal {
        if proposal.descriptive_name.trim().is_empty() {
            return Err(SharedSurfaceValidationError::EmptyProposalDescriptiveName);
        }
        if proposal.fields.is_empty() || proposal.fields.iter().any(|f| f.trim().is_empty()) {
            return Err(SharedSurfaceValidationError::EmptyProposalFields);
        }
    }

    Ok(())
}

/// Validate shared-surface governance metadata without a full publish body.
pub fn validate_surface_metadata(
    surface: &SharedSurfaceMetadata,
) -> Result<(), SharedSurfaceValidationError> {
    if surface.visibility != SharedSurfaceVisibility::Shared {
        return Err(SharedSurfaceValidationError::VisibilityNotShared);
    }
    if surface.owner_app_id.trim().is_empty() {
        return Err(SharedSurfaceValidationError::EmptyOwnerAppId);
    }
    if surface.owner_app_id.contains('/') {
        return Err(SharedSurfaceValidationError::OwnerAppIdMustNotContainSlash);
    }
    if surface.contract_name.trim().is_empty() {
        return Err(SharedSurfaceValidationError::EmptyContractName);
    }
    if surface.provenance.origin.trim().is_empty() {
        return Err(SharedSurfaceValidationError::EmptyProvenanceOrigin);
    }
    Ok(())
}

/// Inventory class for a schema already present in the registry.
///
/// Used to measure legacy private bootstrap traffic before rejection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RegistrationClass {
    /// Explicitly accepted shared-surface contract (has accepted metadata).
    Shared,
    /// App-namespaced user schema without shared-surface metadata — likely
    /// a private schema that was bootstrapped through the service before
    /// Mini local declaration was the rule.
    PrivateLegacyBootstrap,
    /// Service/system seed (Fingerprint, Edge, Schema.org starters, …).
    SystemOwned,
    /// Unowned user schema or otherwise unclassifiable row.
    Unknown,
}

impl RegistrationClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Shared => "shared",
            Self::PrivateLegacyBootstrap => "private_legacy_bootstrap",
            Self::SystemOwned => "system_owned",
            Self::Unknown => "unknown",
        }
    }

    /// Whether this class may enter the shared-only resolver projection.
    pub fn allowed_in_shared_projection(self) -> bool {
        matches!(self, Self::Shared | Self::SystemOwned)
    }
}

/// Optional persisted/attached shared-surface metadata used during inventory.
///
/// Today most registry rows have no attached surface metadata; classification
/// falls back to source + ownership heuristics until migration completes.
#[derive(Debug, Clone, Default)]
pub struct SharedSurfaceAttachment {
    pub surface: Option<SharedSurfaceMetadata>,
    /// True when the row was registered with `offer_to_shared_discovery`.
    pub offered_to_shared_discovery: bool,
}

/// Classify one registered schema for migration inventory.
pub fn classify_registration(
    schema: &Schema,
    is_system_schema: bool,
    attachment: &SharedSurfaceAttachment,
) -> RegistrationClass {
    if is_system_schema
        || matches!(
            schema.source,
            SchemaSource::SystemSeed | SchemaSource::StarterSeed
        )
    {
        return RegistrationClass::SystemOwned;
    }

    if let Some(surface) = &attachment.surface {
        if surface.visibility == SharedSurfaceVisibility::Shared
            && validate_surface_metadata(surface).is_ok()
        {
            return RegistrationClass::Shared;
        }
    }

    if attachment.offered_to_shared_discovery {
        // Explicit offer without full metadata is still shared intent, but
        // incomplete. Keep it out of the projection until metadata lands.
        return RegistrationClass::Unknown;
    }

    let has_owner = schema
        .owner_app_id
        .as_deref()
        .is_some_and(|s| !s.trim().is_empty());

    if has_owner && schema.source == SchemaSource::User {
        return RegistrationClass::PrivateLegacyBootstrap;
    }

    RegistrationClass::Unknown
}

/// Counts from an inventory sweep.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegistrationInventory {
    pub total: usize,
    pub shared: usize,
    pub private_legacy_bootstrap: usize,
    pub system_owned: usize,
    pub unknown: usize,
    pub deprecated_or_superseded: usize,
}

impl RegistrationInventory {
    pub fn record(&mut self, class: RegistrationClass, superseded: bool) {
        self.total += 1;
        if superseded {
            self.deprecated_or_superseded += 1;
        }
        match class {
            RegistrationClass::Shared => self.shared += 1,
            RegistrationClass::PrivateLegacyBootstrap => self.private_legacy_bootstrap += 1,
            RegistrationClass::SystemOwned => self.system_owned += 1,
            RegistrationClass::Unknown => self.unknown += 1,
        }
    }
}

/// Build inventory counts over a schema set.
pub fn inventory_registrations<'a, I, F>(schemas: I, mut is_system: F) -> RegistrationInventory
where
    I: IntoIterator<Item = (&'a Schema, SharedSurfaceAttachment)>,
    F: FnMut(&Schema) -> bool,
{
    let mut inv = RegistrationInventory::default();
    for (schema, attachment) in schemas {
        let class = classify_registration(schema, is_system(schema), &attachment);
        let superseded = schema
            .superseded_by
            .as_deref()
            .is_some_and(|s| !s.trim().is_empty());
        inv.record(class, superseded);
    }
    inv
}

/// Decide whether a registered schema belongs in the shared-only resolver
/// projection (snapshot / embeddings input).
///
/// Rules (PR 0):
/// - superseded / deprecated rows are excluded;
/// - only `Shared` and `SystemOwned` classes are included;
/// - private legacy bootstrap and unknown rows are excluded.
pub fn include_in_shared_only_projection(
    schema: &Schema,
    is_system_schema: bool,
    attachment: &SharedSurfaceAttachment,
) -> bool {
    if schema
        .superseded_by
        .as_deref()
        .is_some_and(|s| !s.trim().is_empty())
    {
        return false;
    }
    let class = classify_registration(schema, is_system_schema, attachment);
    class.allowed_in_shared_projection()
}

/// Filter a schema list down to the shared-only projection.
pub fn project_shared_only_schemas<'a, I, F>(schemas: I, mut is_system: F) -> Vec<&'a Schema>
where
    I: IntoIterator<Item = (&'a Schema, SharedSurfaceAttachment)>,
    F: FnMut(&Schema) -> bool,
{
    schemas
        .into_iter()
        .filter(|(schema, attachment)| {
            include_in_shared_only_projection(schema, is_system(schema), attachment)
        })
        .map(|(schema, _)| schema)
        .collect()
}

/// Observe-mode classification for legacy `POST /v1/schemas` traffic.
///
/// Used until callers migrate to the explicit shared-surface request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacySchemaCallerKind {
    /// Mini-style local claim (`offer_to_shared_discovery = false` + owner).
    LocalClaim,
    /// Explicit offer flag without full shared-surface envelope.
    SharedOfferWithoutSurface,
    /// Unowned registration (historical "new shared mutation" path).
    UnownedRegistration,
    /// Full shared-surface envelope present (new path).
    ExplicitSharedSurface,
}

impl LegacySchemaCallerKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LocalClaim => "local_claim",
            Self::SharedOfferWithoutSurface => "shared_offer_without_surface",
            Self::UnownedRegistration => "unowned_registration",
            Self::ExplicitSharedSurface => "explicit_shared_surface",
        }
    }
}

/// Classify a legacy add-schema style call for observability.
pub fn classify_legacy_schema_caller(
    owner_app_id: Option<&str>,
    offer_to_shared_discovery: bool,
    has_shared_surface_envelope: bool,
) -> LegacySchemaCallerKind {
    if has_shared_surface_envelope {
        return LegacySchemaCallerKind::ExplicitSharedSurface;
    }
    let has_owner = owner_app_id.is_some_and(|s| !s.trim().is_empty());
    if has_owner && !offer_to_shared_discovery {
        return LegacySchemaCallerKind::LocalClaim;
    }
    if offer_to_shared_discovery {
        return LegacySchemaCallerKind::SharedOfferWithoutSurface;
    }
    LegacySchemaCallerKind::UnownedRegistration
}

/// Emit observe-mode telemetry for a legacy (or transitional) schema caller.
///
/// Does not enforce rejection. Safe to call on every `POST /v1/schemas`.
pub fn observe_legacy_schema_caller(
    owner_app_id: Option<&str>,
    offer_to_shared_discovery: bool,
    has_shared_surface_envelope: bool,
) -> LegacySchemaCallerKind {
    let kind = classify_legacy_schema_caller(
        owner_app_id,
        offer_to_shared_discovery,
        has_shared_surface_envelope,
    );
    tracing::info!(
        target: "schema_service::shared_surface",
        metric = "schema_shared_surface_legacy_caller_total",
        mode = "observe",
        caller_kind = kind.as_str(),
        offer_to_shared_discovery,
        has_shared_surface_envelope,
        owner_present = owner_app_id.is_some_and(|s| !s.trim().is_empty()),
        "schema registration caller classified for shared-surface migration"
    );
    kind
}

/// True when a Mini private declare body cannot be mistaken for a shared
/// publish request.
///
/// Private declare uses `{ namespace, schema }` (or apps declare
/// `{ app_id, schema }`). Shared publish requires `local_schema_id` +
/// `surface` with purpose/compatibility/provenance. Deserializing a private
/// declare body as [`SharedSurfacePublishAttachRequest`] must fail.
pub fn private_declare_body_is_not_shared_surface(private_declare_json: &str) -> bool {
    serde_json::from_str::<SharedSurfacePublishAttachRequest>(private_declare_json).is_err()
}
