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

mod classify;
pub use classify::*;
