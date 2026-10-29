use super::*;

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
