//! Startup configuration for the schema service.
//!
//! The process shell reads the environment once and passes a [`SchemaConfig`]
//! into the state constructors. This crate does not read process environment
//! variables. Do not log [`SchemaMutationGateConfig::hmac_secret`] or
//! [`RegistryIndexSigningKey::Seed`].

use std::time::Duration;

use app_identity_crypto::{Env, SigningKey};
use schema_types::{FoldDbError, FoldDbResult};

use crate::app_identity::AppIdentityConfig;
use crate::registry_index::REGISTRY_INDEX_SIGNING_KEY_ENV;
use crate::schema_mutation_gate::SchemaMutationGateConfig;
use crate::state::SchemaServiceState;
use crate::state_matching::{
    REUSE_FIELD_FIDELITY_FLOOR, REUSE_PURPOSE_THRESHOLD, REUSE_STRUCT_FLOOR,
};

/// Wall-clock budget for registry embedding during native resolve when
/// `SCHEMA_NATIVE_RESOLVE_BUDGET_MS` is unset.
pub const DEFAULT_NATIVE_RESOLVE_BUDGET_MS: u64 = 8_000;

/// Ed25519 seed for `REGISTRY_INDEX_SIGNING_KEY_B64`, captured at startup.
///
/// `Seed` bytes are a secret. Debug and log output must not print them.
#[derive(Clone)]
pub enum RegistryIndexSigningKey {
    /// 32-byte seed decoded from the env var.
    Seed([u8; 32]),
    /// Unset, unreadable, or whitespace-only. Dev uses a fixed seed; prod errors.
    Absent,
    /// Set, but not a 32-byte base64 seed. `sign` returns `message` unchanged.
    Invalid(String),
}

/// Process configuration held on [`SchemaServiceState`].
///
/// Shells build this with `SchemaConfig::from_env()` (see
/// `schema_service_server_shared`). Defaults match an empty environment.
#[derive(Clone)]
pub struct SchemaConfig {
    pub app_identity: AppIdentityConfig,
    pub mutation_gate: SchemaMutationGateConfig,
    /// `SCHEMA_DUAL_SIGNAL_CANONICALIZATION`. Default on.
    pub dual_signal_canonicalization: bool,
    /// `SCHEMA_REUSE_BEFORE_NEW`. Default on.
    pub reuse_before_new: bool,
    /// `SCHEMA_REUSE_PURPOSE_THRESHOLD`.
    pub reuse_purpose_threshold: f32,
    /// `SCHEMA_REUSE_STRUCT_FLOOR`.
    pub reuse_struct_floor: f32,
    /// `SCHEMA_REUSE_FIELD_FIDELITY_FLOOR`.
    pub reuse_field_fidelity_floor: f32,
    /// `SCHEMA_SHADOW_MODE`. Default off.
    pub shadow_mode: bool,
    /// `SCHEMA_COMPOSITIONAL_DECOMPOSITION` truthy values, including `apply`.
    pub compositional_decomposition: bool,
    /// `SCHEMA_COMPOSITIONAL_DECOMPOSITION=apply`.
    pub compositional_apply: bool,
    /// `SCHEMA_NATIVE_COMPONENT_COVER_RESOLVE`. Default on.
    pub native_component_cover_resolve: bool,
    /// `SCHEMA_NATIVE_RESOLVE_BUDGET_MS`.
    pub native_resolve_budget: Duration,
    pub registry_index_signing_key: RegistryIndexSigningKey,
}

impl Default for SchemaConfig {
    fn default() -> Self {
        Self {
            app_identity: AppIdentityConfig::default(),
            mutation_gate: SchemaMutationGateConfig::default(),
            dual_signal_canonicalization: true,
            reuse_before_new: true,
            reuse_purpose_threshold: REUSE_PURPOSE_THRESHOLD,
            reuse_struct_floor: REUSE_STRUCT_FLOOR,
            reuse_field_fidelity_floor: REUSE_FIELD_FIDELITY_FLOOR,
            shadow_mode: false,
            compositional_decomposition: false,
            compositional_apply: false,
            native_component_cover_resolve: true,
            native_resolve_budget: Duration::from_millis(DEFAULT_NATIVE_RESOLVE_BUDGET_MS),
            registry_index_signing_key: RegistryIndexSigningKey::Absent,
        }
    }
}

impl SchemaServiceState {
    pub(crate) fn dual_signal_canonicalization_enabled(&self) -> bool {
        self.schema_config.dual_signal_canonicalization
    }

    pub(crate) fn reuse_before_new_enabled(&self) -> bool {
        self.schema_config.reuse_before_new
    }

    pub(crate) fn reuse_purpose_threshold(&self) -> f32 {
        self.schema_config.reuse_purpose_threshold
    }

    pub(crate) fn reuse_struct_floor(&self) -> f32 {
        self.schema_config.reuse_struct_floor
    }

    pub(crate) fn reuse_field_fidelity_floor(&self) -> f32 {
        self.schema_config.reuse_field_fidelity_floor
    }

    pub(crate) fn shadow_mode_enabled(&self) -> bool {
        self.schema_config.shadow_mode
    }

    pub(crate) fn compositional_decomposition_enabled(&self) -> bool {
        self.schema_config.compositional_decomposition
    }

    pub(crate) fn compositional_apply_enabled(&self) -> bool {
        self.schema_config.compositional_apply
    }

    pub(crate) fn native_component_cover_resolve_enabled(&self) -> bool {
        self.schema_config.native_component_cover_resolve
    }

    pub(crate) fn native_resolve_budget(&self) -> Duration {
        self.schema_config.native_resolve_budget
    }

    pub(crate) fn registry_index_signing_key(&self) -> FoldDbResult<SigningKey> {
        resolve_registry_index_signing_key(
            &self.schema_config.registry_index_signing_key,
            self.schema_config.app_identity.deployment_env,
        )
    }
}

fn resolve_registry_index_signing_key(
    key: &RegistryIndexSigningKey,
    deployment_env: Env,
) -> FoldDbResult<SigningKey> {
    match key {
        RegistryIndexSigningKey::Seed(seed) => Ok(SigningKey::from_bytes(seed)),
        RegistryIndexSigningKey::Invalid(message) => Err(FoldDbError::Config(message.clone())),
        RegistryIndexSigningKey::Absent => {
            if deployment_env == Env::Prod {
                return Err(FoldDbError::Config(format!(
                    "{REGISTRY_INDEX_SIGNING_KEY_ENV} must be set in prod"
                )));
            }
            Ok(SigningKey::from_bytes(&[0x42; 32]))
        }
    }
}
