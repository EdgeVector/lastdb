//! One-shot process environment load for [`SchemaConfig`].
//!
//! Parsing matches the readers that used to live in `schema_service_core`.
//! Several flags do **not** use the `env_flag` vocabulary; those differences
//! are preserved below. Secret values (`SCHEMA_MUTATION_GATE_HMAC_SECRET`,
//! `REGISTRY_INDEX_SIGNING_KEY_B64`) are never logged.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use app_identity_crypto::{root_key_id, Env};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use schema_service_core::registry_index::REGISTRY_INDEX_SIGNING_KEY_ENV;
use schema_service_core::schema_config::DEFAULT_NATIVE_RESOLVE_BUDGET_MS;
use schema_service_core::{
    AppIdentityConfig, RegistryIndexSigningKey, SchemaConfig, SchemaMutationGateConfig,
};

/// Implemented for [`SchemaConfig`] so shells can call [`SchemaConfig::from_env`].
pub trait SchemaConfigEnv {
    fn from_env() -> Self;
}

impl SchemaConfigEnv for SchemaConfig {
    fn from_env() -> Self {
        schema_config_from_env()
    }
}

fn schema_config_from_env() -> SchemaConfig {
    let defaults = SchemaConfig::default();
    let (compositional_decomposition, compositional_apply) = compositional_from_env();
    SchemaConfig {
        app_identity: app_identity_from_env(),
        mutation_gate: mutation_gate_from_env(),
        dual_signal_canonicalization: default_on_unless_exact_falsy(
            "SCHEMA_DUAL_SIGNAL_CANONICALIZATION",
        ),
        reuse_before_new: default_on_unless_exact_falsy("SCHEMA_REUSE_BEFORE_NEW"),
        reuse_purpose_threshold: var_or(
            "SCHEMA_REUSE_PURPOSE_THRESHOLD",
            defaults.reuse_purpose_threshold,
        ),
        reuse_struct_floor: var_or("SCHEMA_REUSE_STRUCT_FLOOR", defaults.reuse_struct_floor),
        reuse_field_fidelity_floor: var_or(
            "SCHEMA_REUSE_FIELD_FIDELITY_FLOOR",
            defaults.reuse_field_fidelity_floor,
        ),
        shadow_mode: var_truthy("SCHEMA_SHADOW_MODE"),
        compositional_decomposition,
        compositional_apply,
        native_component_cover_resolve: native_cover_from_env(),
        native_resolve_budget: native_budget_from_env(defaults.native_resolve_budget),
        registry_index_signing_key: registry_signing_key_from_env(),
    }
}

fn app_identity_from_env() -> AppIdentityConfig {
    let environment = std::env::var("ENVIRONMENT").ok();
    AppIdentityConfig {
        deployment_env: deployment_env(environment.as_deref()),
        trusted_roots: std::env::var("APP_IDENTITY_ROOT_PUBKEYS")
            .ok()
            .map(|raw| parse_trusted_roots(&raw))
            .unwrap_or_default(),
        revoked_dev_pubkeys: std::env::var("APP_IDENTITY_REVOKED_DEV_PUBKEYS")
            .ok()
            .map(|raw| parse_revoked_dev_pubkeys(&raw))
            .unwrap_or_default(),
        verifier_pubkey: std::env::var("APP_REGISTRY_VERIFIER_PUBKEY")
            .ok()
            .and_then(|raw| parse_verifier_pubkey(&raw)),
        // DEV only. `AppIdentityConfig::registration_is_open` ignores it in PROD.
        open_registration: var_truthy("SCHEMA_OPEN_REGISTRATION"),
    }
}

fn deployment_env(raw: Option<&str>) -> Env {
    match raw {
        Some("prod" | "production") => Env::Prod,
        _ => Env::Dev,
    }
}

fn parse_trusted_roots(raw: &str) -> HashMap<String, Vec<u8>> {
    let mut roots = HashMap::new();
    for entry in raw.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let Ok(der) = BASE64.decode(entry.as_bytes()) else {
            tracing::warn!(
                target: "schema_service::app_identity",
                "Skipping malformed APP_IDENTITY_ROOT_PUBKEYS entry (not base64)"
            );
            continue;
        };
        roots.insert(root_key_id(&der), der);
    }
    roots
}

fn parse_revoked_dev_pubkeys(raw: &str) -> HashSet<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(str::to_string)
        .collect()
}

fn parse_verifier_pubkey(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

fn mutation_gate_from_env() -> SchemaMutationGateConfig {
    let defaults = SchemaMutationGateConfig::default();
    SchemaMutationGateConfig {
        enforce_shared_mutations: var_truthy("SCHEMA_MUTATION_GATE_ENFORCE"),
        challenge_ttl: Duration::from_secs(var_or(
            "SCHEMA_MUTATION_GATE_CHALLENGE_TTL_SECS",
            defaults.challenge_ttl.as_secs(),
        )),
        base_difficulty_bits: var_or(
            "SCHEMA_MUTATION_GATE_BASE_DIFFICULTY_BITS",
            defaults.base_difficulty_bits,
        ),
        quota_window: Duration::from_secs(var_or(
            "SCHEMA_MUTATION_GATE_QUOTA_WINDOW_SECS",
            defaults.quota_window.as_secs(),
        )),
        node_quota: var_or("SCHEMA_MUTATION_GATE_NODE_QUOTA", defaults.node_quota),
        ip_quota: var_or("SCHEMA_MUTATION_GATE_IP_QUOTA", defaults.ip_quota),
        app_quota: var_or("SCHEMA_MUTATION_GATE_APP_QUOTA", defaults.app_quota),
        dev_quota: var_or("SCHEMA_MUTATION_GATE_DEV_QUOTA", defaults.dev_quota),
        novel_hour_window: Duration::from_secs(var_or(
            "SCHEMA_MUTATION_GATE_NOVEL_HOUR_WINDOW_SECS",
            defaults.novel_hour_window.as_secs(),
        )),
        novel_day_window: Duration::from_secs(var_or(
            "SCHEMA_MUTATION_GATE_NOVEL_DAY_WINDOW_SECS",
            defaults.novel_day_window.as_secs(),
        )),
        node_novel_hour_quota: var_or(
            "SCHEMA_MUTATION_GATE_NODE_NOVEL_HOUR_QUOTA",
            defaults.node_novel_hour_quota,
        ),
        node_novel_day_quota: var_or(
            "SCHEMA_MUTATION_GATE_NODE_NOVEL_DAY_QUOTA",
            defaults.node_novel_day_quota,
        ),
        dev_novel_hour_quota: var_or(
            "SCHEMA_MUTATION_GATE_DEV_NOVEL_HOUR_QUOTA",
            defaults.dev_novel_hour_quota,
        ),
        dev_novel_day_quota: var_or(
            "SCHEMA_MUTATION_GATE_DEV_NOVEL_DAY_QUOTA",
            defaults.dev_novel_day_quota,
        ),
        max_adaptive_difficulty_bits: var_or(
            "SCHEMA_MUTATION_GATE_MAX_ADAPTIVE_DIFFICULTY_BITS",
            defaults.max_adaptive_difficulty_bits,
        ),
        max_catalog_difficulty_bits: var_or(
            "SCHEMA_MUTATION_GATE_MAX_CATALOG_DIFFICULTY_BITS",
            defaults.max_catalog_difficulty_bits,
        ),
        hmac_secret: hmac_secret_from_env(defaults.hmac_secret),
    }
}

/// Unset, unreadable, or whitespace-only keeps the dev default. Any other
/// value is taken as raw bytes, including surrounding spaces.
fn hmac_secret_from_env(default: Vec<u8>) -> Vec<u8> {
    match std::env::var("SCHEMA_MUTATION_GATE_HMAC_SECRET") {
        Ok(value) if !value.trim().is_empty() => value.into_bytes(),
        _ => default,
    }
}

/// `env_flag::var_or`: trim, parse, else `default`.
fn var_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(default)
}

/// `env_flag::var_truthy`: trim, ASCII lowercase, `1`/`true`/`yes`/`on`.
fn var_truthy(name: &str) -> bool {
    std::env::var(name).ok().is_some_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

/// Default-on. Exact byte match, no trim: `0`, `false`, `FALSE`, `no`.
fn default_on_unless_exact_falsy(name: &str) -> bool {
    let Ok(value) = std::env::var(name) else {
        return true;
    };
    !matches!(value.as_str(), "0" | "false" | "FALSE" | "no")
}

/// Default off. Exact byte match, no trim.
fn compositional_from_env() -> (bool, bool) {
    let Ok(value) = std::env::var("SCHEMA_COMPOSITIONAL_DECOMPOSITION") else {
        return (false, false);
    };
    let enabled = matches!(
        value.as_str(),
        "1" | "true" | "TRUE" | "yes" | "advisory" | "apply"
    );
    (enabled, value == "apply")
}

/// Default on. Trim and ASCII lowercase. Falsy: `0`, `false`, `off`, `no`.
fn native_cover_from_env() -> bool {
    let Ok(value) = std::env::var("SCHEMA_NATIVE_COMPONENT_COVER_RESOLVE") else {
        return true;
    };
    let trimmed = value.trim().to_ascii_lowercase();
    !matches!(trimmed.as_str(), "0" | "false" | "off" | "no")
}

fn native_budget_from_env(default: Duration) -> Duration {
    let default_ms = u64::try_from(default.as_millis()).unwrap_or(DEFAULT_NATIVE_RESOLVE_BUDGET_MS);
    Duration::from_millis(var_or("SCHEMA_NATIVE_RESOLVE_BUDGET_MS", default_ms))
}

fn registry_signing_key_from_env() -> RegistryIndexSigningKey {
    match std::env::var(REGISTRY_INDEX_SIGNING_KEY_ENV) {
        Ok(raw) if !raw.trim().is_empty() => parse_registry_signing_seed(&raw),
        _ => RegistryIndexSigningKey::Absent,
    }
}

fn parse_registry_signing_seed(raw: &str) -> RegistryIndexSigningKey {
    let Ok(bytes) = BASE64.decode(raw.trim().as_bytes()) else {
        return RegistryIndexSigningKey::Invalid(format!(
            "{REGISTRY_INDEX_SIGNING_KEY_ENV} is not base64"
        ));
    };
    let Ok(seed) = <[u8; 32]>::try_from(bytes.as_slice()) else {
        return RegistryIndexSigningKey::Invalid(format!(
            "{REGISTRY_INDEX_SIGNING_KEY_ENV} must decode to a 32-byte Ed25519 seed"
        ));
    };
    RegistryIndexSigningKey::Seed(seed)
}
