//! `$FOLDDB_HOME/config.toml` — the active env profile + cached API-key
//! handle written by `folddb login`. The brief calls this "the env
//! profile"; after `login`, no `folddb` verb takes `--dev-api-key` or
//! `--schema-service-url` because the profile carries them.
//!
//! This machinery used to live in `fold_db_node::app_identity_client::profile`;
//! it moved into this leaf crate so the `folddb dev` dev node can resolve the
//! same active-env profile **without** depending on `fold_db_node`. The
//! store-aware resolver (`resolve_api_key_with_store`) stayed behind in
//! `fold_db_node` because it depends on the heavy `DeveloperKeyStore` seam;
//! this crate exposes only the pure flag → env → profile resolution, which is
//! all the dev node's publish verbs need (the dev node has its own,
//! separate, out-of-scope Ed25519 keypair store).
//!
//! The file shape is deliberately small: one `[active]` section naming
//! the current env, plus one `[env.dev]` / `[env.prod]` table per env
//! that carries the URL overrides and the API-key handle. Defaults come
//! from [`crate::endpoints`] (the `environments.json` registry), so an
//! absent or partial table is fine — the profile only stores divergence
//! from the registry.
//!
//! The API-key value itself is **not** stored in this file. Here we keep
//! only its env-var name (`EXEMEM_DEV_API_KEY` by default) plus the
//! `api_key_tail` (last 6 chars, for human display). The full `em_` key
//! `folddb login` obtained is persisted at rest in the developer key
//! store's secure file (the `folddb` CLI layers that store fallback in via
//! `fold_db_node`'s `resolve_api_key_with_store`) so an invited developer
//! (who has no env var) can still publish.
//!
//! Reading the profile never errors on missing — a brand-new machine
//! looks like `Profile::default()` (active = dev, both env tables empty).

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::endpoints::{schema_service_url_for, Environment};
use crate::paths::folddb_home;

const PROFILE_FILE: &str = "config.toml";

/// One env's settings — schema_service / exemem URL overrides + the
/// API-key handle. All fields are optional; absence means "use the
/// `environments.json` default".
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct EnvProfile {
    /// Override the schema_service URL for this env (rare — usually
    /// only set when pointing at a local schema_service for tests).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_service_url: Option<String>,
    /// Override the exemem base URL for this env.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exemem_url: Option<String>,
    /// Name of the env var holding the dev API key. Defaults to
    /// `EXEMEM_DEV_API_KEY`. Profile-stored so a developer can run
    /// `folddb login --api-key-env MY_VAR` and have it stick.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key_env: Option<String>,
    /// Last 6 chars of the API-key body (for human display only — never
    /// the full secret).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key_tail: Option<String>,
    /// Optional human handle the developer chose at `login` (mirrored
    /// from `DeveloperRecord.handle` for `app status` readouts when the
    /// keypair lookup is deferred).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handle: Option<String>,
}

/// `$FOLDDB_HOME/config.toml` shape.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Profile {
    /// `dev` or `prod`. Defaults to `dev` when absent.
    #[serde(default = "default_active_env")]
    pub active: String,
    #[serde(default)]
    pub env: ProfileEnvs,
}

impl Default for Profile {
    fn default() -> Self {
        Self {
            active: default_active_env(),
            env: ProfileEnvs::default(),
        }
    }
}

fn default_active_env() -> String {
    "dev".to_string()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProfileEnvs {
    #[serde(default)]
    pub dev: EnvProfile,
    #[serde(default)]
    pub prod: EnvProfile,
}

#[derive(thiserror::Error, Debug)]
pub enum ProfileError {
    #[error("could not resolve $FOLDDB_HOME: {0}")]
    NoHome(String),
    #[error("I/O error on {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{path} is malformed TOML: {source}")]
    MalformedToml {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("unknown env {0:?}; expected `dev` or `prod`")]
    UnknownEnv(String),
}

impl Profile {
    /// Read `$FOLDDB_HOME/config.toml` — or [`Profile::default`] if absent.
    /// A malformed file IS an error (don't silently nuke a developer's
    /// settings).
    pub fn load() -> Result<Self, ProfileError> {
        let path = Self::path()?;
        match std::fs::read_to_string(&path) {
            Ok(text) => {
                let profile: Self = toml::from_str(&text)
                    .map_err(|source| ProfileError::MalformedToml { path, source })?;
                Ok(profile)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(source) => Err(ProfileError::Io { path, source }),
        }
    }

    /// Write the profile back to `$FOLDDB_HOME/config.toml` (creates parent
    /// dirs).
    pub fn save(&self) -> Result<(), ProfileError> {
        let path = Self::path()?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| ProfileError::Io {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        let text = toml::to_string_pretty(self).expect("Profile is a finite TOML tree");
        std::fs::write(&path, text.as_bytes()).map_err(|source| ProfileError::Io { path, source })
    }

    /// Resolve the path the profile lives at.
    pub fn path() -> Result<PathBuf, ProfileError> {
        let home = folddb_home().map_err(ProfileError::NoHome)?;
        Ok(home.join(PROFILE_FILE))
    }

    /// Active [`Environment`] (`dev` or `prod`), erroring loudly on an
    /// unknown value.
    pub fn active_env(&self) -> Result<Environment, ProfileError> {
        env_from_label(&self.active)
    }

    /// Active env's profile slice (read-only).
    pub fn active_env_profile(&self) -> Result<&EnvProfile, ProfileError> {
        Ok(match self.active_env()? {
            Environment::Dev => &self.env.dev,
            Environment::Prod => &self.env.prod,
        })
    }

    /// Set the active env (so a later `save()` records it).
    pub fn set_active(&mut self, env: Environment) {
        self.active = env_label(env).to_string();
    }
}

/// Map an env label (`"dev"` / `"prod"`) onto the typed [`Environment`].
pub fn env_from_label(label: &str) -> Result<Environment, ProfileError> {
    match label {
        "dev" | "development" => Ok(Environment::Dev),
        "prod" | "production" => Ok(Environment::Prod),
        other => Err(ProfileError::UnknownEnv(other.to_string())),
    }
}

/// Inverse of [`env_from_label`] — the canonical label written in the
/// profile.
pub fn env_label(env: Environment) -> &'static str {
    match env {
        Environment::Dev => "dev",
        Environment::Prod => "prod",
    }
}

/// Effective schema_service URL for a given env: profile override wins,
/// else the `environments.json` baked-in default.
pub fn schema_service_url(env: Environment, profile: &EnvProfile) -> String {
    profile
        .schema_service_url
        .clone()
        .unwrap_or_else(|| schema_service_url_for(env).to_string())
}

/// Effective exemem base URL for a given env. The runtime helper
/// [`crate::endpoints::exemem_api_url`] is `EXEMEM_ENV`-driven; we ignore
/// that and pick the URL keyed to the env profile so `folddb login --env
/// prod` doesn't surprise a developer whose shell has `EXEMEM_ENV=dev`.
pub fn exemem_url(env: Environment, profile: &EnvProfile) -> String {
    profile
        .exemem_url
        .clone()
        .unwrap_or_else(|| crate::endpoints::exemem_api_url_for(env).to_string())
}

/// Compact one-letter / 6-char tail of an API key — `em_…ab12cd` for
/// the human-readable last-used display in `app status`.
pub fn api_key_tail(api_key: &str) -> String {
    let len = api_key.len();
    if len <= 6 {
        return api_key.to_string();
    }
    api_key[len - 6..].to_string()
}

/// Resolve the API key for the active env: explicit `--api-key` wins,
/// else `EXEMEM_DEV_API_KEY` (or the profile's `api_key_env` override).
///
/// NOTE: this does **not** consult the secure store. The store-backed
/// fallback (the key a `folddb login` persisted) lives in
/// `fold_db_node`'s `resolve_api_key_with_store` — the `folddb` CLI's
/// publish/promote paths use that one so an invited developer, who has no
/// env var and no `--api-key`, can still publish with the key login saved
/// for them. The `folddb dev` dev node uses the bare resolver here plus its
/// own separate Ed25519 keypair store.
pub fn resolve_api_key(profile: &EnvProfile, explicit: Option<&str>) -> Option<String> {
    if let Some(key) = explicit {
        let trimmed = key.trim();
        if !trimmed.is_empty() {
            return Some(key.to_string());
        }
    }
    let var_name = profile
        .api_key_env
        .as_deref()
        .unwrap_or("EXEMEM_DEV_API_KEY");
    std::env::var(var_name)
        .ok()
        .filter(|s| !s.trim().is_empty())
}

/// Re-export — handy for callers that want to format the URL without
/// touching `endpoints` directly.
pub use crate::endpoints::Environment as ProfileEnvironment;
