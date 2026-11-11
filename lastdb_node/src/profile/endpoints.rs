//! Centralized endpoint registry generated from `lastdb_node/environments.json`.

use std::sync::OnceLock;

mod gen {
    include!(concat!(env!("OUT_DIR"), "/environments_generated.rs"));
}

/// Service environment — `dev` or `prod`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Environment {
    /// Development environment.
    Dev,
    /// Production environment.
    Prod,
}

static EXPLICIT_ENV: OnceLock<Option<Environment>> = OnceLock::new();
static HOME_ENV: OnceLock<Environment> = OnceLock::new();

/// Anonymous S3 base for the registry's signed interface lists.
#[must_use]
pub const fn registry_index_url_for(env: Environment) -> &'static str {
    match env {
        Environment::Dev => gen::DEV_REGISTRY_INDEX,
        Environment::Prod => gen::PROD_REGISTRY_INDEX,
    }
}

/// Explicit first-party owner/CI signer trust, separate from the registry list key.
#[must_use]
pub const fn registry_ci_publisher_keys_for(env: Environment) -> (&'static str, &'static str) {
    match env {
        Environment::Dev => (
            gen::DEV_REGISTRY_CI_OWNER_KEY,
            gen::DEV_REGISTRY_CI_PUBLISHER_KEY,
        ),
        Environment::Prod => (
            gen::PROD_REGISTRY_CI_OWNER_KEY,
            gen::PROD_REGISTRY_CI_PUBLISHER_KEY,
        ),
    }
}

/// Pin the environment derived from the node home's cloud connection.
pub fn set_home_environment(env: Environment) -> bool {
    *HOME_ENV.get_or_init(|| env) == env
}

/// The environment pinned by [`set_home_environment`], if any.
pub fn home_environment() -> Option<Environment> {
    HOME_ENV.get().copied()
}

/// Map a persisted Exemem API URL back to its environment.
pub fn environment_for_exemem_api_url(api_url: &str) -> Option<Environment> {
    let url = api_url.trim().trim_end_matches('/');
    [Environment::Dev, Environment::Prod]
        .into_iter()
        .find(|env| exemem_api_url_for(*env).trim_end_matches('/') == url)
}

/// Raw value did not parse as a known environment name.
#[derive(Debug)]
pub struct UnknownEnvironment(pub String);

impl std::fmt::Display for UnknownEnvironment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "unknown environment value: {:?} (expected \"dev\" or \"prod\")",
            self.0
        )
    }
}

impl std::error::Error for UnknownEnvironment {}

impl Environment {
    /// Resolve from `ENVIRONMENT`, then `EXEMEM_ENV`, then pinned home env, else build default.
    pub fn from_env() -> Self {
        Self::explicit_from_env()
            .or_else(home_environment)
            .unwrap_or_else(Self::build_default)
    }

    /// Resolve `ENVIRONMENT` / `EXEMEM_ENV` once per process (`None` when neither is set).
    pub fn explicit_from_env() -> Option<Self> {
        *EXPLICIT_ENV.get_or_init(Self::resolve_explicit_once)
    }

    fn build_default() -> Self {
        if cfg!(debug_assertions) {
            Self::Dev
        } else {
            Self::Prod
        }
    }

    /// Parse a raw environment name (`"dev"`/`"development"` or `"prod"`/`"production"`).
    pub fn try_from_str(raw: &str) -> Result<Self, UnknownEnvironment> {
        match raw {
            "prod" | "production" => Ok(Self::Prod),
            "dev" | "development" => Ok(Self::Dev),
            other => Err(UnknownEnvironment(other.to_string())),
        }
    }

    /// Lowercase literal (`"dev"` or `"prod"`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Dev => "dev",
            Self::Prod => "prod",
        }
    }

    fn resolve_explicit_once() -> Option<Self> {
        let raw = std::env::var("ENVIRONMENT")
            .ok()
            .filter(|v| !v.is_empty())
            .or_else(|| std::env::var("EXEMEM_ENV").ok());
        match raw.as_deref() {
            Some("prod" | "production") => Some(Self::Prod),
            Some("dev" | "development") => Some(Self::Dev),
            Some(other) => {
                tracing::error!(
                    "ENVIRONMENT/EXEMEM_ENV has unknown value '{other}', defaulting to dev"
                );
                Some(Self::Dev)
            }
            None => {
                tracing::info!(
                    "ENVIRONMENT/EXEMEM_ENV not set; using pinned home env or build default ({})",
                    Self::build_default()
                );
                None
            }
        }
    }
}

impl std::fmt::Display for Environment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Schema service URL: `FOLD_SCHEMA_SERVICE_URL` override, else [`Environment::from_env`].
pub fn schema_service_url() -> String {
    std::env::var("FOLD_SCHEMA_SERVICE_URL")
        .ok()
        .filter(|u| !u.trim().is_empty())
        .unwrap_or_else(|| schema_service_url_for(Environment::from_env()).to_string())
}

/// Schema service URL for a specific environment.
pub fn schema_service_url_for(env: Environment) -> &'static str {
    match env {
        Environment::Dev => gen::DEV_SCHEMA_SERVICE,
        Environment::Prod => gen::PROD_SCHEMA_SERVICE,
    }
}

/// Exemem API URL for a specific environment.
pub fn exemem_api_url_for(env: Environment) -> &'static str {
    match env {
        Environment::Dev => gen::DEV_EXEMEM_API,
        Environment::Prod => gen::PROD_EXEMEM_API,
    }
}
