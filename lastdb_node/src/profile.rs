//! Cross-environment endpoint registry and `LASTDB_HOME` / tilde path helpers.
#![warn(missing_docs)]

use std::path::{Path, PathBuf};

/// Centralized endpoint registry generated from `lastdb_node/environments.json`.
pub mod endpoints {
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
            EXPLICIT_ENV
                .get_or_init(Self::resolve_explicit_once)
                .or_else(|| HOME_ENV.get().copied())
                .unwrap_or_else(Self::build_default)
        }

        const fn build_default() -> Self {
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
        pub const fn as_str(self) -> &'static str {
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
            if let Some(raw) = raw.as_deref() {
                Some(Self::try_from_str(raw).unwrap_or_else(|_| {
                    tracing::error!(
                        "ENVIRONMENT/EXEMEM_ENV has unknown value '{raw}', defaulting to dev"
                    );
                    Self::Dev
                }))
            } else {
                tracing::info!(
                    "ENVIRONMENT/EXEMEM_ENV not set; using pinned home env or build default ({})",
                    Self::build_default()
                );
                None
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
    pub const fn schema_service_url_for(env: Environment) -> &'static str {
        match env {
            Environment::Dev => gen::DEV_SCHEMA_SERVICE,
            Environment::Prod => gen::PROD_SCHEMA_SERVICE,
        }
    }

    /// Exemem API URL for a specific environment.
    pub const fn exemem_api_url_for(env: Environment) -> &'static str {
        match env {
            Environment::Dev => gen::DEV_EXEMEM_API,
            Environment::Prod => gen::PROD_EXEMEM_API,
        }
    }
}

/// Node-home path resolution and tilde expansion.
pub mod paths {
    pub use crate::profile::{expand_tilde_path, folddb_home, LASTDB_DIR, LASTDB_HOME_ENV};
}

/// Directory name of the default node home.
pub const LASTDB_DIR: &str = ".lastdb";
/// Node-home override environment variable.
pub const LASTDB_HOME_ENV: &str = "LASTDB_HOME";

/// Resolve the node-home directory: `LASTDB_HOME` when set, else `~/.lastdb`.
pub fn folddb_home() -> Result<PathBuf, String> {
    if let Ok(home) = std::env::var(LASTDB_HOME_ENV) {
        return expand_tilde_path(&home);
    }
    dirs::home_dir()
        .map(|home| home.join(LASTDB_DIR))
        .ok_or_else(|| "Cannot determine home directory".to_string())
}

/// Tilde-expand a `PathBuf`/`Path` input.
pub fn expand_tilde_path(path: impl AsRef<Path>) -> Result<PathBuf, String> {
    let path = path.as_ref();
    if let Some(raw) = path.to_str().filter(|r| *r == "~" || r.starts_with("~/")) {
        let home = std::env::var("HOME").map_err(|_| {
            format!("Cannot expand ~ in path \"{raw}\": HOME environment variable not set")
        })?;
        return Ok(PathBuf::from(home).join(raw.strip_prefix("~/").unwrap_or("")));
    }
    Ok(path.to_path_buf())
}
