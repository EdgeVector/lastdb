//! Cross-environment endpoint registry and default home path helpers.

pub mod endpoints {
    use std::sync::OnceLock;

    mod gen {
        include!(concat!(env!("OUT_DIR"), "/environments_generated.rs"));
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Environment {
        Dev,
        Prod,
    }

    static EXPLICIT_ENV: OnceLock<Option<Environment>> = OnceLock::new();
    static HOME_ENV: OnceLock<Environment> = OnceLock::new();

    #[must_use]
    pub const fn registry_index_url_for(env: Environment) -> &'static str {
        match env {
            Environment::Dev => gen::DEV_REGISTRY_INDEX,
            Environment::Prod => gen::PROD_REGISTRY_INDEX,
        }
    }

    #[must_use]
    pub const fn registry_api_url_for(env: Environment) -> &'static str {
        match env {
            Environment::Dev => gen::DEV_REGISTRY_API,
            Environment::Prod => gen::PROD_REGISTRY_API,
        }
    }

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

    pub fn set_home_environment(env: Environment) -> bool {
        *HOME_ENV.get_or_init(|| env) == env
    }

    #[must_use]
    pub fn environment_for_exemem_api_url(api_url: &str) -> Option<Environment> {
        let url = api_url.trim().trim_end_matches('/');
        [Environment::Dev, Environment::Prod]
            .into_iter()
            .find(|e| exemem_api_url_for(*e).trim_end_matches('/') == url)
    }

    #[must_use]
    pub fn environment_for_registry_api_url(api_url: &str) -> Option<Environment> {
        let url = api_url.trim().trim_end_matches('/');
        [Environment::Dev, Environment::Prod]
            .into_iter()
            .find(|e| registry_api_url_for(*e).trim_end_matches('/') == url)
    }

    impl Environment {
        #[must_use]
        pub fn explicit_from_env() -> Option<Self> {
            *EXPLICIT_ENV.get_or_init(|| {
                let non_empty = |name| std::env::var(name).ok().filter(|v| !v.trim().is_empty());
                let Some(raw) = non_empty("LASTDB_ENV")
                    .or_else(|| non_empty("ENVIRONMENT"))
                    .or_else(|| non_empty("EXEMEM_ENV"))
                else {
                    tracing::info!(
                        "LASTDB_ENV/ENVIRONMENT/EXEMEM_ENV not set; using pinned home env or build default ({})",
                        Self::build_default()
                    );
                    return None;
                };
                Some(Self::try_from_str(raw.trim()).unwrap_or_else(|_| {
                    tracing::error!(
                        "LASTDB_ENV/ENVIRONMENT/EXEMEM_ENV has unknown value '{raw}', defaulting to dev"
                    );
                    Self::Dev
                }))
            })
        }

        #[must_use]
        pub fn from_env() -> Self {
            Self::explicit_from_env()
                .or_else(|| HOME_ENV.get().copied())
                .unwrap_or_else(Self::build_default)
        }

        #[must_use]
        pub fn from_env_or_prod() -> Self {
            Self::explicit_from_env()
                .or_else(|| HOME_ENV.get().copied())
                .unwrap_or(Self::Prod)
        }

        const fn build_default() -> Self {
            if cfg!(debug_assertions) {
                Self::Dev
            } else {
                Self::Prod
            }
        }

        pub fn try_from_str(raw: &str) -> Result<Self, &'static str> {
            match raw {
                "prod" | "production" => Ok(Self::Prod),
                "dev" | "development" => Ok(Self::Dev),
                _ => Err("unknown environment"),
            }
        }

        pub const fn as_str(self) -> &'static str {
            match self {
                Self::Dev => "dev",
                Self::Prod => "prod",
            }
        }
    }

    impl std::fmt::Display for Environment {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.as_str())
        }
    }

    pub fn schema_service_url() -> String {
        let env_url = std::env::var("FOLD_SCHEMA_SERVICE_URL")
            .ok()
            .filter(|u| !u.trim().is_empty());
        env_url.unwrap_or_else(|| schema_service_url_for(Environment::from_env()).to_string())
    }

    pub const fn schema_service_url_for(env: Environment) -> &'static str {
        match env {
            Environment::Dev => gen::DEV_SCHEMA_SERVICE,
            Environment::Prod => gen::PROD_SCHEMA_SERVICE,
        }
    }

    pub const fn exemem_api_url_for(env: Environment) -> &'static str {
        match env {
            Environment::Dev => gen::DEV_EXEMEM_API,
            Environment::Prod => gen::PROD_EXEMEM_API,
        }
    }
}

pub mod paths {
    use std::path::{Path, PathBuf};

    pub const LASTDB_DIR: &str = ".lastdb";
    pub const LASTDB_HOME_ENV: &str = "LASTDB_HOME";

    pub fn folddb_home() -> Result<PathBuf, String> {
        if let Ok(home) = std::env::var(LASTDB_HOME_ENV) {
            return expand_tilde_path(&home);
        }
        dirs::home_dir()
            .map(|h| h.join(LASTDB_DIR))
            .ok_or_else(|| "Cannot determine home directory".into())
    }

    pub fn expand_tilde_path(path: impl AsRef<Path>) -> Result<PathBuf, String> {
        let p = path.as_ref();
        if let Some(raw) = p.to_str().filter(|r| *r == "~" || r.starts_with("~/")) {
            let home = std::env::var("HOME").map_err(|_| {
                format!("Cannot expand ~ in path \"{raw}\": HOME environment variable not set")
            })?;
            return Ok(if let Some(rest) = raw.strip_prefix("~/") {
                PathBuf::from(home).join(rest)
            } else {
                PathBuf::from(home)
            });
        }
        Ok(p.to_path_buf())
    }
}
