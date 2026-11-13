//! Cross-environment endpoint registry and `LASTDB_HOME` / tilde path helpers.

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

    pub fn environment_for_exemem_api_url(api_url: &str) -> Option<Environment> {
        let url = api_url.trim().trim_end_matches('/');
        [Environment::Dev, Environment::Prod]
            .into_iter()
            .find(|env| exemem_api_url_for(*env).trim_end_matches('/') == url)
    }

    impl Environment {
        pub fn from_env() -> Self {
            EXPLICIT_ENV
                .get_or_init(|| {
                    let raw = std::env::var("ENVIRONMENT")
                        .ok()
                        .filter(|v| !v.is_empty())
                        .or_else(|| std::env::var("EXEMEM_ENV").ok());
                    let Some(raw) = raw.as_deref() else {
                        tracing::info!(
                            "ENVIRONMENT/EXEMEM_ENV not set; using pinned home env or build default ({})",
                            Self::build_default()
                        );
                        return None;
                    };
                    Some(Self::try_from_str(raw).unwrap_or_else(|_| {
                        tracing::error!("ENVIRONMENT/EXEMEM_ENV has unknown value '{raw}', defaulting to dev");
                        Self::Dev
                    }))
                })
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
        std::env::var("FOLD_SCHEMA_SERVICE_URL")
            .ok()
            .filter(|u| !u.trim().is_empty())
            .unwrap_or_else(|| schema_service_url_for(Environment::from_env()).to_string())
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
            return Ok(PathBuf::from(home).join(raw.strip_prefix("~/").unwrap_or("")));
        }
        Ok(p.to_path_buf())
    }
}
