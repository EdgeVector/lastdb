//! Cross-environment endpoint registry, node identity keyfile, and home path helpers.

use std::io::Write as _;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::{Path, PathBuf};

use base64::Engine as _;
use sha2::{Digest, Sha256};

pub const IDENTITY_KEY_FILE: &str = "identity.key";

pub fn load_or_generate_seed_with_meta(home: &Path) -> Result<([u8; 32], bool), String> {
    let key_path = home.join(IDENTITY_KEY_FILE);
    let kp = key_path.display();
    if key_path.exists() {
        let bytes = std::fs::read(&key_path).map_err(|e| format!("failed to read {kp}: {e}"))?;
        let len = bytes.len();
        let seed = bytes
            .as_slice()
            .try_into()
            .map_err(|_| format!("{kp} has invalid length {len} (expected 32)"))?;
        return Ok((seed, false));
    }
    let pair = fold_db::security::Ed25519KeyPair::generate()
        .map_err(|e| format!("identity generation failed: {e}"))?;
    let seed = pair.secret_key_bytes();
    std::fs::create_dir_all(home)
        .map_err(|e| format!("failed to create node home {}: {e}", home.display()))?;
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    opts.mode(0o600);
    opts.open(&key_path)
        .and_then(|mut f| f.write_all(&seed).and_then(|()| f.sync_all()))
        .map_err(|e| format!("failed to write {kp}: {e}"))?;
    Ok((seed, true))
}

pub fn user_hash_from_pubkey(pubkey_b64: &str) -> Result<String, String> {
    let raw = base64::engine::general_purpose::STANDARD
        .decode(pubkey_b64)
        .map_err(|e| format!("node public key is not valid base64: {e}"))?;
    Ok(fold_db::hex::hex_lower(&Sha256::digest(&raw)[..16]))
}

pub fn explicit_home_override_present() -> bool {
    std::env::var_os(paths::LASTDB_HOME_ENV).is_some()
}

pub fn configured_home() -> Result<Option<PathBuf>, String> {
    let path = if let Some(p) = std::env::var_os("LASTDBD_SERVICE_HOME_CONFIG") {
        PathBuf::from(p)
    } else {
        let sub = if cfg!(target_os = "macos") {
            "Library/Application Support/LastDB/lastdbd-service-home"
        } else {
            ".config/lastdb/lastdbd-service-home"
        };
        let h = std::env::var_os("HOME")
            .ok_or("Cannot locate lastdb admin service-home config: HOME is not set")?;
        PathBuf::from(h).join(sub)
    };
    let raw = match std::fs::read_to_string(&path) {
        Ok(r) if r.trim().is_empty() => return Ok(None),
        Ok(r) => r,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(format!(
                "Failed to read lastdb admin service-home config {}: {e}",
                path.display()
            ));
        }
    };
    let home = paths::expand_tilde_path(raw.trim())?;
    if !home.is_absolute() {
        return Err(format!(
            "lastdbd service home must be absolute after ~ expansion (got {})",
            home.display()
        ));
    }
    let primary = std::env::var_os("HOME").map(|h| PathBuf::from(h).join(paths::LASTDB_DIR));
    if let Some(p) = primary.filter(|p| p.is_dir()) {
        let canon = std::fs::canonicalize(&p).unwrap_or(p);
        if std::fs::canonicalize(&home).unwrap_or_else(|_| home.clone()) == canon {
            return Err(format!(
                "Refusing to configure lastdbd service home as the primary live database home {}. Choose a separate directory for a second node.",
                canon.display()
            ));
        }
    }
    Ok(Some(home))
}

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
            .find(|e| exemem_api_url_for(*e).trim_end_matches('/') == url)
    }

    impl Environment {
        pub fn from_env() -> Self {
            let explicit = EXPLICIT_ENV.get_or_init(|| {
                let raw = std::env::var("ENVIRONMENT").ok().filter(|v| !v.is_empty());
                let Some(raw) = raw.or_else(|| std::env::var("EXEMEM_ENV").ok()) else {
                    tracing::info!("ENVIRONMENT/EXEMEM_ENV not set; using pinned home env or build default ({})", Self::build_default());
                    return None;
                };
                Some(Self::try_from_str(&raw).unwrap_or_else(|_| {
                    tracing::error!("ENVIRONMENT/EXEMEM_ENV has unknown value '{raw}', defaulting to dev");
                    Self::Dev
                }))
            });
            explicit
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
            return Ok(PathBuf::from(home).join(raw.strip_prefix("~/").unwrap_or("")));
        }
        Ok(p.to_path_buf())
    }
}
