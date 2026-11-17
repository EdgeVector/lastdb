//! Cross-environment endpoint registry, home path helpers, private-file I/O, and share specs.

use base64::Engine as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const IDENTITY_KEY_FILE: &str = "identity.key";

pub fn user_hash_from_pubkey(pubkey_b64: &str) -> Result<String, String> {
    let raw = base64::engine::general_purpose::STANDARD
        .decode(pubkey_b64)
        .map_err(|e| format!("node public key is not valid base64: {e}"))?;
    Ok(fold_db::hex::hex_lower(&Sha256::digest(&raw)[..16]))
}

/// Registry `spec` object stored on a publication marker that named one.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PublicationSpec {
    /// Spec name. This is not a database name.
    pub(crate) name: String,
    /// Registry version. Catalog label `1.0` is `1`.
    pub(crate) version: u32,
}

impl PublicationSpec {
    pub(crate) fn whole_database(&self) -> bool {
        database_policy::SpecBinding::new(&self.name, self.version)
            .is_ok_and(|binding| binding.is_whole_database())
    }
}

fn valid_label(value: &str, max_len: usize) -> bool {
    (1..=max_len).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

#[must_use]
pub(crate) fn valid_label_64(value: &str) -> bool {
    valid_label(value, 64)
}

#[must_use]
pub(crate) fn valid_label_128(value: &str) -> bool {
    valid_label(value, 128)
}

#[must_use]
pub fn valid_database_name(name: &str) -> bool {
    valid_label_64(name)
        && !name.eq_ignore_ascii_case("personal")
        && uuid::Uuid::parse_str(name).is_err()
}

pub mod private_fs {
    use std::fs::{self, File, OpenOptions};
    use std::io::{self, Read, Write};
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    use std::path::Path;

    pub fn read_file_limited(
        path: &Path,
        limit: usize,
        require_private_mode: bool,
    ) -> io::Result<Option<Vec<u8>>> {
        let metadata = match fs::symlink_metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        if !metadata.is_file()
            || (require_private_mode && metadata.permissions().mode() & 0o022 != 0)
            || metadata.len() > limit as u64
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "private file must be a bounded regular file",
            ));
        }
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)?;
        let mut bytes = Vec::new();
        file.take(limit as u64 + 1).read_to_end(&mut bytes)?;
        if bytes.len() > limit {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "private file exceeds its size limit",
            ));
        }
        Ok(Some(bytes))
    }

    pub(crate) fn read_limited(path: &Path, limit: usize) -> io::Result<Option<Vec<u8>>> {
        read_file_limited(path, limit, true)
    }

    pub(crate) fn write_private(path: &Path, bytes: &[u8]) -> io::Result<()> {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        fs::create_dir_all(parent)?;
        let temporary = parent.join(format!(".{}.tmp", uuid::Uuid::new_v4()));
        let write = (|| -> io::Result<()> {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW)
                .open(&temporary)?;
            file.set_permissions(fs::Permissions::from_mode(0o600))?;
            file.write_all(bytes)?;
            file.sync_all()
        })();
        if let Err(error) = write {
            let _ = fs::remove_file(&temporary);
            return Err(error);
        }
        if let Err(error) = fs::rename(&temporary, path) {
            let _ = fs::remove_file(&temporary);
            return Err(error);
        }
        File::open(parent).and_then(|directory| directory.sync_all())
    }

    pub(crate) fn remove_private(path: &Path) -> io::Result<()> {
        match fs::symlink_metadata(path) {
            Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
                fs::remove_file(path)?;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "private file must be a regular file",
                ));
            }
        }
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        File::open(parent).and_then(|directory| directory.sync_all())
    }
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
    pub const fn registry_api_url_for(env: Environment) -> &'static str {
        match env {
            Environment::Dev => gen::DEV_REGISTRY_API,
            Environment::Prod => gen::PROD_REGISTRY_API,
        }
    }

    #[must_use]
    pub const fn registry_list_key_for(env: Environment) -> &'static str {
        match env {
            Environment::Dev => gen::DEV_REGISTRY_LIST_KEY,
            Environment::Prod => gen::PROD_REGISTRY_LIST_KEY,
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
    pub(crate) fn environment_for_exemem_api_url(api_url: &str) -> Option<Environment> {
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

    #[must_use]
    pub fn environment_for_registry_index_url(index_url: &str) -> Option<Environment> {
        let url = index_url.trim().trim_end_matches('/');
        [Environment::Dev, Environment::Prod]
            .into_iter()
            .find(|e| registry_index_url_for(*e).trim_end_matches('/') == url)
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
        pub(crate) fn from_env() -> Self {
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

    pub(crate) const LASTDB_DIR: &str = ".lastdb";
    pub(crate) const LASTDB_HOME_ENV: &str = "LASTDB_HOME";

    pub(crate) fn folddb_home() -> Result<PathBuf, String> {
        if let Ok(home) = std::env::var(LASTDB_HOME_ENV) {
            return expand_tilde_path(&home);
        }
        std::env::home_dir()
            .filter(|p| !p.as_os_str().is_empty())
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
