use std::path::PathBuf;

/// Local primary storage engine for the FoldDB namespaced data path.
///
/// Last Store names the native packed engine. Normal construction has no
/// old-format backend selection or migration fallback.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StorageEngine {
    /// Native packed storage (the sole product engine).
    #[default]
    Laststore,
}

impl StorageEngine {
    pub fn from_env() -> Result<Option<Self>, String> {
        match std::env::var("LASTDB_ENGINE") {
            Ok(raw) => raw.parse::<Self>().map(Some).map_err(|_| {
                format!("invalid LASTDB_ENGINE '{raw}'; only 'laststore' is supported")
            }),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(std::env::VarError::NotUnicode(_)) => {
                Err("invalid LASTDB_ENGINE; value is not valid UTF-8".to_string())
            }
        }
    }
}

impl std::str::FromStr for StorageEngine {
    type Err = ();

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" | "laststore" => Ok(Self::Laststore),
            _ => Err(()),
        }
    }
}

/// Native local storage configuration. Callers build it in code; no
/// configuration file decodes into it.
#[derive(Clone, Debug)]
pub struct DatabaseConfig {
    /// Path to the local database directory (Last Store root)
    pub path: PathBuf,
    /// Primary namespaced storage engine (Last Store only).
    pub engine: StorageEngine,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("data"),
            engine: StorageEngine::Laststore,
        }
    }
}

impl DatabaseConfig {
    /// Create a local-only config
    pub fn local(path: PathBuf) -> Self {
        Self {
            path,
            engine: StorageEngine::Laststore,
        }
    }
}
