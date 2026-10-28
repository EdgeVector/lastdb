use std::path::PathBuf;

/// Native local storage configuration. Callers build it in code; no
/// configuration file decodes into it. Native storage is unconditional; the
/// retired `LASTDB_ENGINE` environment variable has no effect.
#[derive(Clone, Debug)]
pub struct DatabaseConfig {
    /// Path to the local database directory (Last Store root)
    pub path: PathBuf,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("data"),
        }
    }
}

impl DatabaseConfig {
    /// Create a local-only config
    pub fn local(path: PathBuf) -> Self {
        Self { path }
    }
}
