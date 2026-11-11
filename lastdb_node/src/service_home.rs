//! Persistent service-home configuration for `lastdbd` when no CLI/env home override is set.

use std::fs;
use std::path::{Path, PathBuf};

const CONFIG_ENV: &str = "LASTDBD_SERVICE_HOME_CONFIG";
const MACOS_CONFIG_REL: &[&str] = &[
    "Library",
    "Application Support",
    "LastDB",
    "lastdbd-service-home",
];
const UNIX_CONFIG_REL: &[&str] = &[".config", "lastdb", "lastdbd-service-home"];

pub fn explicit_home_override_present() -> bool {
    std::env::var_os(crate::profile::paths::LASTDB_HOME_ENV).is_some()
}

fn config_path() -> Result<PathBuf, String> {
    if let Some(path) = std::env::var_os(CONFIG_ENV) {
        return Ok(PathBuf::from(path));
    }
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or("Cannot locate lastdb admin service-home config: HOME is not set")?;
    let rel = if cfg!(target_os = "macos") {
        MACOS_CONFIG_REL
    } else {
        UNIX_CONFIG_REL
    };
    Ok(rel.iter().fold(home, |path, part| path.join(part)))
}

pub fn configured_home() -> Result<Option<PathBuf>, String> {
    let path = config_path()?;
    match fs::read_to_string(&path) {
        Ok(raw) if raw.trim().is_empty() => Ok(None),
        Ok(raw) => validate_service_home(raw.trim()).map(Some),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(format!(
            "Failed to read lastdb admin service-home config {}: {e}",
            path.display()
        )),
    }
}

fn validate_service_home(raw: impl AsRef<Path>) -> Result<PathBuf, String> {
    let home = crate::profile::paths::expand_tilde_path(raw)?;
    if !home.is_absolute() {
        return Err(format!(
            "lastdbd service home must be absolute after ~ expansion (got {})",
            home.display()
        ));
    }
    if let Some(user_home) = std::env::var_os("HOME").map(PathBuf::from) {
        let primary = user_home.join(crate::profile::paths::LASTDB_DIR);
        if primary.is_dir() {
            let canon_primary = fs::canonicalize(&primary).unwrap_or(primary);
            let canon_home = fs::canonicalize(&home).unwrap_or_else(|_| home.clone());
            if canon_home == canon_primary {
                return Err(format!(
                    "Refusing to configure lastdbd service home as the primary live database home {}. Choose a separate directory for a second node.",
                    canon_primary.display()
                ));
            }
        }
    }
    Ok(home)
}
