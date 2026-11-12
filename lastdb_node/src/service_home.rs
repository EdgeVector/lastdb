//! Persistent service-home configuration for `lastdbd` when no CLI/env home override is set.

use std::fs;
use std::path::{Path, PathBuf};

pub fn explicit_home_override_present() -> bool {
    std::env::var_os(crate::profile::paths::LASTDB_HOME_ENV).is_some()
}

pub fn configured_home() -> Result<Option<PathBuf>, String> {
    let path = if let Some(p) = std::env::var_os("LASTDBD_SERVICE_HOME_CONFIG") {
        PathBuf::from(p)
    } else {
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .ok_or("Cannot locate lastdb admin service-home config: HOME is not set")?;
        home.join(if cfg!(target_os = "macos") {
            "Library/Application Support/LastDB/lastdbd-service-home"
        } else {
            ".config/lastdb/lastdbd-service-home"
        })
    };
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
    if let Some(primary) = std::env::var_os("HOME")
        .map(|h| PathBuf::from(h).join(crate::profile::paths::LASTDB_DIR))
        .filter(|p| p.is_dir())
    {
        let canon = fs::canonicalize(&primary).unwrap_or(primary);
        if fs::canonicalize(&home).unwrap_or_else(|_| home.clone()) == canon {
            return Err(format!(
                "Refusing to configure lastdbd service home as the primary live database home {}. Choose a separate directory for a second node.",
                canon.display()
            ));
        }
    }
    Ok(home)
}
