//! Persistent service-home configuration for `lastdbd` when no CLI/env home override is set.

use std::fs;
use std::path::PathBuf;

pub fn explicit_home_override_present() -> bool {
    std::env::var_os(crate::profile::paths::LASTDB_HOME_ENV).is_some()
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
        PathBuf::from(
            std::env::var_os("HOME")
                .ok_or("Cannot locate lastdb admin service-home config: HOME is not set")?,
        )
        .join(sub)
    };
    let raw = match fs::read_to_string(&path) {
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
    let home = crate::profile::paths::expand_tilde_path(raw.trim())?;
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
    Ok(Some(home))
}
