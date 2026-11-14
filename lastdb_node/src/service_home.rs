//! Admin service-home configuration and explicit `LASTDB_HOME` override detection.

use std::path::PathBuf;

use crate::profile::paths;

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
