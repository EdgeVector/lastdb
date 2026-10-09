//! Persistent service-home configuration for the minimal `lastdbd` daemon.
//!
//! `brew services start lastdb` cannot pass `--data-dir`, and the formula should
//! not bake a personal path into its service block. This module provides a small
//! per-user config file that `lastdbd` reads only when no explicit CLI/env home
//! override is present.

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
    std::env::var_os(folddb_profile::paths::LASTDB_HOME_ENV).is_some()
        || std::env::var_os(folddb_profile::paths::FOLDDB_HOME_ENV).is_some()
}

pub fn config_path() -> Result<PathBuf, String> {
    if let Some(path) = std::env::var_os(CONFIG_ENV) {
        return Ok(PathBuf::from(path));
    }
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| "Cannot locate lastdbd service-home config: HOME is not set".to_string())?;
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
        Ok(raw) => {
            let raw = raw.trim();
            if raw.is_empty() {
                return Ok(None);
            }
            let home = normalize_home_path(raw)?;
            reject_primary_live_home(&home)?;
            Ok(Some(home))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(format!(
            "Failed to read lastdbd service-home config {}: {e}",
            path.display()
        )),
    }
}

pub fn set_configured_home(raw_home: &Path) -> Result<String, String> {
    let home = normalize_home_path(raw_home)?;
    reject_primary_live_home(&home)?;

    let path = config_path()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            format!(
                "Failed to create lastdbd service-home config directory {}: {e}",
                parent.display()
            )
        })?;
    }
    fs::write(&path, format!("{}\n", home.display())).map_err(|e| {
        format!(
            "Failed to write lastdbd service-home config {}: {e}",
            path.display()
        )
    })?;
    Ok(format!(
        "lastdbd service home set to {}\nConfig: {}\nRestart the Homebrew service with `brew services restart lastdb`.",
        home.display(),
        path.display()
    ))
}

pub fn show_configured_home() -> Result<String, String> {
    let path = config_path()?;
    match configured_home()? {
        Some(home) => Ok(format!(
            "lastdbd service home: {}\nConfig: {}",
            home.display(),
            path.display()
        )),
        None => Ok(format!(
            "No lastdbd service home configured.\nConfig: {}\nService start will use LASTDB_HOME/FOLDDB_HOME/default resolution.",
            path.display()
        )),
    }
}

pub fn clear_configured_home() -> Result<String, String> {
    let path = config_path()?;
    match fs::remove_file(&path) {
        Ok(()) => Ok(format!(
            "Removed lastdbd service-home config {}\nService start will use LASTDB_HOME/FOLDDB_HOME/default resolution.",
            path.display()
        )),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(format!(
            "No lastdbd service-home config existed at {}",
            path.display()
        )),
        Err(e) => Err(format!(
            "Failed to remove lastdbd service-home config {}: {e}",
            path.display()
        )),
    }
}

fn normalize_home_path(path: impl AsRef<Path>) -> Result<PathBuf, String> {
    let expanded = folddb_profile::paths::expand_tilde_path(path)?;
    if !expanded.is_absolute() {
        return Err(format!(
            "lastdbd service home must be absolute after ~ expansion (got {})",
            expanded.display()
        ));
    }
    Ok(expanded)
}

fn reject_primary_live_home(home: &Path) -> Result<(), String> {
    let Some(primary) = primary_live_home() else {
        return Ok(());
    };
    if canonical_if_exists(home) == primary {
        return Err(format!(
            "Refusing to configure lastdbd service home as the primary live database home {}. Choose a separate directory for a second node.",
            primary.display()
        ));
    }
    Ok(())
}

fn primary_live_home() -> Option<PathBuf> {
    let home = std::env::var_os("HOME").map(PathBuf::from)?;
    let primary = home.join(folddb_profile::paths::FOLDDB_DIR);
    primary.is_dir().then(|| canonical_if_exists(&primary))
}

fn canonical_if_exists(path: &Path) -> PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}
