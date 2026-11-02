//! Stable command links and atomic symlink replacement.

use super::managed_manifest::{relative, InstallPlan};
use std::{
    fs,
    path::{Path, PathBuf},
};

pub fn bin_root() -> Result<PathBuf, String> {
    std::env::var_os("HOME")
        .map(|p| PathBuf::from(p).join(".local/bin"))
        .ok_or_else(|| "HOME is required".into())
}

pub fn validate_sources(source: &Path, plan: &InstallPlan) -> Result<(), String> {
    let source = fs::canonicalize(source).map_err(|e| e.to_string())?;
    for link in &plan.links {
        if ["lastdb", "lastdbd", "folddb", "host-track"].contains(&link.command.as_str()) {
            return Err("app links cannot replace the primary program pair or host-track".into());
        }
        let path =
            fs::canonicalize(source.join(relative(&link.source)?)).map_err(|e| e.to_string())?;
        if !path.starts_with(&source) || !path.is_file() {
            return Err("app command escapes source tree".into());
        }
    }
    Ok(())
}

pub fn flip(link: &Path, target: &Path) -> Result<(), String> {
    let temporary = link.with_extension(format!("new-{}", std::process::id()));
    #[cfg(unix)]
    std::os::unix::fs::symlink(target, &temporary).map_err(|e| e.to_string())?;
    #[cfg(not(unix))]
    return Err("managed app links require a Unix host".into());
    fs::rename(&temporary, link).map_err(|e| {
        let _ = fs::remove_file(&temporary);
        e.to_string()
    })
}

pub fn install(root: &Path, plan: &InstallPlan) -> Result<(), String> {
    validate_sources(&root.join("current/source"), plan)?;
    let bin = bin_root()?;
    fs::create_dir_all(&bin).map_err(|e| e.to_string())?;
    for link in &plan.links {
        let target = bin.join(&link.command);
        if target
            .symlink_metadata()
            .is_ok_and(|m| !m.file_type().is_symlink())
        {
            return Err(format!(
                "refusing to replace a regular file: {}",
                target.display()
            ));
        }
        flip(&target, &root.join("current/source").join(&link.source))?;
    }
    Ok(())
}

pub fn verify(root: &Path, plan: &InstallPlan) -> Result<(), String> {
    validate_sources(&root.join("current/source"), plan)?;
    for link in &plan.links {
        let expected = root.join("current/source").join(&link.source);
        let actual = fs::read_link(bin_root()?.join(&link.command)).map_err(|e| e.to_string())?;
        if actual != expected {
            return Err(format!("command link drift: {}", link.command));
        }
    }
    Ok(())
}

pub fn snapshot(plan: &InstallPlan) -> Result<Vec<(PathBuf, Option<PathBuf>)>, String> {
    let mut result = Vec::new();
    for link in &plan.links {
        let path = bin_root()?.join(&link.command);
        if path
            .symlink_metadata()
            .is_ok_and(|m| !m.file_type().is_symlink())
        {
            return Err(format!(
                "refusing to replace a regular file: {}",
                path.display()
            ));
        }
        result.push((path.clone(), fs::read_link(path).ok()));
    }
    Ok(result)
}

pub fn restore(snapshot: &[(PathBuf, Option<PathBuf>)]) -> Result<(), String> {
    for (path, target) in snapshot {
        if let Some(target) = target {
            flip(path, target)?;
        } else if path.symlink_metadata().is_ok() {
            fs::remove_file(path).map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}
