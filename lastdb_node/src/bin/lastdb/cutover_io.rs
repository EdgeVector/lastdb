//! Read cutover evidence. This file does not open the daemon socket and does not write.

use std::path::{Path, PathBuf};

use database_cutover::{
    parse_bundle, EvidenceBytes, DAEMON_READY_FILE, DELIVERY_READY_FILE, FINAL_WRITES_FILE,
    GRANTS_FILE, LEGACY_POSITION_FILE, MAX_EVIDENCE_BYTES, SNAPSHOT_FILE,
};

pub(crate) fn load_bundle(dir: &Path) -> Result<database_cutover::Bundle, String> {
    let dir = require_dir(dir)?;
    let snapshot = read_bounded(&dir.join(SNAPSHOT_FILE))?;
    let legacy_position = read_bounded(&dir.join(LEGACY_POSITION_FILE))?;
    let final_writes = read_bounded(&dir.join(FINAL_WRITES_FILE))?;
    let grants = read_bounded(&dir.join(GRANTS_FILE))?;
    let daemon_ready = read_bounded(&dir.join(DAEMON_READY_FILE))?;
    let delivery_ready = read_bounded(&dir.join(DELIVERY_READY_FILE))?;
    let files = EvidenceBytes {
        snapshot: &snapshot,
        legacy_position: &legacy_position,
        final_writes: &final_writes,
        grants: &grants,
        daemon_ready: &daemon_ready,
        delivery_ready: &delivery_ready,
    };
    parse_bundle(&files).map_err(|refusal| refusal.to_string())
}

fn require_dir(path: &Path) -> Result<PathBuf, String> {
    let located = locate(path)?;
    if !located.is_dir() {
        return Err(format!("{} is not a directory", path.display()));
    }
    Ok(located)
}

fn read_bounded(path: &Path) -> Result<Vec<u8>, String> {
    let located = locate(path)?;
    let meta =
        std::fs::metadata(&located).map_err(|_| format!("cannot read {}", path.display()))?;
    if !meta.is_file() {
        return Err(format!("{} is not a file", path.display()));
    }
    let cap =
        u64::try_from(MAX_EVIDENCE_BYTES).map_err(|_| "evidence cap is invalid".to_owned())?;
    if meta.len() > cap {
        return Err(format!("{} exceeds the evidence size cap", path.display()));
    }
    std::fs::read(&located).map_err(|_| format!("cannot read {}", path.display()))
}

fn locate(path: &Path) -> Result<PathBuf, String> {
    let located = if path.exists() {
        path.canonicalize()
            .map_err(|_| format!("cannot read {}", path.display()))?
    } else {
        let Some(name) = path.file_name() else {
            return Err(format!("{} has no file name", path.display()));
        };
        let Some(parent) = path.parent() else {
            return Err(format!("{} has no parent directory", path.display()));
        };
        if !parent.exists() {
            return Err(format!(
                "parent directory does not exist: {}",
                parent.display()
            ));
        }
        let parent = parent
            .canonicalize()
            .map_err(|_| format!("cannot read {}", parent.display()))?;
        parent.join(name)
    };
    refuse_primary(&located)?;
    Ok(located)
}

fn refuse_primary(path: &Path) -> Result<(), String> {
    if primary_roots().iter().any(|root| path.starts_with(root)) {
        return Err(format!(
            "refusing {}: it is inside the primary LastDB home",
            path.display()
        ));
    }
    Ok(())
}

fn primary_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Some(home) = std::env::var_os("HOME") {
        push_root(&mut roots, PathBuf::from(home).join(".lastdb"));
    }
    if let Some(home) = std::env::var_os("LASTDB_HOME") {
        push_root(&mut roots, PathBuf::from(home));
    }
    roots
}

fn push_root(roots: &mut Vec<PathBuf>, path: PathBuf) {
    if let Ok(canonical) = path.canonicalize() {
        roots.push(canonical);
        return;
    }
    roots.push(path);
}
