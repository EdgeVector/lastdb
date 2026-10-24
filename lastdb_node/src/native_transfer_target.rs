//! Prepare an inactive native-record transfer target under the existing account.
//! No candidate opens the source as a writer and no source data file is copied.

use std::path::Path;

use fold_db::storage::disk_pack::{
    convert_transferred_node, prepare_restored_node, validate_restored_node,
};

use crate::offline_home::{load_e2e_keys, preflight_stopped_home, refuse_primary};

/// Validate the transferred store, preserve the account through the native key
/// service, and create the normal target wrapper. Cloud remains unconfigured.
pub fn prepare(source: &Path, target: &Path, allow_primary_source: bool) -> Result<(), String> {
    if !allow_primary_source {
        refuse_primary(source)?;
    }
    let source = source.canonicalize().map_err(|_| "source home is absent")?;
    let target = target.canonicalize().map_err(|_| "target home is absent")?;
    if source == target || source.starts_with(&target) || target.starts_with(&source) {
        return Err("source and target must be separate non-nested homes".into());
    }
    require_private_directory(&target)?;
    let source_data = source.join("data");
    let (_proof, _maintenance) = preflight_stopped_home(&source, &source_data)?;
    // Initial scope: complete native records, with no unhandled filesystem
    // dependency. Never silently omit a future external file directory.
    for entry in std::fs::read_dir(&source_data).map_err(|_| "source data cannot be read")? {
        let entry = entry.map_err(|_| "source data entry cannot be read")?;
        if !matches!(
            entry.file_name().to_str(),
            Some(
                "disk-pack"
                    | "laststore-layout-v1"
                    | "laststore-pack-node-v1"
                    | "folddb.sock"
                    | "folddb-full.sock"
                    | "maintenance.lock"
            )
        ) {
            return Err("source has an unsupported external migration dependency".into());
        }
    }
    let (keys, _) = load_e2e_keys(&source)?;
    let data = target.join("data");
    require_private_directory(&data)?;
    convert_transferred_node(&data, &keys.encryption_key())
        .map_err(|error| format!("transferred native target conversion failed: {error}"))?;
    let target_lock = prepare_restored_node(&data, &keys.encryption_key())
        .map_err(|_| "transferred native target validation failed")?;
    drop(target_lock);
    let _validated = validate_restored_node(&data, &keys.encryption_key())
        .map_err(|_| "transferred native account proof validation failed")?;
    let source_seed = lastdb_identity::load_seed(&source)?
        .ok_or_else(|| "source identity is absent".to_string())?;
    match lastdb_identity::load_seed(&target)? {
        None => lastdb_identity::write_seed(&target, &source_seed)?,
        Some(existing) if existing == source_seed => {}
        Some(_) => return Err("target account identity differs".into()),
    }
    std::fs::File::open(&target)
        .and_then(|file| file.sync_all())
        .map_err(|_| "target account directory barrier failed")?;
    Ok(())
}

fn require_private_directory(path: &Path) -> Result<(), String> {
    use std::os::unix::fs::MetadataExt;
    let metadata = std::fs::symlink_metadata(path).map_err(|_| "target directory is absent")?;
    if !metadata.is_dir()
        || metadata.mode() & 0o077 != 0
        || metadata.uid() != rustix::process::getuid().as_raw()
    {
        return Err("target must be an owner-only real directory".into());
    }
    Ok(())
}
