//! Expensive full archive verification runs at switch boundaries, not per queue page.

use super::*;
use std::collections::{BTreeMap, BTreeSet};

impl CloudPurgePeer {
    /// Verify the exact inventory and every old-data byte before a resumed
    /// switch or final local adoption commit. Queue-page retention remains cheap.
    pub fn verify_retained_files(
        &self,
        plan: &AdoptionPlan,
        retained: &RetainedHome,
    ) -> SyncResult<()> {
        self.verify_retained_home(plan, retained)?;
        let encoded = read_manifest(&retained.directory)?;
        let manifest: RetentionManifest = serde_json::from_slice(&encoded)?;
        if retained.files != manifest.files.len() as u64
            || retained.bytes != manifest.files.iter().map(|file| file.bytes).sum::<u64>()
        {
            return Err(held("the retained data inventory totals changed"));
        }
        verify_files(&retained.directory, &manifest)
    }
}

pub(super) fn verify_files(directory: &Path, manifest: &RetentionManifest) -> SyncResult<()> {
    let root = directory.join("home");
    let mut expected = BTreeMap::new();
    for file in &manifest.files {
        if file.relative_path.as_os_str().is_empty()
            || file
                .relative_path
                .components()
                .any(|part| !matches!(part, std::path::Component::Normal(_)))
            || !crate::hex::is_lower_hex_sha256(&file.sha256)
            || expected
                .insert(file.relative_path.clone(), (&file.sha256, file.bytes))
                .is_some()
        {
            return Err(held("the retained data inventory contains an invalid path"));
        }
    }
    let mut found = BTreeSet::new();
    inventory(&root, &root, &expected, &mut found, 0)?;
    if found.len() != expected.len() {
        return Err(held("a retained old-data file is absent"));
    }
    Ok(())
}

fn inventory(
    root: &Path,
    current: &Path,
    expected: &BTreeMap<PathBuf, (&String, u64)>,
    found: &mut BTreeSet<PathBuf>,
    depth: usize,
) -> SyncResult<()> {
    if depth > 64 || !fs::symlink_metadata(current)?.file_type().is_dir() {
        return Err(held("the retained data directory type or depth differs"));
    }
    for entry in fs::read_dir(current)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        let path = entry.path();
        if kind.is_dir() {
            inventory(root, &path, expected, found, depth + 1)?;
        } else if kind.is_file() {
            let relative = path
                .strip_prefix(root)
                .map_err(|_| held("the retained path escaped its root"))?;
            let (hash, bytes) = expected
                .get(relative)
                .ok_or_else(|| held("the retained data contains an unlisted file"))?;
            let actual = digest_file(&path)?;
            if actual.0 != **hash || actual.1 != *bytes {
                return Err(held("a retained old-data file changed"));
            }
            found.insert(relative.to_path_buf());
        } else {
            return Err(held("the retained data contains a link or special file"));
        }
    }
    Ok(())
}
