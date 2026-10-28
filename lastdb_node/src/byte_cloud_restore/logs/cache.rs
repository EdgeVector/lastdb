//! The accepted chain stays in the plan; verified transient object bytes need not accumulate.
use super::*;

pub(super) fn clear(session: &RestoreSession) -> Result<()> {
    let root = session.root.join("cache");
    local::directory(&root)?;
    let mut files = Vec::new();
    for entry in fs::read_dir(&root)? {
        if files.len() > 2 * cloud_backup_protocol::v2::limits::MAX_PAGES + 8 {
            return Err(RestoreError::Limit("mutation metadata cache files"));
        }
        let entry = entry?;
        let name = entry.file_name();
        let name = name
            .to_str()
            .ok_or(RestoreError::Invalid("mutation cache name"))?;
        let digest = name.strip_suffix(".object").is_some_and(|id| {
            id.len() == 64
                && id
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        });
        if !digest && name != "payload.object" {
            return Err(RestoreError::Invalid("mutation cache entry"));
        }
        local::owner_file(&entry.path(), false)?;
        files.push(entry.path());
    }
    // No download future or payload reader remains at these call sites.
    for path in files {
        session.remove_spool_file(&path)?;
    }
    File::open(root)?.sync_all()?;
    Ok(())
}
