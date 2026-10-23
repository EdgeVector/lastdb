//! Refuse legacy interrupted repair state without reading or replaying it.

use super::{SyncError, SyncResult};
use std::path::Path;

pub fn check_retired_cloud_repair(home: &Path) -> SyncResult<()> {
    let home = std::fs::canonicalize(home)?;
    let parent = home
        .parent()
        .ok_or_else(|| SyncError::Storage("cloud repair home has no parent".into()))?;
    let identity = crate::hex::sha256_hex(home.as_os_str().as_encoded_bytes());
    let peer = parent
        .join(".lastdb-cloud-purge")
        .join(identity)
        .join("peer");
    for path in [home.join("cloud-purge-resync"), peer] {
        match std::fs::symlink_metadata(&path) {
            Ok(_) => {
                return Err(SyncError::Storage(format!(
                    "unsupported retired cloud repair state at {}; retain the home and use an explicit recovery procedure; startup does not replay repair records",
                    path.display()
                )));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}
