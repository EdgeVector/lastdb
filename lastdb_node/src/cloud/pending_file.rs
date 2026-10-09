//! Owner-only pending file with rollback on drop.

use super::*;

pub(super) struct PendingOwnerOnlyFile {
    path: PathBuf,
    armed: bool,
    #[cfg(unix)]
    created_device: u64,
    #[cfg(unix)]
    created_inode: u64,
}

impl PendingOwnerOnlyFile {
    pub(super) fn create(path: &Path, bytes: &[u8], label: &str) -> Result<Self, String> {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        let mut file = options.open(path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                format!("refusing to replace existing {label}: {}", path.display())
            } else {
                format!("failed to create {label} at {}: {e}", path.display())
            }
        })?;
        let created_metadata = file
            .metadata()
            .map_err(|e| format!("failed to inspect new {label} at {}: {e}", path.display()))?;
        let mut pending = Self {
            path: path.to_path_buf(),
            armed: true,
            #[cfg(unix)]
            created_device: {
                use std::os::unix::fs::MetadataExt as _;
                created_metadata.dev()
            },
            #[cfg(unix)]
            created_inode: {
                use std::os::unix::fs::MetadataExt as _;
                created_metadata.ino()
            },
        };
        file.write_all(bytes)
            .map_err(|e| format!("failed to write {label} at {}: {e}", path.display()))?;
        file.sync_all()
            .map_err(|e| format!("failed to sync {label} at {}: {e}", path.display()))?;
        drop(file);
        let persisted = std::fs::read(path).map_err(|e| {
            format!(
                "failed to verify persisted {label} at {}: {e}",
                path.display()
            )
        })?;
        if persisted != bytes {
            return Err(format!(
                "persisted {label} did not match the requested value at {}",
                path.display()
            ));
        }
        pending.verify_owner_only(label)?;
        Ok(pending)
    }

    #[cfg(unix)]
    fn verify_owner_only(&mut self, label: &str) -> Result<(), String> {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
        let metadata = std::fs::symlink_metadata(&self.path).map_err(|e| {
            format!(
                "failed to inspect persisted {label} at {}: {e}",
                self.path.display()
            )
        })?;
        if !metadata.file_type().is_file()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.permissions().mode() & 0o777 != 0o600
            || metadata.dev() != self.created_device
            || metadata.ino() != self.created_inode
        {
            return Err(format!(
                "persisted {label} is not an owner-only regular file at {}",
                self.path.display()
            ));
        }
        Ok(())
    }

    #[cfg(not(unix))]
    fn verify_owner_only(&mut self, _label: &str) -> Result<(), String> {
        Ok(())
    }

    pub(super) fn commit(mut self) {
        self.armed = false;
    }

    #[cfg(unix)]
    fn still_owns_path(&self) -> bool {
        use std::os::unix::fs::MetadataExt as _;
        std::fs::symlink_metadata(&self.path).is_ok_and(|metadata| {
            metadata.file_type().is_file()
                && metadata.uid() == unsafe { libc::geteuid() }
                && metadata.dev() == self.created_device
                && metadata.ino() == self.created_inode
        })
    }

    #[cfg(not(unix))]
    fn still_owns_path(&self) -> bool {
        true
    }
}

impl Drop for PendingOwnerOnlyFile {
    fn drop(&mut self) {
        if self.armed && self.still_owns_path() {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

pub(super) fn write_new_cloud_sync_config(
    home: &Path,
    api_url: &str,
    api_key: &str,
) -> Result<(), String> {
    let body = serde_json::json!({ "api_url": api_url, "api_key": api_key });
    let bytes = serde_json::to_vec_pretty(&body)
        .map_err(|_| "failed to serialize new DEV cloud credentials".to_string())?;
    PendingOwnerOnlyFile::create(
        &home.join(CLOUD_SYNC_CONFIG_FILE),
        &bytes,
        "DEV cloud credentials",
    )?
    .commit();
    Ok(())
}
