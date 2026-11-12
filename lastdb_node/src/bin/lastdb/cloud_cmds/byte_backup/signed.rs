//! Publish a fresh home only after its signed owner prefix is durable.

use std::fs::{self, File};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cloud_backup_protocol::CloudScope;
use lastdb_node::Host;
use zeroize::Zeroizing;

use super::{id, print_backup, with_gen, BackupRestoreArgs, FreshHome, IDENTITY_KEY_FILE};

pub(super) async fn restore(
    connection_home: &Path,
    scope: &CloudScope,
    phrase: &str,
    args: BackupRestoreArgs,
) -> Result<(), String> {
    let destination = args.into;
    let database = id(args.stream)?;
    let parent = destination.parent().ok_or("missing recovery parent")?;
    FreshHome::preflight_empty(&destination)?;
    let staged_path = parent.join(format!(".lastdb-signed-restore-{}", uuid::Uuid::new_v4()));
    let seed = Zeroizing::new(lastdb_node::backup_access::seed_from_phrase(phrase)?);
    let mut fresh = FreshHome::preflight_empty(&staged_path)?.create()?;
    fresh.write(IDENTITY_KEY_FILE, seed.as_ref())?;
    fresh.commit()?;
    drop(fresh);
    let staged = StagedHome::open(staged_path)?;
    let host = Arc::new(Host::boot(&staged.path).await?);
    let result = lastdb_node::log_sync::recover_owned(
        &host,
        connection_home,
        with_gen(scope, args.generation),
        &database,
        "recovered",
    )
    .await;
    let owned_closed = host.shutdown_owned_databases().await;
    let owner_closed = host.db.shutdown().await;
    drop(host);
    let through = result?;
    owned_closed?;
    owner_closed.map_err(|error| error.to_string())?;
    staged.publish(&destination)?;
    print_backup(
        &serde_json::json!({
            "ok": true,
            "database_id": database,
            "name": "recovered",
            "through_delivery_seq": through,
            "destination": destination,
        }),
        args.json,
    )
}

struct StagedHome {
    path: PathBuf,
    inode: (u64, u64),
}

impl StagedHome {
    fn open(path: PathBuf) -> Result<Self, String> {
        let metadata = fs::symlink_metadata(&path).map_err(|error| error.to_string())?;
        if !metadata.is_dir() {
            return Err("the staged recovery home changed".into());
        }
        Ok(Self {
            path,
            inode: (metadata.dev(), metadata.ino()),
        })
    }

    fn owns_path(&self) -> bool {
        fs::symlink_metadata(&self.path)
            .is_ok_and(|meta| meta.is_dir() && (meta.dev(), meta.ino()) == self.inode)
    }

    fn publish(&self, destination: &Path) -> Result<(), String> {
        if !self.owns_path() {
            return Err("the staged recovery home changed".into());
        }
        File::open(&self.path)
            .and_then(|file| file.sync_all())
            .map_err(|error| format!("cannot sync the staged recovery home: {error}"))?;
        rustix::fs::renameat_with(
            rustix::fs::CWD,
            &self.path,
            rustix::fs::CWD,
            destination,
            rustix::fs::RenameFlags::NOREPLACE,
        )
        .map_err(|error| format!("cannot publish the recovered home: {error}"))?;
        File::open(destination.parent().ok_or("missing recovery parent")?)
            .and_then(|file| file.sync_all())
            .map_err(|error| format!("cannot sync the recovered home parent: {error}"))
    }
}

impl Drop for StagedHome {
    fn drop(&mut self) {
        if self.owns_path() {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}
