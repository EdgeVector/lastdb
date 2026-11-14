//! Offline signed cloud backup restore orchestration (`lastdb backup restore`).

use super::snapshot_wait::print_backup;
use super::{block_on_cloud, read_stdin_secret, BackupRestoreArgs, FreshHome};
use cloud_backup_protocol::v2::{context::ContextReadRequest, Version};
use cloud_backup_protocol::Id;
use lastdb_node::host::{Host, IDENTITY_KEY_FILE};
use rustix::fs;
use std::fs::File;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use zeroize::Zeroizing;

pub(super) fn run(home: &Path, args: BackupRestoreArgs) -> Result<(), String> {
    let phrase = read_stdin_secret(4096, "recovery phrase")?;
    let database = Id::try_from(args.database)
        .map_err(|_| "signed restore requires a canonical database id".to_owned())?;
    let destination = args.into;
    let parent = destination.parent().ok_or("missing recovery parent")?;
    FreshHome::preflight_empty(&destination)?;
    let restored = signed_restore_worker(|| async {
        let account_id = lastdb_node::cloud::account_from_phrase(&phrase)?;
        let mut scope = lastdb_node::cloud::client_from_home(home)?
            .context(&ContextReadRequest {
                version: Version::V2,
                account_id,
                required_destinations: Vec::new(),
            })
            .await
            .map_err(|e| e.to_string())?
            .scope;
        if let Some(generation) = args.generation {
            scope.account_generation = cloud_backup_protocol::DecimalU64::new(generation);
        }
        let staged_path = parent.join(format!(".lastdb-signed-restore-{}", uuid::Uuid::new_v4()));
        let seed = Zeroizing::new(lastdb_node::cloud::seed_from_phrase(&phrase)?);
        let mut fresh = FreshHome::preflight_empty(&staged_path)?.create()?;
        fresh.write(IDENTITY_KEY_FILE, seed.as_ref())?;
        fresh.commit()?;
        drop(fresh);
        let staged = StagedHome::open(staged_path)?;
        let base = lastdb_node::log_sync::restore_snapshot_base(
            &staged.path,
            home,
            &scope,
            &database,
            &seed,
        )
        .await?;
        let host = Arc::new(if base.is_some() {
            Host::boot_for_snapshot_restore(&staged.path).await?
        } else {
            Host::boot_for_signed_restore(&staged.path).await?
        });
        let res =
            lastdb_node::log_sync::recover_owned(&host, home, scope, &database, "recovered", base)
                .await;
        let owned_closed = host.shutdown_owned_databases().await;
        let owner_closed = host.db.shutdown().await;
        drop(host);
        let out = res?;
        owned_closed?;
        owner_closed.map_err(|e| e.to_string())?;
        staged.publish(&destination).map(|()| out)
    })?;
    print_backup(
        &serde_json::json!({
            "ok": true, "database_id": database, "name": restored.name,
            "through_delivery_seq": restored.through,
            "snapshot_through": restored.snapshot_through, "replayed": restored.replayed,
            "destination": destination,
        }),
        args.json,
    )
}

// Replay uses the same native write path as the daemon. Run it off the
// process entry thread with the same stack allowance as a daemon worker.
fn signed_restore_worker<T: Send, F>(make_future: impl FnOnce() -> F + Send) -> Result<T, String>
where
    F: std::future::Future<Output = Result<T, String>>,
{
    let stack = fold_db::env_flag::var_or("RUST_MIN_STACK", 0usize).max(64 * 1024 * 1024);
    std::thread::scope(|scope| {
        std::thread::Builder::new()
            .name("lastdb-signed-restore".to_owned())
            .stack_size(stack)
            .spawn_scoped(scope, move || {
                block_on_cloud("signed restore", make_future())
            })
            .map_err(|_| "cannot start signed restore worker".to_owned())?
            .join()
            .map_err(|_| "signed restore worker stopped".to_owned())?
    })
}

struct StagedHome {
    path: PathBuf,
    inode: (u64, u64),
}

impl StagedHome {
    fn open(path: PathBuf) -> Result<Self, String> {
        let m = std::fs::symlink_metadata(&path).map_err(|e| e.to_string())?;
        if !m.is_dir() {
            return Err("the staged recovery home changed".into());
        }
        Ok(Self {
            path,
            inode: (m.dev(), m.ino()),
        })
    }

    fn owns_path(&self) -> bool {
        std::fs::symlink_metadata(&self.path)
            .is_ok_and(|m| m.is_dir() && (m.dev(), m.ino()) == self.inode)
    }

    fn publish(&self, destination: &Path) -> Result<(), String> {
        if !self.owns_path() {
            return Err("the staged recovery home changed".into());
        }
        File::open(&self.path)
            .and_then(|f| f.sync_all())
            .map_err(|e| format!("cannot sync the staged recovery home: {e}"))?;
        fs::renameat_with(
            fs::CWD,
            &self.path,
            fs::CWD,
            destination,
            fs::RenameFlags::NOREPLACE,
        )
        .map_err(|e| format!("cannot publish the recovered home: {e}"))?;
        File::open(destination.parent().ok_or("missing recovery parent")?)
            .and_then(|f| f.sync_all())
            .map_err(|e| format!("cannot sync the recovered home parent: {e}"))
    }
}

impl Drop for StagedHome {
    fn drop(&mut self) {
        if self.owns_path() {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}
