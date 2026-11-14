//! Replace a stopped personal store with a verified, fully replayed stage.

use std::fs;
use std::path::Path;

use cloud_backup_protocol::v2::{context::ContextReadRequest, Version};
use cloud_backup_protocol::{CloudScope, Digest, Id};
use fold_db::security::Ed25519KeyPair;
use fold_db::storage::disk_backup::{
    accepted::{UploadIdentity, UploadPosition},
    stopped_sync::SyncBoundary,
};
use mutation_log::MutationLog;
use serde::{Deserialize, Serialize};

use crate::host::{owned, Host};

use super::super::{cloud::CloudLog, disk, session, wire::NativeCut};
#[path = "install_fs.rs"]
mod files;

use files::{
    checked_child, directory, durable_file, exchange, file_inode, inode, io_error, lock_home,
    open_stopped, seed, storage_error, sync_dir, valid_stage_name, Inode, Stage,
};

const JOURNAL: &str = ".native-sync-recovery.json";
const LIMIT: usize = 8192;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Origin {
    database_id: Id,
    scope: CloudScope,
    environment: String,
    device_id: String,
    principal: String,
    cursor: u64,
    data: Inode,
    boundary: SyncBoundary,
}

impl Origin {
    fn upload(&self) -> UploadIdentity {
        UploadIdentity {
            scope: self.scope.clone(),
            environment: self.environment.clone(),
            database_id: self.database_id.as_str().to_owned(),
            device_id: self.device_id.clone(),
            writer_key: self.principal.clone(),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Journal {
    version: u8,
    origin: Origin,
    stage_name: String,
    stage_home: Inode,
    stage_data: Inode,
    stage_boundary: SyncBoundary,
    snapshot_id: Digest,
    snapshot_through: u64,
    through: u64,
}

pub(super) async fn freeze(host: &Host, log: &CloudLog, cursor: u64) -> Result<Origin, String> {
    let owner = host.owned.owner().map_err(|error| error.status().1)?;
    let database_id = Id::try_from(log.database_id().to_owned()).map_err(|e| e.to_string())?;
    let environment = saved_environment(&host.home, &log.scope)?;
    if owner.kind != owned::Kind::Personal
        || owner.id != log.database_id()
        || log.scope.account_id.as_str() != host.user_hash
        || log.environment() != environment
        || cursor != session::load_cursor(&host.home, log.database_id())?
        || search_enabled()
    {
        return Err(
            "native recovery needs the active personal database and local download position".into(),
        );
    }
    let upload = UploadIdentity {
        scope: log.scope.clone(),
        environment: environment.clone(),
        database_id: log.database_id().to_owned(),
        device_id: log.device().as_str().to_owned(),
        writer_key: log.principal().as_str().to_owned(),
    };
    let runtime = host.db.db_ops().native_runtime();
    let status = runtime.accepted_status();
    if status.head == 0
        && runtime
            .accepted_upload_position(&upload)
            .map_err(|_| "could not read the local upload position")?
            .is_none()
    {
        let log = log.clone();
        let writer =
            tokio::task::spawn_blocking(move || log.writer_tip(log.principal(), &log.reader()))
                .await
                .map_err(|_| "native recovery writer check stopped")?
                .map_err(|_| "could not verify the native recovery writer's accepted entries")?;
        if writer != 0 {
            return Err(
                "native recovery cannot resume an unbound writer that already uploaded".into(),
            );
        }
        runtime
            .bind_empty_sync_writer(&upload)
            .await
            .map_err(|error| storage_error(&error))?;
    }
    let origin = Origin {
        database_id,
        scope: log.scope.clone(),
        environment,
        device_id: log.device().as_str().to_owned(),
        principal: log.principal().as_str().to_owned(),
        cursor,
        data: inode(&host.home.join("data")).map_err(|error| io_error(&error))?,
        boundary: runtime
            .sync_boundary(&upload)
            .await
            .map_err(|error| storage_error(&error))?,
    };
    identity_matches(&host.home, &origin)?;
    Ok(origin)
}

pub(super) fn stage_home(host: &Host) -> Result<Stage, String> {
    Stage::create(host)
}

pub(super) fn bind_stage_upload(staged: &Host, origin: &Origin) -> Result<(), String> {
    let runtime = staged.db.db_ops().native_runtime();
    let status = runtime.accepted_status();
    if status.head != 0 || status.acked != 0 || status.gap || status.snapshot_required {
        return Err("the native recovery stage has unexpected local writes".into());
    }
    runtime
        .confirm_accepted_upload(
            &origin.upload(),
            0,
            UploadPosition {
                writer_seq: origin.boundary.writer_sequence(),
                spool_through: 0,
            },
        )
        .map_err(|_| "could not bind the staged upload position".to_owned())
}

pub(super) async fn prepare(
    host: &Host,
    log: &CloudLog,
    origin: Origin,
    mut stage: Stage,
    cut: &NativeCut,
    through: u64,
) -> Result<(), String> {
    let journal_lock = lock_home(&host.home)?;
    if prepared_native_recovery(&host.home)?
        || cut.snapshot_through.get() <= origin.cursor
        || through < cut.snapshot_through.get()
        || inode(&stage.path).map_err(|error| io_error(&error))? != stage.inode
        || stage.path.parent() != Some(host.home.as_path())
        || session::load_cursor(stage.path(), origin.database_id.as_str())? != through
    {
        return Err("native recovery stage does not cover the selected database position".into());
    }
    let checked = freeze(host, log, origin.cursor).await?;
    if origin.data != checked.data
        || origin.boundary != checked.boundary
        || origin.environment != checked.environment
        || origin.scope != checked.scope
    {
        return Err("local writes or cloud scope changed while preparing recovery".into());
    }
    let data = stage.path.join("data");
    let stage_data = inode(&data).map_err(|error| io_error(&error))?;
    if !stage_data.same_device(origin.data) {
        return Err("native recovery needs both data directories on one filesystem".into());
    }
    let stage_lock = open_stopped(&data, &*seed(&stage.path)?, &origin.upload())?;
    if stage_lock.boundary.writer_sequence() != origin.boundary.writer_sequence() {
        return Err("the staged upload position changed before recovery was prepared".into());
    }
    let journal = Journal {
        version: 1,
        origin,
        stage_name: stage
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or("invalid stage name")?
            .to_owned(),
        stage_home: stage.inode,
        stage_data,
        stage_boundary: stage_lock.boundary.clone(),
        snapshot_id: cut.native_snapshot_id.clone(),
        snapshot_through: cut.snapshot_through.get(),
        through,
    };
    stage.remove_identity()?;
    sync_dir(&stage.path)?;
    let bytes =
        serde_json::to_vec(&journal).map_err(|_| "could not encode native recovery journal")?;
    stage.keep = true;
    let result = durable_file(&host.home.join(JOURNAL), &bytes);
    drop((stage_lock, journal_lock));
    result
}

/// Check for a staged recovery that still needs a stopped-home activation.
pub fn prepared_native_recovery(home: &Path) -> Result<bool, String> {
    disk::read_limited(&home.join(JOURNAL), LIMIT)
        .map(|value| value.is_some())
        .map_err(|error| io_error(&error))
}

/// Activate a verified stage while the daemon is closed. Returns false if no
/// stage exists or a still-unexchanged preparation was invalidated.
pub async fn install_prepared_native_recovery(home: &Path) -> Result<bool, String> {
    if !prepared_native_recovery(home)? {
        return Ok(false);
    }
    let _lock = lock_home(home)?;
    let Some(bytes) =
        disk::read_limited(&home.join(JOURNAL), LIMIT).map_err(|error| io_error(&error))?
    else {
        return Ok(false);
    };
    let journal: Journal =
        serde_json::from_slice(&bytes).map_err(|_| "invalid native recovery journal")?;
    let stage_home = home.join(valid_stage_name(&journal.stage_name)?);
    let data = home.join("data");
    let staged = stage_home.join("data");
    if journal.version != 1
        || inode(&stage_home).map_err(|error| io_error(&error))? != journal.stage_home
    {
        return Err("native recovery stage identity changed".into());
    }
    let order = (
        inode(&data).map_err(|error| io_error(&error))?,
        inode(&staged).map_err(|error| io_error(&error))?,
    );
    let prior = (journal.origin.data, journal.stage_data);
    if order == (prior.1, prior.0) {
        let seed = seed(home)?;
        let installed = open_stopped(&data, &seed, &journal.origin.upload())?;
        if installed.boundary != journal.stage_boundary {
            return Err("installed native recovery stage changed before local completion".into());
        }
        complete(home, &stage_home, &journal)?;
        return Ok(true);
    }
    if order != prior || !prior.0.same_device(prior.1) {
        return Err("native recovery cannot identify both data directories".into());
    }
    if identity_matches(home, &journal.origin).is_err()
        || search_enabled()
        || session::load_cursor(home, journal.origin.database_id.as_str())? != journal.origin.cursor
    {
        stale(home, &stage_home)?;
        return Ok(false);
    }
    let Ok(client) = crate::cloud::client_from_home(home) else {
        stale(home, &stage_home)?;
        return Ok(false);
    };
    let Ok(current) = client
        .context(&ContextReadRequest {
            version: Version::V2,
            account_id: journal.origin.scope.account_id.clone(),
            required_destinations: Vec::new(),
        })
        .await
    else {
        stale(home, &stage_home)?;
        return Ok(false);
    };
    if current.scope != journal.origin.scope || identity_matches(home, &journal.origin).is_err() {
        stale(home, &stage_home)?;
        return Ok(false);
    }
    let seed = seed(home)?;
    let (Ok(source), Ok(candidate)) = (
        open_stopped(&data, &seed, &journal.origin.upload()),
        open_stopped(&staged, &seed, &journal.origin.upload()),
    ) else {
        stale(home, &stage_home)?;
        return Ok(false);
    };
    if source.boundary != journal.origin.boundary || candidate.boundary != journal.stage_boundary {
        stale(home, &stage_home)?;
        return Ok(false);
    }
    let home_fd = directory(home)?;
    let stage_fd = directory(&stage_home)?;
    if file_inode(&stage_fd)? != journal.stage_home
        || checked_child(&home_fd, "data")? != prior.0
        || checked_child(&stage_fd, "data")? != prior.1
    {
        return Err("native recovery data directories changed while locking them".into());
    }
    if let Err(error) = exchange(&home_fd, &stage_fd) {
        if checked_child(&home_fd, "data")? != prior.0
            || checked_child(&stage_fd, "data")? != prior.1
        {
            return Err(error);
        }
        tracing::warn!(target: "lastdbd::log_sync", %error, "the filesystem refused native recovery exchange");
        stale(home, &stage_home)?;
        return Ok(false);
    }
    complete(home, &stage_home, &journal)?;
    drop((source, candidate));
    Ok(true)
}

fn complete(home: &Path, stage: &Path, journal: &Journal) -> Result<(), String> {
    sync_dir(stage)?;
    sync_dir(home)?;
    let database = journal.origin.database_id.as_str();
    let scope = home.join("log-sync").join(database);
    let cursor = serde_json::json!({ "cursor": journal.through });
    let cursor =
        serde_json::to_vec(&cursor).map_err(|_| "could not encode native recovery cursor")?;
    durable_file(&scope.join("session.json"), &cursor)?;
    let prefix = serde_json::json!({ "database_id": database, "through": journal.through });
    let prefix =
        serde_json::to_vec(&prefix).map_err(|_| "could not encode native recovery prefix")?;
    durable_file(&scope.join("prefix.json"), &prefix)?;
    let receipt = serde_json::json!({
        "version": 1, "database_id": journal.origin.database_id,
        "device_id": journal.origin.device_id, "snapshot_id": journal.snapshot_id,
        "snapshot_through": journal.snapshot_through, "through": journal.through,
    });
    let bytes =
        serde_json::to_vec(&receipt).map_err(|_| "could not encode native recovery receipt")?;
    durable_file(&scope.join("native-recovery.json"), &bytes)?;
    // The retained stage now holds the old native files and the original bound journal.
    retire(home, stage, "installed.json")
}

fn stale(home: &Path, stage: &Path) -> Result<(), String> {
    retire(home, stage, "stale.json")?;
    if let Err(error) = fs::remove_dir_all(stage) {
        tracing::warn!(target: "lastdbd::log_sync", %error, "could not remove the unused native recovery stage");
    }
    sync_dir(home)
}

fn retire(home: &Path, stage: &Path, name: &str) -> Result<(), String> {
    fs::rename(home.join(JOURNAL), stage.join(name)).map_err(|error| io_error(&error))?;
    sync_dir(stage)?;
    sync_dir(home)
}

fn identity_matches(home: &Path, origin: &Origin) -> Result<(), String> {
    let pair = Ed25519KeyPair::from_secret_key(seed(home)?.as_ref())
        .map_err(|_| "invalid native recovery identity")?;
    let device = disk::read_limited(&home.join("log-sync").join("device-id"), 128)
        .map_err(|error| io_error(&error))?
        .ok_or("missing original sync device")?;
    let owner = super::super::registry::databases(home).into_iter().next();
    if hex::encode(pair.public_key_bytes()) != origin.principal
        || std::str::from_utf8(&device).ok().map(str::trim) != Some(origin.device_id.as_str())
        || saved_environment(home, &origin.scope)? != origin.environment
        || !owner.is_some_and(|db| {
            !db.native && db.kind == "personal" && db.id == origin.database_id.as_str()
        })
    {
        return Err("identity or database changed since native recovery was prepared".into());
    }
    Ok(())
}

fn saved_environment(home: &Path, scope: &CloudScope) -> Result<String, String> {
    let connection = crate::cloud::load_saved_credentials(home)?
        .and_then(|saved| saved.connection)
        .ok_or("native recovery needs a saved cloud connection")?;
    let selected = super::super::http::selected_service(home)?;
    if connection.account_id != scope.account_id || connection.environment != selected.label {
        return Err("native recovery needs the selected cloud account".into());
    }
    Ok(connection.environment)
}

fn search_enabled() -> bool {
    fold_db::db_operations::search_index::queue_policy::enabled()
}
