//! Actual clean-stop, unchanged credentials/identity, and owner evidence gates.

use super::{err, historical_claim, io, model, NormalSnapshotArgs};
use crate::home::resolve_laststore_root;
use crate::reap::guard::{self, Flags};
use fold_db::storage::config::CloudSyncConfig;
use fold_db::storage::laststore::{
    high_water_path_for_store_root, manifest_sha256_hex, BackupManifest,
};
use lastdb_node::session_ledger::{self, Ledger, SessionRecord};
use std::path::PathBuf;

pub(super) struct Inputs {
    pub home: PathBuf,
    pub store_root: PathBuf,
    pub cloud: CloudSyncConfig,
    pub identity_sha256: String,
    pub device_file_sha256: String,
    pub device_id: String,
    pub previous: BackupManifest,
    pub previous_cache_sha256: String,
    pub historical_unproved_flush_claim_sha256: Option<String>,
    pub db_hash: String,
    pub operator: model::OperatorEvidence,
}

pub(super) fn stopped(args: &NormalSnapshotArgs) -> Result<(), String> {
    let root = resolve_laststore_root(&args.home)?;
    let sockets = guard::socket_paths(&args.home, &root);
    let view = guard::live_process_view(&sockets);
    let report = guard::prove(
        &args.home,
        &root,
        Flags {
            stopped_primary: args.stopped_primary,
            i_know_this_is_primary: args.i_know_this_is_primary,
        },
        &view,
    )
    .map_err(err)?;
    if !report.primary_path || !args.stopped_primary || !args.i_know_this_is_primary {
        return Err("normal offline snapshot requires an explicitly stopped primary".into());
    }
    clean_session(args)?;
    forbidden_states(&args.home)?;
    let _ = historical_claim::check(args)?;
    Ok(())
}

fn clean_session(args: &NormalSnapshotArgs) -> Result<(), String> {
    let receipt_path = session_ledger::shutdown_flush_receipt_path(&args.home);
    io::read(&receipt_path, 4096, false)?;
    let receipt = session_ledger::read_shutdown_flush_receipt(&args.home).map_err(err)?;
    let record = session_ledger::read_last_record(&args.home).ok_or("no latest stopped session")?;
    if receipt.pid != args.expected_pid
        || receipt.start_ts != args.expected_start_ts
        || record.pid != receipt.pid
        || record.start_ts != receipt.start_ts
        || record.build_version != args.expected_build_version
        || !record.ended_clean()
        || record.end_ts.is_none_or(|end| end < record.start_ts)
    {
        return Err("the clean stop does not identify the expected daemon session".into());
    }
    // The bounded production reader can skip malformed lines. Require its
    // chosen record to be the actual final nonblank ledger line too.
    check_last_ledger_line(&args.home, &record)?;
    io::absent(&Ledger::current_session_path(&args.home))?;
    if args.expected_pid == 0
        || unsafe { libc::kill(args.expected_pid as libc::pid_t, 0) } == 0
        || std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
    {
        return Err("the stopped daemon PID is present or cannot be checked".into());
    }
    Ok(())
}

fn check_last_ledger_line(home: &std::path::Path, record: &SessionRecord) -> Result<(), String> {
    use std::io::{Read, Seek, SeekFrom};
    let path = Ledger::ledger_path(home);
    let meta = std::fs::symlink_metadata(&path).map_err(err)?;
    if !meta.is_file() {
        return Err("the session ledger is not a regular file".into());
    }
    let mut file = std::fs::File::open(path).map_err(err)?;
    let start = meta.len().saturating_sub(65536);
    file.seek(SeekFrom::Start(start)).map_err(err)?;
    let mut bytes = Vec::new();
    file.take(65537).read_to_end(&mut bytes).map_err(err)?;
    if bytes.len() > 65536 {
        return Err("the session ledger changed during its bounded read".into());
    }
    let text = std::str::from_utf8(&bytes).map_err(err)?;
    let text = if start > 0 {
        text.split_once('\n')
            .ok_or("no complete final session line")?
            .1
    } else {
        text
    };
    let latest: SessionRecord = serde_json::from_str(
        text.lines()
            .rfind(|line| !line.trim().is_empty())
            .ok_or("empty session ledger")?,
    )
    .map_err(err)?;
    if &latest != record {
        return Err("the latest session reader skipped the actual final line".into());
    }
    Ok(())
}

fn forbidden_states(home: &std::path::Path) -> Result<(), String> {
    use lastdb_node::cloud;
    for name in [
        cloud::CLOUD_SYNC_PAUSED_FILE,
        cloud::CLOUD_RESUME_REQUIRED_FILE,
        cloud::CLOUD_RESUME_REQUESTED_FILE,
        cloud::CLOUD_RESUME_READY_FILE,
        cloud::CLOUD_BACKUP_SOURCE_COPY_FILE,
    ] {
        io::absent(&home.join(name))?;
    }
    // The inert constructor normally cleans legacy freeze generations. This
    // mode refuses them before construction rather than removing old copies.
    let path = home.join("backup-cut-freeze");
    match std::fs::read_dir(&path) {
        Ok(entries) => {
            for (count, entry) in entries.enumerate() {
                let entry = entry.map_err(err)?;
                if count >= 1000
                    || entry
                        .file_name()
                        .to_str()
                        .is_some_and(|name| name.parse::<u64>().is_ok())
                {
                    return Err(
                        "legacy backup freeze generations require separate maintenance".into(),
                    );
                }
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(err(error)),
    }
    Ok(())
}

pub(super) fn load(args: &NormalSnapshotArgs) -> Result<Inputs, String> {
    let home = std::fs::canonicalize(&args.home).map_err(err)?;
    let store_root = std::fs::canonicalize(resolve_laststore_root(&home)?).map_err(err)?;
    if store_root != home.join("data") {
        return Err("normal primary snapshot requires the Mini home/data layout".into());
    }
    let cloud_bytes = io::bound(
        &home.join(lastdb_node::host::CLOUD_SYNC_CONFIG_FILE),
        &args.cloud_config_sha256,
        65536,
    )?;
    let cloud: CloudSyncConfig = serde_json::from_slice(&cloud_bytes).map_err(err)?;
    if cloud.api_key.trim().is_empty() || cloud.api_url.trim().is_empty() {
        return Err("normal snapshot requires the existing active cloud credential".into());
    }
    let identity = io::read(&home.join("identity.key"), 32, true)?;
    if identity.len() != 32 {
        return Err("normal snapshot requires the existing 32-byte identity".into());
    }
    let device = io::read(&store_root.join(".device_id"), 4096, false)?;
    let persisted = std::str::from_utf8(&device).map_err(err)?.trim();
    if persisted.is_empty() {
        return Err("normal snapshot cannot create a missing device identity".into());
    }
    if std::env::var("FOLD_SYNC_DEVICE_ID")
        .ok()
        .filter(|id| !id.is_empty())
        .is_some_and(|id| id != persisted)
    {
        return Err("the device override differs from the persisted primary identity".into());
    }
    let device_id = persisted.to_owned();
    let cache = io::read(
        &lastdb_node::host::backup_manifest_cache_path(&home),
        16 * 1024 * 1024,
        false,
    )?;
    let previous: BackupManifest = serde_json::from_slice(&cache).map_err(err)?;
    io::check_digest(&args.previous_manifest_sha256)?;
    if previous.version != 1
        || previous.counter == 0
        || manifest_sha256_hex(&previous).map_err(err)? != args.previous_manifest_sha256
    {
        return Err("normal snapshot requires the exact committed v1 predecessor".into());
    }
    let db_hash = high_water(&store_root, &previous)?;
    let operator = operator(args, &home)?;
    Ok(Inputs {
        home,
        store_root,
        cloud,
        identity_sha256: io::digest(&identity),
        device_file_sha256: io::digest(&device),
        device_id,
        previous,
        previous_cache_sha256: io::digest(&cache),
        historical_unproved_flush_claim_sha256: historical_claim::check(args)?,
        db_hash,
        operator,
    })
}

fn high_water(root: &std::path::Path, previous: &BackupManifest) -> Result<String, String> {
    let bytes = io::read(&high_water_path_for_store_root(root), 65536, false)?;
    let state: serde_json::Value = serde_json::from_slice(&bytes).map_err(err)?;
    if state.get("version").and_then(serde_json::Value::as_u64) != Some(1)
        || state.get("store_uuid").and_then(serde_json::Value::as_str)
            != Some(previous.store_uuid.as_str())
        || state
            .get("backup_epoch")
            .and_then(serde_json::Value::as_u64)
            != Some(previous.epoch)
        || state
            .get("backup_manifest_counter")
            .and_then(serde_json::Value::as_u64)
            != Some(previous.counter)
    {
        return Err("the durable backup identity differs from the predecessor".into());
    }
    fold_db::storage::laststore::read_cloud_db_hash(root)
        .ok_or("no durable database cloud identity".into())
}

fn operator(
    args: &NormalSnapshotArgs,
    home: &std::path::Path,
) -> Result<model::OperatorEvidence, String> {
    let bytes = io::bound(
        &args.operator_evidence_file,
        &args.operator_evidence_sha256,
        1024 * 1024,
    )?;
    let evidence: model::OperatorEvidence = serde_json::from_slice(&bytes).map_err(err)?;
    if evidence.version != 1
        || evidence.home != home
        || evidence.pid != args.expected_pid
        || evidence.start_ts != args.expected_start_ts
        || !evidence.local_writers_paused
        || !evidence.other_local_clients_absent
        || !evidence.supervisor_unloaded
        || !evidence.rollback_preserved
        || evidence.controls.is_empty()
        || evidence.controls.len() > 64
    {
        return Err("the owner evidence does not bind the stopped controls and rollback".into());
    }
    for artifact in evidence
        .controls
        .iter()
        .chain([&evidence.stopped_receipt, &evidence.rollback_receipt])
    {
        if !artifact.path.is_absolute() || artifact.path.starts_with(home) {
            return Err("operator receipts must be explicit private files outside the home".into());
        }
        io::bound(&artifact.path, &artifact.sha256, 4 * 1024 * 1024)?;
    }
    Ok(evidence)
}

pub(super) fn unchanged(
    args: &NormalSnapshotArgs,
    inputs: &Inputs,
    before_publish: bool,
) -> Result<(), String> {
    stopped(args)?;
    if historical_claim::check(args)? != inputs.historical_unproved_flush_claim_sha256 {
        return Err("the historical unproved flush claim changed during publication".into());
    }
    io::bound(
        &inputs.home.join(lastdb_node::host::CLOUD_SYNC_CONFIG_FILE),
        &args.cloud_config_sha256,
        65536,
    )?;
    io::bound(
        &inputs.home.join("identity.key"),
        &inputs.identity_sha256,
        32,
    )?;
    if io::digest(&io::read(
        &inputs.store_root.join(".device_id"),
        4096,
        false,
    )?) != inputs.device_file_sha256
        || operator(args, &inputs.home)? != inputs.operator
    {
        return Err("normal snapshot identity or held operator evidence changed".into());
    }
    if before_publish
        && io::digest(&io::read(
            &lastdb_node::host::backup_manifest_cache_path(&inputs.home),
            16 * 1024 * 1024,
            false,
        )?) != inputs.previous_cache_sha256
    {
        return Err("the normal predecessor cache changed before publication".into());
    }
    Ok(())
}
