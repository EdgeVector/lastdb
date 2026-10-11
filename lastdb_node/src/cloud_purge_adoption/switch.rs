//! A peer switches only after a clean stop and a durable candidate proof.

use super::*;
use fold_db::sync::auth::{AuthClient, SyncAuth};
use std::fs::{self, File};

mod candidate;
mod files;
mod proof;

pub(super) async fn before_boot(home: &Path) -> Result<(), String> {
    let path = pending_path(home).map_err(message)?;
    if !path.exists() {
        return Ok(());
    }
    let mut pending: Pending = crate::cloud_purge_runtime::read(&path)?;
    if pending.format_version != 1 {
        return Err("unsupported cloud adoption journal".into());
    }
    if pending.phase == "committed" {
        proof::load(home, &pending)?;
        return Ok(());
    }
    if !ready_to_switch(home, &pending).await? {
        return Ok(());
    }
    let peer = CloudPurgePeer::open(
        CloudPurgePeer::directory_for_home(home).map_err(message)?,
        pending.plan.source_id.clone(),
    )
    .map_err(message)?;
    peer.bind_local_account(&pending.plan.account)
        .map_err(message)?;
    let retained = retain(home, &path, &peer, &mut pending)?;
    if pending.phase == "staged" {
        let sidecars = files::retain_sidecars(home, &retained.directory)?;
        let (candidate, queue_digest) = candidate::prepare(home, &pending, &retained).await?;
        files::copy_device_id(home, &candidate)?;
        let digest = proof::prepare(
            home,
            &pending,
            &retained,
            &candidate,
            sidecars,
            queue_digest,
        )?;
        pending.staged_home = candidate;
        pending.switch_proof_sha256 = Some(digest);
        pending.phase = "prepared".into();
        crate::cloud_purge_runtime::atomic(&path, &pending)?;
    }
    if pending.phase == "prepared" {
        let proof = proof::load(home, &pending)?;
        files::switch_data(home, &pending.staged_home, &retained.directory, &proof)?;
        pending.phase = "switched".into();
        crate::cloud_purge_runtime::atomic(&path, &pending)?;
    }
    commit(home, &path, &peer, &mut pending, retained)
}

async fn ready_to_switch(home: &Path, pending: &Pending) -> Result<bool, String> {
    let untouched = pending.phase == "staged" && pending.retained.is_none();
    let config = crate::cloud_purge_job::read_cloud_config(home)?;
    let auth = AuthClient::new(
        Arc::new(reqwest::Client::new()),
        config.api_url,
        SyncAuth::ApiKey(config.api_key),
    )
    .without_db_auto_claim();
    let current = match auth.cloud_control_get(&pending.plan.account).await {
        Ok(current) => current,
        Err(_) if untouched => return Ok(false),
        Err(_) => {
            return Err(
                "cloud adoption needs a current cloud control before the directory switch".into(),
            );
        }
    };
    let effective = fold_db::sync::cloud_purge_peer::effective_adoption_control(
        current
            .control
            .as_ref()
            .ok_or("cloud adoption control disappeared")?,
        &pending.plan,
    )
    .map_err(message)?;
    require_control(&pending.plan, &effective).map_err(message)?;
    let flush = match crate::session_ledger::read_shutdown_flush_receipt(home) {
        Ok(flush) => flush,
        Err(_) if untouched => return Ok(false),
        Err(_) => return Err("cloud adoption needs the prior node's clean flush receipt".into()),
    };
    if flush.pid != pending.prior_pid || flush.start_ts != pending.prior_start_ts || !flush.flush_ok
    {
        if untouched {
            return Ok(false);
        }
        return Err("cloud adoption flush proof belongs to another node session".into());
    }
    let prior_pid =
        i32::try_from(pending.prior_pid).map_err(|_| "the prior cloud adoption PID is invalid")?;
    if unsafe { libc::kill(prior_pid, 0) } == 0 {
        return Err("the prior cloud adoption node is still active".into());
    }
    if std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH) {
        return Err("the prior cloud adoption node has no verified stop".into());
    }
    Ok(true)
}

fn retain(
    home: &Path,
    path: &Path,
    peer: &CloudPurgePeer,
    pending: &mut Pending,
) -> Result<fold_db::sync::cloud_purge_peer::RetainedHome, String> {
    if let Some(retained) = &pending.retained {
        peer.verify_retained_files(&pending.plan, retained)
            .map_err(message)?;
        return Ok(retained.clone());
    }
    let retained = peer
        .retain_directory(&pending.plan, &home.join("data"))
        .map_err(message)?;
    pending.retained = Some(retained.clone());
    crate::cloud_purge_runtime::atomic(path, pending)?;
    Ok(retained)
}

fn commit(
    home: &Path,
    path: &Path,
    peer: &CloudPurgePeer,
    pending: &mut Pending,
    retained: fold_db::sync::cloud_purge_peer::RetainedHome,
) -> Result<(), String> {
    if pending.phase != "switched" {
        return Err("cloud adoption switch is incomplete".into());
    }
    peer.verify_retained_files(&pending.plan, &retained)
        .map_err(message)?;
    let proof = proof::load(home, pending)?;
    proof::verify_installed(home, &proof)?;
    let receipt = AdoptionReceipt {
        account: pending.plan.account.clone(),
        operation_id: pending.plan.operation_id.clone(),
        generation: pending.plan.generation,
        source_id: pending.plan.source_id.clone(),
        retained_directory: retained.directory,
        retention_sha256: retained.receipt_sha256,
        installed_manifest_sha256: pending.plan.replacement_manifest_sha256.clone(),
        switch_receipt_sha256: pending
            .switch_proof_sha256
            .clone()
            .ok_or("cloud adoption switch proof absent")?,
    };
    peer.mark_local_adopted(&pending.plan, receipt.clone())
        .map_err(message)?;
    pending.receipt = Some(receipt);
    pending.phase = "committed".into();
    crate::cloud_purge_runtime::atomic(path, pending)
}

fn message(error: impl std::fmt::Display) -> String {
    error.to_string()
}
