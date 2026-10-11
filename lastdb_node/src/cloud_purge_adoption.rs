//! Retain old local bytes before an exact cloud generation can publish.

use crate::host::Host;
use fold_db::sync::cloud_purge::protocol::{
    CloudAccountKind, CloudPurgeControl, CloudPurgePhase, CloudReplacementManifest,
};
use fold_db::sync::cloud_purge_peer::{AdoptionPlan, AdoptionReceipt, CloudPurgePeer};
use fold_db::sync::{SyncEngine, SyncError, SyncResult};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

mod continuation;
mod switch;

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Pending {
    pub format_version: u32,
    pub plan: AdoptionPlan,
    pub prior_pid: u32,
    pub prior_start_ts: u64,
    pub staged_home: PathBuf,
    pub phase: String,
    pub receipt: Option<AdoptionReceipt>,
    pub retained: Option<fold_db::sync::cloud_purge_peer::RetainedHome>,
    #[serde(default)]
    pub switch_proof_sha256: Option<String>,
}

pub(crate) fn pending_path(home: &Path) -> SyncResult<PathBuf> {
    Ok(CloudPurgePeer::directory_for_home(home)?.join("adoption-pending.json"))
}

pub(crate) fn install(
    host: &Arc<Host>,
    engine: &Arc<SyncEngine>,
    stop: Arc<AtomicBool>,
) -> SyncResult<()> {
    let host = Arc::downgrade(host);
    let engine = Arc::downgrade(engine);
    engine
        .upgrade()
        .ok_or_else(|| held("cloud engine closed"))?
        .set_cloud_purge_adoption_handler(Arc::new(move |plan| {
            let host = host.clone();
            let engine = engine.clone();
            let stop = Arc::clone(&stop);
            Box::pin(async move {
                let host = host
                    .upgrade()
                    .ok_or_else(|| held("cloud adoption host closed"))?;
                let engine = engine
                    .upgrade()
                    .ok_or_else(|| held("cloud adoption engine closed"))?;
                adopt(&host, &engine, plan, &stop).await
            })
        }))
}

async fn adopt(
    host: &Host,
    engine: &SyncEngine,
    plan: AdoptionPlan,
    stop: &AtomicBool,
) -> SyncResult<AdoptionReceipt> {
    let peer = engine
        .cloud_purge_peer()
        .ok_or_else(|| held("cloud peer receipt store absent"))?;
    let control = peer
        .known_control(&plan.account)?
        .ok_or_else(|| held("cloud replacement was not observed"))?;
    if plan.continuation.is_some() {
        return continuation::adopt(host, engine, &peer, &plan, &control).await;
    }
    let control = fold_db::sync::cloud_purge_peer::effective_adoption_control(&control, &plan)?;
    require_control(&plan, &control)?;
    if let Some(receipt) = try_selected_personal(host, engine, &peer, &plan, &control).await? {
        return Ok(receipt);
    }
    let path = pending_path(&host.home)?;
    if path.exists() {
        let pending: Pending = crate::cloud_purge_runtime::read(&path).map_err(held)?;
        if same_plan(&pending.plan, &plan) {
            if let Some(receipt) = pending.receipt {
                return Ok(receipt);
            }
        } else if pending.phase == "committed"
            && (pending.plan.account != plan.account || pending.plan.generation < plan.generation)
        {
            crate::cloud_purge_runtime::atomic(
                &path.with_file_name(format!(
                    "adoption-{}-{}-{}.json",
                    fold_db::hex::sha256_hex(&serde_json::to_vec(&pending.plan.account)?),
                    pending.plan.generation,
                    pending.plan.operation_id
                )),
                &pending,
            )
            .map_err(held)?;
        } else {
            return Err(held("another cloud adoption owns the inactive destination"));
        }
    }
    let replacement: CloudReplacementManifest =
        serde_json::from_value(plan.replacement_manifest.clone())?;
    let root = local_store_root(&replacement)?;
    let staged = CloudPurgePeer::directory_for_home(&host.home)?
        .join("inactive")
        .join(plan.generation.to_string())
        .join(&plan.operation_id);
    let boot = host
        .own_boot_identity
        .get()
        .ok_or_else(|| held("the peer has no exact boot identity"))?;
    let pending = Pending {
        format_version: 1,
        plan,
        prior_pid: boot.pid,
        prior_start_ts: boot.start_ts,
        staged_home: staged,
        phase: "staged".into(),
        receipt: None,
        retained: None,
        switch_proof_sha256: None,
    };
    // Leave room for the bounded local receipt and candidate proof fields that
    // appear after shutdown, before any download or stop request is issued.
    if serde_json::to_vec(&pending)?.len() > 63 * 1024 * 1024 {
        return Err(held(
            "the cloud adoption journal exceeds its local metadata budget",
        ));
    }
    engine
        .restore_cloud_purge_replacement(&control, root, &pending.staged_home)
        .await?;
    crate::cloud_purge_runtime::atomic(&path, &pending).map_err(held)?;
    stop.store(true, Ordering::Release);
    Err(held(
        "the peer needs its clean local restart to commit the replacement",
    ))
}

async fn try_selected_personal(
    host: &Host,
    engine: &SyncEngine,
    peer: &CloudPurgePeer,
    plan: &AdoptionPlan,
    control: &CloudPurgeControl,
) -> SyncResult<Option<AdoptionReceipt>> {
    let directory = crate::cloud_purge_job::directory(&host.home, &plan.operation_id)
        .map_err(held)?
        .join("source");
    if control.selected_writer.source_id != peer.source_id()
        || !directory.join("source.json").exists()
        || !crate::cloud_purge_runtime::has_write_fence(host, &plan.operation_id).map_err(held)?
    {
        return Ok(None);
    }
    if plan.account.kind == CloudAccountKind::Org {
        // Org export changes instance prefixes and can convert legacy keys.
        // Install that exact export through the stopped candidate switch.
        verify_selected_source(peer, plan, &directory)?;
        return Ok(None);
    }
    selected(host, engine, peer, plan, control, &directory)
        .await
        .map(Some)
}

pub(crate) fn selected_restart_pending(
    home: &Path,
    operation_id: &str,
    account: &fold_db::sync::cloud_purge::protocol::CloudAccount,
) -> SyncResult<bool> {
    let path = pending_path(home)?;
    if !path.exists() {
        return Ok(false);
    }
    let pending: Pending = crate::cloud_purge_runtime::read(&path).map_err(held)?;
    Ok(pending.format_version == 1
        && pending.plan.operation_id == operation_id
        && pending.plan.account == *account
        && matches!(pending.phase.as_str(), "staged" | "prepared" | "switched"))
}

async fn selected(
    host: &Host,
    engine: &SyncEngine,
    peer: &CloudPurgePeer,
    plan: &AdoptionPlan,
    control: &CloudPurgeControl,
    source_directory: &Path,
) -> SyncResult<AdoptionReceipt> {
    if !crate::cloud_purge_runtime::has_write_fence(host, &plan.operation_id).map_err(held)? {
        return Err(held(
            "the selected source has no retained local write fence",
        ));
    }
    let source = verify_selected_source(peer, plan, source_directory)?;
    let retained = peer.retain_directory(plan, source_directory)?;
    let prefix = if plan.account.kind == CloudAccountKind::Personal {
        ""
    } else {
        &plan.account.id
    };
    let detach = engine
        .detach_cloud_purge_queues(plan, &retained, prefix, &source.prepared.writer_frontier)
        .await?;
    let journal = serde_json::json!({"format_version":1,"phase":"committed","plan":plan,
        "source_manifest_sha256":source.prepared.manifest_sha256,"control_revision":control.revision,
        "queue_detach_sha256":detach.sha256()?});
    let path = peer
        .directory()
        .join(format!("selected-switch-{}.json", plan.operation_id));
    crate::cloud_purge_runtime::atomic(&path, &journal).map_err(held)?;
    Ok(AdoptionReceipt {
        account: plan.account.clone(),
        operation_id: plan.operation_id.clone(),
        generation: plan.generation,
        source_id: plan.source_id.clone(),
        retained_directory: retained.directory,
        retention_sha256: retained.receipt_sha256,
        installed_manifest_sha256: plan.replacement_manifest_sha256.clone(),
        switch_receipt_sha256: fold_db::hex::sha256_hex(&serde_json::to_vec(&journal)?),
    })
}

fn verify_selected_source(
    peer: &CloudPurgePeer,
    plan: &AdoptionPlan,
    source_directory: &Path,
) -> SyncResult<fold_db::sync::cloud_purge::RetainedSource> {
    let source = fold_db::sync::cloud_purge::RetainedSource::reopen(source_directory)?;
    let replacement: CloudReplacementManifest =
        serde_json::from_value(plan.replacement_manifest.clone())?;
    let expected_roots = fold_db::hex::sha256_hex(&serde_json::to_vec(&source.prepared.roots)?);
    let replacement_roots = fold_db::hex::sha256_hex(&serde_json::to_vec(&replacement.roots)?);
    if source.account != plan.account
        || source.prepared.source_id != peer.source_id()
        || replacement.source_manifest_sha256 != source.prepared.manifest_sha256
        || replacement_roots != expected_roots
    {
        return Err(held(
            "the ready cloud root differs from the selected retained source",
        ));
    }
    Ok(source)
}

pub async fn before_boot(home: &Path) -> Result<(), String> {
    switch::before_boot(home).await
}

pub(crate) fn require_control(plan: &AdoptionPlan, control: &CloudPurgeControl) -> SyncResult<()> {
    if control.phase != CloudPurgePhase::Ready
        || !control.deletion_is_complete()
        || control.account != plan.account
        || control.operation_id != plan.operation_id
        || control.generation != plan.generation
        || control
            .replacement_manifest
            .as_ref()
            .map(|value| serde_json::to_vec(value).map(|bytes| fold_db::hex::sha256_hex(&bytes)))
            .transpose()?
            .as_deref()
            != Some(&plan.replacement_manifest_sha256)
    {
        return Err(held(
            "cloud adoption differs from the current verified replacement",
        ));
    }
    Ok(())
}

fn same_plan(a: &AdoptionPlan, b: &AdoptionPlan) -> bool {
    a.account == b.account
        && a.operation_id == b.operation_id
        && a.generation == b.generation
        && a.source_id == b.source_id
        && a.replacement_manifest_sha256 == b.replacement_manifest_sha256
}

fn local_store_root(
    replacement: &CloudReplacementManifest,
) -> SyncResult<&fold_db::sync::cloud_purge::protocol::CloudReplacementRoot> {
    let mut roots = replacement
        .roots
        .iter()
        .filter(|root| root.format == "laststore_v1");
    let root = roots
        .next()
        .ok_or_else(|| held("local store replacement root absent"))?;
    if roots.next().is_some()
        || replacement
            .roots
            .iter()
            .any(|root| root.format != "laststore_v1" && root.format != "logical_snapshot_v1")
    {
        return Err(held(
            "the replacement does not have one exact local store root",
        ));
    }
    Ok(root)
}

fn held(message: impl Into<String>) -> SyncError {
    SyncError::CloudPurgeHeld(message.into())
}
