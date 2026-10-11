//! Durable cloud purge worker operations.

use super::storage::save_intent;
use super::*;

pub(super) fn worker(host: &Host, id: &str, stop: &AtomicBool, cancel: &AtomicBool) {
    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())
        .and_then(|runtime| runtime.block_on(run(host, id, stop, cancel)));
    if let Err(error) = result {
        if let Ok(mut intent) = selected_intent(host, Some(id)) {
            intent.phase = "failed".into();
            intent.error = Some(error);
            let _ = save_intent(host, &intent);
        }
    }
}

pub(super) async fn run(
    host: &Host,
    id: &str,
    stop: &AtomicBool,
    cancel: &AtomicBool,
) -> Result<(), String> {
    let mut intent = selected_intent(host, Some(id))?;
    if cancel.load(Ordering::Acquire) || super::storage::cancel_requested(host, &intent)? {
        return finish_cancel(host, &mut intent).await;
    }
    if !crate::cloud_purge_job::directory(&host.home, id)?
        .join("job.json")
        .exists()
    {
        let preparation = crate::cloud_purge_job::source_engine(host)
            .await?
            .cloud_purge_auth()
            .cloud_purge_prepare(&intent.account)
            .await
            .map_err(|error| error.to_string())?;
        if preparation.account != intent.account {
            return Err("the cloud resolved another purge account".into());
        }
        if preparation.capabilities.get("purge_resync") != Some(&serde_json::Value::Bool(true)) {
            return Err("the cloud purge protocol deployment is not ready".into());
        }
        if preparation.capabilities.get("erase_first") != Some(&serde_json::Value::Bool(true)) {
            return Err(
                "the cloud purge needs verified old upload cutoff and complete account inventory"
                    .into(),
            );
        }
    }
    intent.local_cut_started = true;
    save_intent(host, &intent)?;
    let fence = ensure_fence(host, id).await?;
    intent.phase = "source_preparing".into();
    save_intent(host, &intent)?;
    crate::cloud_purge_job::prepare_held(host, intent.account.clone(), id, &fence).await?;
    intent.phase = "active".into();
    save_intent(host, &intent)?;
    while !stop.load(Ordering::Acquire) {
        if cancel.load(Ordering::Acquire) || super::storage::cancel_requested(host, &intent)? {
            return finish_cancel(host, &mut intent).await;
        }
        if crate::cloud_purge_job::run_step(host, id).await? {
            let job = fold_db::sync::cloud_purge::CloudPurgeJob::reopen(
                &crate::cloud_purge_job::directory(&host.home, id)?,
            )
            .map_err(|error| error.to_string())?;
            if job.control.as_ref().is_some_and(|control| {
                control.phase == fold_db::sync::cloud_purge::protocol::CloudPurgePhase::Ready
            }) {
                adopt_selected(host, &job).await?;
            } else {
                if job.control.is_some() {
                    adopt_selected(host, &job).await?;
                }
                intent.phase = "cancelled".into();
                intent.local_cut_started = false;
                save_intent(host, &intent)?;
                release_fence(host, id)?;
                return Ok(());
            }
            intent.phase = "ready".into();
            intent.local_cut_started = false;
            save_intent(host, &intent)?;
            release_fence(host, id)?;
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
    intent.phase = "interrupted".into();
    save_intent(host, &intent)
}

pub(super) async fn adopt_selected(
    host: &Host,
    job: &fold_db::sync::cloud_purge::CloudPurgeJob,
) -> Result<(), String> {
    let engine = crate::cloud_purge_job::source_engine(host).await?;
    let peer = engine
        .cloud_purge_peer()
        .ok_or("selected cloud peer receipt store absent")?;
    let control = job
        .control
        .as_ref()
        .ok_or("selected cloud control absent")?;
    if !control.permits_local_adoption() {
        return Err("the cloud control does not permit local adoption".into());
    }
    peer.observe(&fold_db::sync::cloud_purge::protocol::CloudControlGet {
        account: Some(job.account.clone()),
        protocol_required: true,
        generation: control.generation,
        control: Some(control.clone()),
    })
    .map_err(|error| error.to_string())?;
    peer.bind_local_account(&job.account)
        .map_err(|error| error.to_string())?;
    peer.ensure_adopted(&engine.cloud_purge_auth(), control)
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

pub(super) async fn finish_cancel(host: &Host, intent: &mut Intent) -> Result<(), String> {
    let directory = crate::cloud_purge_job::directory(&host.home, &intent.operation_id)?;
    if directory.join("job.json").exists() {
        crate::cloud_purge_job::cancel(host, &intent.operation_id).await?;
        let job = fold_db::sync::cloud_purge::CloudPurgeJob::reopen(&directory)
            .map_err(|error| error.to_string())?;
        if job.control.is_some() {
            adopt_selected(host, &job).await?;
        }
    }
    intent.phase = "cancelled".into();
    intent.local_cut_started = false;
    save_intent(host, intent)?;
    release_fence(host, &intent.operation_id)
}
