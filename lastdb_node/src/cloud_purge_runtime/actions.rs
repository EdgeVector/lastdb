//! Durable cloud purge actions operations.

use super::storage::save_intent;
use super::*;

pub(crate) async fn handle(host: &Host, request: Request) -> Result<Value, String> {
    match request.action.as_str() {
        "begin" => accept(host, request.operation_id, request.account),
        "status" => status(host, request).await,
        "resume" => resume(host, &request),
        "cancel" => cancel(host, request).await,
        _ => Err("unknown cloud purge action".into()),
    }
}

async fn status(host: &Host, request: Request) -> Result<Value, String> {
    let intent = selected_intent(host, request.operation_id.as_deref())?;
    if request.proof_receipt.is_some() && intent.phase != "ready" {
        return Err("cloud purge proof requires the completed local generation switch".into());
    }
    if !crate::cloud_purge_job::directory(&host.home, &intent.operation_id)?
        .join("job.json")
        .exists()
    {
        return serde_json::to_value(intent).map_err(|error| error.to_string());
    }
    let mut status = crate::cloud_purge_job::status(
        host,
        &intent.operation_id,
        request.proof_receipt.as_deref(),
    )
    .await?;
    status
        .as_object_mut()
        .ok_or("cloud purge status is not an object")?
        .insert(
            "node_intent".into(),
            serde_json::to_value(intent).map_err(|error| error.to_string())?,
        );
    Ok(status)
}

fn resume(host: &Host, request: &Request) -> Result<Value, String> {
    let mut intent = selected_intent(host, request.operation_id.as_deref())?;
    require_active(host, &intent)?;
    if intent.phase == "cancelled" {
        return Err("a cancelled purge cannot resume".into());
    }
    if intent.phase == "ready" || is_running(host, &intent.operation_id)? {
        return serde_json::to_value(intent).map_err(|error| error.to_string());
    }
    intent.error = None;
    intent.phase = "accepted".into();
    super::storage::clear_cancel_for_resume(host, &intent)?;
    save_intent(host, &intent)?;
    spawn(host, &intent.operation_id)?;
    serde_json::to_value(intent).map_err(|error| error.to_string())
}

async fn cancel(host: &Host, request: Request) -> Result<Value, String> {
    let mut intent = selected_intent(host, request.operation_id.as_deref())?;
    require_active(host, &intent)?;
    if intent.phase == "ready" {
        return Err("a completed purge cannot cancel".into());
    }
    if intent.phase == "cancelled" {
        return serde_json::to_value(intent).map_err(|error| error.to_string());
    }
    super::storage::request_cancel(host, &intent)?;
    if is_running(host, &intent.operation_id)? {
        let mut runtimes = runtimes()
            .lock()
            .map_err(|_| "cloud purge runtime lock failed")?;
        let runtime = runtimes
            .get_mut(&host.home)
            .ok_or("cloud purge runtime is not attached")?;
        runtime.cancel.store(true, Ordering::Release);
        intent.phase = "cancel_requested".into();
        save_intent(host, &intent)?;
        return serde_json::to_value(intent).map_err(|error| error.to_string());
    }
    super::worker::finish_cancel(host, &mut intent).await?;
    let result = if crate::cloud_purge_job::directory(&host.home, &intent.operation_id)?
        .join("job.json")
        .exists()
    {
        let job = fold_db::sync::cloud_purge::CloudPurgeJob::reopen(
            &crate::cloud_purge_job::directory(&host.home, &intent.operation_id)?,
        )
        .map_err(|error| error.to_string())?;
        serde_json::to_value(job).map_err(|error| error.to_string())?
    } else {
        serde_json::json!({"phase":"cancelled","cloud_hold_started":false})
    };
    Ok(result)
}

pub(super) fn accept(
    host: &Host,
    operation_id: Option<String>,
    account: Option<CloudAccount>,
) -> Result<Value, String> {
    static ACCEPT: OnceLock<Mutex<()>> = OnceLock::new();
    let _accept = ACCEPT
        .get_or_init(|| Mutex::new(()))
        .lock()
        .map_err(|_| "cloud purge acceptance lock failed")?;
    let account = account.unwrap_or_else(|| CloudAccount {
        kind: CloudAccountKind::Personal,
        id: host.user_hash.clone(),
    });
    if account.id.is_empty()
        || (account.kind == CloudAccountKind::Personal && account.id != host.user_hash)
    {
        return Err("personal purge must use this node's account".into());
    }
    let id = operation_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    uuid::Uuid::parse_str(&id).map_err(|_| "cloud purge request ID must be a UUID")?;
    let active = host.home.join("cloud-purge-resync/active.json");
    if active.exists() {
        let existing: Intent = read(&active)?;
        if existing.operation_id == id {
            if existing.account != account {
                return Err("the request ID belongs to another account".into());
            }
            if !matches!(existing.phase.as_str(), "ready" | "cancelled" | "failed") {
                spawn(host, &id)?;
            }
            return serde_json::to_value(existing).map_err(|error| error.to_string());
        }
        if !matches!(existing.phase.as_str(), "ready" | "cancelled") {
            return Err(format!(
                "cloud purge {} already owns the retained source; resume or cancel it",
                existing.operation_id
            ));
        }
        if is_running(host, &existing.operation_id)? {
            return Err("the prior cloud purge worker still owns this home".into());
        }
    }
    let intent = Intent {
        format_version: 1,
        operation_id: id,
        account,
        phase: "accepted".into(),
        error: None,
        local_cut_started: false,
    };
    save_intent(host, &intent)?;
    spawn(host, &intent.operation_id)?;
    serde_json::to_value(intent).map_err(|error| error.to_string())
}

pub(super) fn selected_intent(host: &Host, operation_id: Option<&str>) -> Result<Intent, String> {
    let path = if let Some(id) = operation_id {
        uuid::Uuid::parse_str(id).map_err(|_| "cloud purge job ID must be a UUID")?;
        host.home
            .join("cloud-purge-resync/jobs")
            .join(id)
            .join("intent.json")
    } else {
        host.home.join("cloud-purge-resync/active.json")
    };
    let intent: Intent = read(&path)?;
    if intent.format_version != 1 {
        return Err("unsupported cloud purge intent".into());
    }
    Ok(intent)
}

fn require_active(host: &Host, intent: &Intent) -> Result<(), String> {
    let active = selected_intent(host, None)?;
    if active.operation_id != intent.operation_id || active.account != intent.account {
        return Err("this historical job does not own the active cloud purge".into());
    }
    Ok(())
}
