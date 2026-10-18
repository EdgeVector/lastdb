//! Cloud On from an Off boot needs the exact local generation acknowledgement.

use crate::Host;
use fold_db::sync::cloud_purge::protocol::{CloudAccount, CloudAccountKind};
use std::sync::Arc;

pub(crate) async fn activate_adopted_generation(
    host: &Host,
) -> Result<Option<serde_json::Value>, String> {
    host.db.require_cloud_sync_supported()?;
    static ACTIVATE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    let _activation = ACTIVATE.lock().await;
    if crate::cloud_purge_runtime::local_writes_paused(host)? {
        return Err(
            "the selected source remains paused until its cloud operation completes".into(),
        );
    }
    let engine = crate::cloud_purge_job::source_engine(host).await?;
    let auth = engine.cloud_purge_auth();
    let account = CloudAccount {
        kind: CloudAccountKind::Personal,
        id: host.user_hash.clone(),
    };
    let current = auth
        .cloud_control_get(&account)
        .await
        .map_err(|error| error.to_string())?;
    if current.notification_only || current.account.as_ref() != Some(&account) {
        return Err("Cloud On needs the authoritative personal account control".into());
    }
    if !current.protocol_required {
        if current.generation != 0 || current.control.is_some() {
            return Err("Cloud On received invalid legacy generation state".into());
        }
        return Ok(None);
    }
    let control = current
        .control
        .as_ref()
        .ok_or("the permanent cloud control is absent")?;
    if !control.permits_local_adoption() {
        return Err("Cloud On remains paused while the account cloud generation is held".into());
    }
    let peer = engine
        .cloud_purge_peer()
        .ok_or("the local adoption receipt store is absent")?;
    peer.observe(&current).map_err(|error| error.to_string())?;
    peer.bind_local_account(&account)
        .map_err(|error| error.to_string())?;
    peer.ensure_adopted(&auth, control)
        .await
        .map_err(|error| error.to_string())?;
    let active = prepare_runtime_engine(host).await?;
    crate::cloud_purge_runtime::cache_engine(host, Arc::clone(&active))?;
    // A durable active config is safe now: a restart uses the same exact peer
    // receipt and cannot publish without a fresh generation/adoption check.
    let renamed = crate::cloud::resume_cloud_sync_file(&host.home)?;
    crate::cloud::clear_cloud_resume_required(&host.home)?;
    if let Err(error) = host
        .db
        .activate_adopted_cloud_purge_engine(Arc::clone(&active))
        .await
    {
        active.set_cloud_sync_disabled(true).await;
        crate::cloud::mark_cloud_resume_required(&host.home)?;
        crate::cloud::pause_cloud_sync_file(&host.home)?;
        return Err(format!(
            "Cloud On could not attach the serving engine: {error}"
        ));
    }
    Ok(Some(
        serde_json::json!({"ok":true,"intent":"on","file_state":"on",
        "file_restored":renamed,"generation":control.generation,"operation_id":control.operation_id,
        "resume_pending":false,"local_adoption_acknowledged":true}),
    ))
}

async fn prepare_runtime_engine(host: &Host) -> Result<Arc<fold_db::sync::SyncEngine>, String> {
    if let Some(engine) = host.db.sync_engine() {
        return Ok(engine);
    }
    let config = crate::cloud_purge_job::read_cloud_config(&host.home)?;
    let seed: [u8; 32] = std::fs::read(host.home.join(crate::host::IDENTITY_KEY_FILE))
        .map_err(|_| "the node identity is unavailable")?
        .as_slice()
        .try_into()
        .map_err(|_| "the node identity length is invalid")?;
    let keys = fold_db::crypto::E2eKeys::from_ed25519_seed(&seed)
        .map_err(|_| "the node encryption keys are unavailable")?;
    let signer = Arc::new(
        fold_db::security::Ed25519KeyPair::from_secret_key(&seed)
            .map_err(|_| "the node signing identity is invalid")?,
    );
    let refresh =
        crate::cloud::auth_refresh_callback(host.home.clone(), config.api_url.clone(), signer);
    let source = host
        .db
        .cloud_purge_source_engine(
            &config.api_url,
            &config.api_key,
            &host.data_dir.to_string_lossy(),
            &keys,
        )
        .await
        .map_err(|error| error.to_string())?;
    let mut engine =
        Arc::try_unwrap(source).map_err(|_| "the inactive runtime engine has another owner")?;
    engine.set_auth_refresh(refresh);
    Ok(Arc::new(engine))
}
