//! Cloud activation refuses generations that require the removed repair workflow.

use crate::Host;
use fold_db::sync::cloud_purge::protocol::{CloudAccount, CloudAccountKind};

pub(crate) async fn require_legacy_generation(host: &Host) -> Result<(), String> {
    host.db.require_cloud_sync_supported()?;
    let auth = crate::cloud_clients::auth(host)?;
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
    if current.protocol_required || current.generation != 0 || current.control.is_some() {
        return Err("this cloud generation requires the retired cloud repair workflow".into());
    }
    Ok(())
}
