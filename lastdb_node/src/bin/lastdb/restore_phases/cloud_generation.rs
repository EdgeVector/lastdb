//! Refuse remote generations that require the retired journal-based repair.

use super::*;
use fold_db::sync::cloud_purge::protocol::{CloudAccount, CloudAccountKind};

pub(crate) fn require_legacy_cloud_generation(
    identity: &Identity,
    flavor: &Flavor<'_>,
) -> Phase<()> {
    if !flavor.remote_only() {
        return Ok(());
    }
    let clients = build_cloud_clients(identity, "")?;
    let signer = fold_db::security::Ed25519KeyPair::from_secret_key(&identity.seed)
        .map_err(|_| scope_failure("the recovery identity is invalid"))?;
    let account = CloudAccount {
        kind: CloudAccountKind::Personal,
        id: lastdb_node::host::user_hash_from_pubkey(&signer.public_key_base64())
            .map_err(scope_failure)?,
    };
    let current = clients
        .runtime
        .block_on(clients.auth.cloud_control_get(&account))
        .map_err(|error| scope_failure(error.to_string()))?;
    if current.notification_only
        || current.account.as_ref() != Some(&account)
        || current.protocol_required
        || current.generation != 0
        || current.control.is_some()
    {
        return Err(scope_failure(
            "this cloud generation requires the retired cloud repair workflow",
        ));
    }
    Ok(())
}
