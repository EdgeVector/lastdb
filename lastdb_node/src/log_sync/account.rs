//! Node account for one cloud log connection.

use cloud_backup_protocol::v2::context::ContextReadRequest;
use cloud_backup_protocol::v2::Version;
use cloud_backup_protocol::{AccountId, CloudScope};

use crate::host::Host;

use super::http::{self, Endpoint};

/// Publisher account scope from the saved connection. Receivers do not call this.
pub(super) async fn publisher_scope(
    host: &Host,
) -> Result<(CloudScope, Endpoint, &'static str), String> {
    let selected = http::selected_service(&host.home)?;
    let account = account_of(host)?;
    let endpoint = Endpoint::open(&selected.api_url, selected.label)?.load_owner_key(&host.home);
    let backup = crate::cloud::client_from_home(&host.home)?;
    let context = backup
        .context(&ContextReadRequest {
            version: Version::V2,
            account_id: account.clone(),
            required_destinations: Vec::new(),
        })
        .await
        .map_err(|_| "cloud account context failed")?;
    if context.scope.account_id != account {
        return Err("cloud account does not match this node key".into());
    }
    Ok((context.scope, endpoint, selected.label))
}

pub(super) fn node_key(host: &Host) -> String {
    hex::encode(host.keypair.public_key_bytes())
}

pub(super) fn account_of(host: &Host) -> Result<AccountId, String> {
    let derived = crate::identity::user_hash_from_pubkey(&host.public_key())?;
    if derived != host.user_hash {
        return Err("node account id does not match the node key".into());
    }
    AccountId::try_from(derived).map_err(|_| "node account id is not a cloud account id".into())
}
