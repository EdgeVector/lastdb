//! Node account and canonical JSON for one cloud log connection.

use cloud_backup_protocol::{canonical_bytes, AccountId};
use mutation_log::LogError;
use serde::Serialize;

use crate::host::Host;

pub(super) fn json_body<T: Serialize>(value: &T) -> Result<Vec<u8>, LogError> {
    canonical_bytes(value).map_err(|_| LogError::InvalidEntry)
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
