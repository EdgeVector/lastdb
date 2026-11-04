use cloud_backup_client::byte_backup::ByteBackupClient;
use std::path::Path;

/// Read saved recovery credentials without enrolling or enabling backup.
pub fn client_from_home(home: &Path) -> Result<ByteBackupClient, String> {
    client_for_recovery(home)
}

pub(super) fn client_for_recovery(home: &Path) -> Result<ByteBackupClient, String> {
    let auth = crate::backup_access::auth_for_recovery(home)?;
    ByteBackupClient::from_auth(auth).map_err(|error| error.to_string())
}
