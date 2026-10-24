use fold_db::sync::auth::byte_backup::ByteBackupClient;
use std::path::Path;

/// Use an existing paused or active credential. Never enroll or claim a database.
pub fn client_from_home(home: &Path) -> Result<ByteBackupClient, String> {
    super::require_enabled().map_err(|e| e.to_string())?;
    client_for_recovery(home)
}

pub(super) fn client_for_recovery(home: &Path) -> Result<ByteBackupClient, String> {
    let auth = crate::native_backup_access::auth_for_recovery(home)?;
    ByteBackupClient::from_auth(auth).map_err(|error| error.to_string())
}
