//! Explicit credential load and exact offline native recovery selection.
//! No helper enrolls a device, adopts a generation, or starts legacy sync.

mod credentials;
mod discovery;
mod restore;

pub(crate) use credentials::client_for_recovery;
pub use credentials::client_from_home;
pub use discovery::{discover, NativeBackupDiscovery};
pub use restore::{prepare_restore, NativeRestore, NativeRestoreRequest};

use cloud_backup_protocol::AccountId;
use std::path::Path;

pub fn account_from_phrase(phrase: &str) -> Result<AccountId, String> {
    let seed = zeroize::Zeroizing::new(
        crate::cloud::seed_from_phrase(phrase).map_err(|_| "invalid recovery phrase".to_owned())?,
    );
    let pair = fold_db::security::Ed25519KeyPair::from_secret_key(seed.as_ref())
        .map_err(|_| "invalid recovery identity".to_owned())?;
    let account = lastdb_identity::user_hash_from_pubkey(&pair.public_key_base64())?;
    AccountId::try_from(account).map_err(|_| "invalid recovery account".to_owned())
}

fn enabled() -> Result<(), String> {
    crate::native_cloud_backup::require_enabled().map_err(|error| error.to_string())
}

fn path_text(path: &Path) -> String {
    path.display().to_string()
}
