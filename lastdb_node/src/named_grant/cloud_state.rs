//! One database's cloud enrollment.
//!
//! The sync loop records a database here when cloud register returns live.
//! This is that same set, held where a share call can read it. There is no
//! second tracker. The set is empty after a process start until register
//! returns live again.
//!
//! `NoAccount`: no credentials are saved, or they have no account connection.
//! `Unverified`: saved credentials could not be read or did not verify. That
//! is not proof that no account exists, so a caller may retry.
//! `Enrolling`: a usable account exists and this database is not in the set
//! for that account and environment. `Enrolled`: the sync loop has recorded
//! this database for the account and environment the credentials name now.
//! A database recorded for another account or environment is not enrolled.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Mutex, OnceLock};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CloudState {
    NoAccount,
    Unverified,
    Enrolling,
    Enrolled,
}

/// True only when the credentials say there is no account: none saved, or
/// saved with no account connection. A read or verify failure is not that.
pub(crate) fn no_cloud_account(home: &Path) -> bool {
    match crate::cloud::load_verified_cloud_credentials(home) {
        Ok(Some(creds)) => creds.connection.is_none(),
        Ok(None) => true,
        Err(_) => false,
    }
}

pub(crate) fn database_cloud_state(home: &Path, database_id: &str) -> CloudState {
    match crate::cloud::load_verified_cloud_credentials(home) {
        Ok(Some(creds)) => match &creds.connection {
            Some(connection)
                if confirmed(
                    database_id,
                    &identity(creds.env.as_str(), connection.account_id.as_str()),
                ) =>
            {
                CloudState::Enrolled
            }
            Some(_) => CloudState::Enrolling,
            None => CloudState::NoAccount,
        },
        Ok(None) => CloudState::NoAccount,
        Err(_) => CloudState::Unverified,
    }
}

/// The account and environment a cloud log was registered under.
pub(crate) fn identity(environment: &str, account_id: &str) -> String {
    format!("{environment}:{account_id}")
}

pub(crate) fn confirmed(database_id: &str, identity: &str) -> bool {
    confirmed_set()
        .get(database_id)
        .is_some_and(|recorded| recorded == identity)
}

fn confirmed_set() -> std::sync::MutexGuard<'static, HashMap<String, String>> {
    static CREATED: OnceLock<Mutex<HashMap<String, String>>> = OnceLock::new();
    CREATED
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
