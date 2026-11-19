//! One database's cloud enrollment.
//!
//! The sync loop records a database here when cloud register returns live.
//! This is that same set, held where a share call can read it. There is no
//! second tracker. The set is empty after a process start until register
//! returns live again.
//!
//! `NoAccount`: verified credentials are missing, or they have no account
//! connection. `Enrolling`: a usable account exists and this database is not
//! in the set. `Enrolled`: the sync loop has recorded this database.

use std::collections::HashSet;
use std::path::Path;
use std::sync::{Mutex, OnceLock};

use tokio::sync::Notify;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CloudState {
    NoAccount,
    Enrolling,
    Enrolled,
}

pub(crate) fn has_cloud_account(home: &Path) -> bool {
    match crate::cloud::load_verified_cloud_credentials(home) {
        Ok(Some(creds)) => creds.connection.is_some(),
        _ => false,
    }
}

pub(crate) fn database_cloud_state(home: &Path, database_id: &str) -> CloudState {
    if !has_cloud_account(home) {
        CloudState::NoAccount
    } else if confirmed(database_id) {
        CloudState::Enrolled
    } else {
        CloudState::Enrolling
    }
}

/// Record a live cloud log and wake share calls that are already waiting.
/// A waiter that has not registered yet misses the signal. The wait polls.
pub(crate) fn remember(database_id: &str) {
    confirmed_set().insert(database_id.to_owned());
    signal().notify_waiters();
}

pub(crate) fn confirmed(database_id: &str) -> bool {
    confirmed_set().contains(database_id)
}

pub(crate) fn notified() -> tokio::sync::futures::Notified<'static> {
    signal().notified()
}

fn confirmed_set() -> std::sync::MutexGuard<'static, HashSet<String>> {
    static CREATED: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    CREATED
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn signal() -> &'static Notify {
    static SIGNAL: OnceLock<Notify> = OnceLock::new();
    SIGNAL.get_or_init(Notify::new)
}
