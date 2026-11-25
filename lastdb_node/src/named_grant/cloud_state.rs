//! Cloud account credential state for open-link mint failure classification.

use std::path::Path;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CloudState {
    NoAccount,
    Enrolled,
}

pub(super) fn account_cloud_state(home: &Path) -> Option<CloudState> {
    match crate::cloud::load_verified_cloud_credentials(home) {
        Ok(Some(creds)) if creds.connection.is_some() => Some(CloudState::Enrolled),
        Ok(Some(_) | None) => Some(CloudState::NoAccount),
        Err(_) => None,
    }
}
