//! Enrollment failure classification for the local open-link mint.
//!
//! Publisher grants live only on the signed chain (`log_sync::chain_grant`).
//! ExeMem share/members and sealed invitations are deleted.

mod cloud_state;

use std::path::Path;

#[derive(Clone, Copy)]
pub(crate) enum ConfigError {
    PaymentRequired,
    Unconfirmed,
    /// The node has no usable cloud account. The caller does not wait.
    NoAccount,
}

/// Classify a cloud failure by the node's saved cloud credentials.
/// `Unconfirmed` remains when credentials exist or could not be verified.
pub(crate) fn cloud_failure(home: &Path, error: &str) -> ConfigError {
    if error == crate::cloud::PAYMENT_REQUIRED_MESSAGE {
        return ConfigError::PaymentRequired;
    }
    match cloud_state::account_cloud_state(home) {
        Some(cloud_state::CloudState::NoAccount) => ConfigError::NoAccount,
        Some(cloud_state::CloudState::Enrolled) | None => ConfigError::Unconfirmed,
    }
}
