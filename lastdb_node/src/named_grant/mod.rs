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
    /// The node has an account and this database's cloud log is not confirmed.
    Enrolling,
}

/// Classify a cloud failure by the database's current enrollment.
/// This does not wait. `Unconfirmed` remains when the database is enrolled or
/// the saved credentials could not be verified.
pub(crate) fn cloud_failure(home: &Path, database_id: &str, error: &str) -> ConfigError {
    if error == crate::cloud::PAYMENT_REQUIRED_MESSAGE {
        return ConfigError::PaymentRequired;
    }
    match cloud_state::database_cloud_state(home, database_id) {
        cloud_state::CloudState::NoAccount => ConfigError::NoAccount,
        cloud_state::CloudState::Enrolling => ConfigError::Enrolling,
        cloud_state::CloudState::Unverified | cloud_state::CloudState::Enrolled => {
            ConfigError::Unconfirmed
        }
    }
}
