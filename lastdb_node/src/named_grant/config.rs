//! Failure classification for the publisher's cloud calls.
//!
//! Grants live in the signed chain. This module keeps only the error type the
//! remaining invitation, link, and member routes answer with, and the
//! publisher session open that those routes share.

use super::PublisherSession;
use std::path::Path;

pub(crate) enum ConfigError {
    Invalid(String),
    PaymentRequired,
    Unconfirmed,
    /// The node has no usable cloud account. The caller does not wait.
    NoAccount,
    /// The node has an account and this database's cloud log is not confirmed.
    Enrolling,
}

pub(super) fn unconfirmed_or_payment(error: &str) -> ConfigError {
    if error == crate::cloud::PAYMENT_REQUIRED_MESSAGE {
        ConfigError::PaymentRequired
    } else {
        ConfigError::Unconfirmed
    }
}

/// Classify a cloud failure by the database's current enrollment.
/// This does not wait. `Unconfirmed` remains when the database is enrolled or
/// the saved credentials could not be verified.
pub(crate) fn cloud_failure(home: &Path, database_id: &str, error: &str) -> ConfigError {
    if error == crate::cloud::PAYMENT_REQUIRED_MESSAGE {
        return ConfigError::PaymentRequired;
    }
    match super::cloud_state::database_cloud_state(home, database_id) {
        super::cloud_state::CloudState::NoAccount => ConfigError::NoAccount,
        super::cloud_state::CloudState::Enrolling => ConfigError::Enrolling,
        super::cloud_state::CloudState::Unverified | super::cloud_state::CloudState::Enrolled => {
            ConfigError::Unconfirmed
        }
    }
}

pub(super) async fn open(home: &Path) -> Result<PublisherSession, ConfigError> {
    PublisherSession::open(home).await.map_err(|error| {
        if super::cloud_state::no_cloud_account(home) {
            ConfigError::NoAccount
        } else {
            unconfirmed_or_payment(&error)
        }
    })
}
