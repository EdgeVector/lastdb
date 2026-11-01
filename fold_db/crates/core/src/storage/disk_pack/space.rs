//! Native disk-space reservation policy.

use super::*;
use laststore::space::SpacePolicy;

/// An unset cap keeps compatibility; an explicit cap must be a positive byte count.
pub(in crate::storage) fn policy_from_env() -> StorageResult<SpacePolicy> {
    let mut policy = SpacePolicy::default();
    match std::env::var("LASTDB_DISK_CAP_BYTES") {
        Ok(raw) => {
            policy.disk_cap_bytes =
                raw.parse::<u64>().ok().filter(|n| *n > 0).ok_or_else(|| {
                    StorageError::ConfigurationError(
                        "LASTDB_DISK_CAP_BYTES must be a positive integer byte count".into(),
                    )
                })?;
        }
        Err(std::env::VarError::NotPresent) => {}
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(StorageError::ConfigurationError(
                "LASTDB_DISK_CAP_BYTES must contain a positive integer byte count".into(),
            ));
        }
    }
    Ok(policy)
}
