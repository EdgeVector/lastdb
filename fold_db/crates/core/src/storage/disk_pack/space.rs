//! Native space reservations and bounded memory-ack flush policy.

use super::*;
use laststore::disk_pack::maintenance::SpacePolicy;

/// An unset cap keeps compatibility; an explicit cap must be a positive byte count.
pub(super) fn policy_from_env() -> StorageResult<SpacePolicy> {
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

pub(super) fn validate_resident_policy() -> StorageResult<()> {
    memory_flush_delay()?;
    Ok(())
}

pub(super) fn memory_flush_delay() -> StorageResult<std::time::Duration> {
    crate::resident::ResidentPolicy::from_env()
        .persist_interval
        .filter(|delay| *delay <= std::time::Duration::from_millis(500))
        .ok_or_else(|| {
            StorageError::ConfigurationError(
                "native memory ack requires LASTDB_RESIDENT_PERSIST_MS between 1 and 500".into(),
            )
        })
}
