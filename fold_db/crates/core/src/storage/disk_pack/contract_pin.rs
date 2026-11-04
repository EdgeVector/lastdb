//! Disk boundary for the cloud-format spike.
//!
//! Cloud planning does not call this module and does not receive a path,
//! a write handle, or a way to release the pin.

use std::path::Path;

use super::PackNamespacedStore;
use crate::storage::cloud_contract::ContractError;
use crate::storage::disk_contract::pin_homes;

pub use crate::storage::disk_contract::PinnedDiskExport;

/// Hardlink the published run homes and the node marker into `hold_dir`.
///
/// `hold_dir` is a disk-boundary argument. It has to be on the same filesystem
/// as the node home. It is not a cloud destination.
pub fn pin_disk_export(
    store: &PackNamespacedStore,
    hold_dir: &Path,
) -> Result<PinnedDiskExport, ContractError> {
    pin_homes(
        hold_dir,
        &store.runtime.physical.home,
        &store.runtime.physical.cas,
    )
}
