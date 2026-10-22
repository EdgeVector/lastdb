use super::dependency_state::*;
use super::dependency_store::DependencyStore;
use super::{invalid, StorageResult};

pub(super) fn status(
    store: &DependencyStore,
    state: &DependencyState,
) -> StorageResult<BackupFileDependencyStatus> {
    let mut status = BackupFileDependencyStatus {
        certified: state.certified && state.metadata_covered,
        entries: state.entries,
        native_covered: state.native_covered,
        external_needed: 0,
        external_prepared: 0,
        external_confirmed: state.external_confirmed,
        spool_bytes: store.tracked_spool_bytes(state)?,
    };
    if let Some(index) = state.active_external {
        match store.entry(&state.job, index)?.coverage {
            FileCoverage::BackupOwned {
                receipt: Some(_), ..
            } => status.external_confirmed += 1,
            FileCoverage::BackupOwned { .. } => status.external_prepared += 1,
            FileCoverage::ExternalNeeded => {}
            FileCoverage::NativeCovered { .. } => {
                return Err(invalid(
                    "native-covered dependency cannot own the external spool",
                ))
            }
        }
    }
    status.external_needed = status
        .entries
        .checked_sub(status.native_covered + status.external_confirmed + status.external_prepared)
        .ok_or_else(|| invalid("backup dependency counters differ"))?;
    Ok(status)
}

pub(super) fn audit(
    store: &DependencyStore,
    state: &DependencyState,
) -> StorageResult<BackupFileDependencyStatus> {
    let mut checked = BackupFileDependencyStatus {
        certified: state.certified && state.metadata_covered,
        entries: state.entries,
        native_covered: 0,
        external_needed: 0,
        external_prepared: 0,
        external_confirmed: 0,
        spool_bytes: store.spool_bytes(state.entries)?,
    };
    for index in 0..state.entries {
        let entry = store.entry(&state.job, index)?;
        if store.find(
            &state.job,
            &entry.dependency.logical_pointer_sha256,
            entry.dependency.kind,
        )? != Some(index)
        {
            return Err(invalid(
                "backup dependency lookup differs from full inventory",
            ));
        }
        match entry.coverage {
            FileCoverage::NativeCovered { .. } => checked.native_covered += 1,
            FileCoverage::ExternalNeeded => checked.external_needed += 1,
            FileCoverage::BackupOwned {
                receipt: Some(_), ..
            } => checked.external_confirmed += 1,
            FileCoverage::BackupOwned { .. } => checked.external_prepared += 1,
        }
    }
    let expected = status(store, state)?;
    if checked.native_covered != expected.native_covered
        || checked.external_needed != expected.external_needed
        || checked.external_prepared != expected.external_prepared
        || checked.external_confirmed != expected.external_confirmed
        || checked.spool_bytes != expected.spool_bytes
    {
        return Err(invalid("backup native coverage count differs"));
    }
    Ok(checked)
}
