use super::dependencies::{ready, selection, ActiveWork};
use super::dependency_state::{BackupFileDependencyEntry, DependencyState, FileCoverage};
use super::dependency_store::DependencyStore;
use super::{invalid, protocol, NativeBackupCapture, StorageResult};
use cloud_backup_protocol::transfer::{ObjectReceipt, TransferStatus, TransferView};
use std::sync::atomic::Ordering;

impl NativeBackupCapture {
    /// Compatibility entry point for the first extent. Multi-part callers use
    /// confirm_file_part with the exact frozen part identity.
    pub fn confirm_file_transfer(&self, index: u64, response: &TransferView) -> StorageResult<()> {
        self.confirm_file_part(index, 0, response)
    }

    pub fn confirm_file_part(
        &self,
        index: u64,
        part: u16,
        response: &TransferView,
    ) -> StorageResult<()> {
        let _active = ActiveWork::acquire(&self.inner)?;
        if self.inner.readers.load(Ordering::Acquire) != 1 {
            return Err(invalid("backup readers must stop before spool release"));
        }
        let (job, current) = selection(&self.inner)?;
        let store = DependencyStore::open(&self.inner.store, &job)?;
        let mut state = ready(&store, &job, &current)?;
        if index >= state.entries {
            return Err(invalid("backup dependency does not exist"));
        }
        let mut entry = store.entry(&job, index)?;
        match &mut entry.coverage {
            FileCoverage::BackupOwned { input, receipt, .. } if part == 0 => {
                response.validate_for(input).map_err(protocol)?;
                let verified = verified(response)?;
                if receipt.as_ref().is_some_and(|prior| prior != verified) {
                    return Err(invalid("backup receipt changed"));
                }
                if receipt.is_none() && state.active_external != Some(index) {
                    return Err(invalid("backup transfer lacks active source intent"));
                }
                *receipt = Some(verified.clone());
                store.save_entry(&entry)?;
            }
            FileCoverage::BackupOwnedExtents { parts, .. } if part < *parts => {
                let mut saved = store.part(&entry, part)?;
                response.validate_for(&saved.input).map_err(protocol)?;
                let verified = verified(response)?;
                if saved
                    .receipt
                    .as_ref()
                    .is_some_and(|prior| prior != verified)
                {
                    return Err(invalid("backup part receipt changed"));
                }
                if saved.receipt.is_none() && state.active_external != Some(index) {
                    return Err(invalid("backup part lacks active source intent"));
                }
                saved.receipt = Some(verified.clone());
                store.save_part(&entry, &saved)?;
                recount(&store, &mut entry)?;
            }
            _ => return Err(invalid("backup file has no selected frozen transfer")),
        }
        if complete(&entry.coverage) {
            if state.active_external == Some(index) {
                complete_active(&store, &mut state, index)?;
            } else {
                store.release_blob(index)?;
            }
        }
        Ok(())
    }
}

fn verified(response: &TransferView) -> StorageResult<&ObjectReceipt> {
    match &response.state {
        TransferStatus::Verified { receipt } => Ok(receipt),
        _ => Err(invalid(
            "unverified file transfer retains exact source bytes",
        )),
    }
}

fn recount(store: &DependencyStore, entry: &mut BackupFileDependencyEntry) -> StorageResult<()> {
    let FileCoverage::BackupOwnedExtents { parts, .. } = &entry.coverage else {
        // A visible legacy receipt can follow an uncertain directory fsync.
        // Reestablish its barrier before reconciliation can remove its source.
        return store.save_entry(entry);
    };
    let mut count = 0u16;
    for part in 0..*parts {
        count += u16::from(store.part(entry, part)?.receipt.is_some());
    }
    if let FileCoverage::BackupOwnedExtents { confirmed, .. } = &mut entry.coverage {
        *confirmed = count;
    }
    store.save_entry(entry)
}

fn complete(coverage: &FileCoverage) -> bool {
    match coverage {
        FileCoverage::BackupOwned {
            receipt: Some(_), ..
        } => true,
        FileCoverage::BackupOwnedExtents {
            parts, confirmed, ..
        } => parts == confirmed,
        _ => false,
    }
}

pub(super) fn reconcile(store: &DependencyStore, state: &mut DependencyState) -> StorageResult<()> {
    if let Some(index) = state.active_external {
        let mut entry = store.entry(&state.job, index)?;
        recount(store, &mut entry)?;
        if complete(&entry.coverage) {
            complete_active(store, state, index)?;
        }
    }
    Ok(())
}

fn complete_active(
    store: &DependencyStore,
    state: &mut DependencyState,
    index: u64,
) -> StorageResult<()> {
    store.release_blob(index)?;
    store.release_pending()?;
    let entry = store.entry(&state.job, index)?;
    let parts = match entry.coverage {
        FileCoverage::BackupOwnedExtents { parts, .. } => parts,
        _ => 0,
    };
    let current = store.entry_bytes(index)? + store.part_bytes(index, parts)?;
    state.catalog_bytes = state
        .catalog_bytes
        .checked_sub(state.active_entry_bytes)
        .and_then(|bytes| bytes.checked_add(current))
        .ok_or_else(|| invalid("backup catalog byte count differs"))?;
    state.external_confirmed += 1;
    state.active_external = None;
    state.active_entry_bytes = 0;
    store.save_state(state)
}
