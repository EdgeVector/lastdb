use super::super::{file_dependencies as files, file_recovery};
use super::dependencies::{
    held_read, ready, require_room, selection, ActiveWork, AuthorizedFileSource,
};
use super::dependency_state::*;
use super::dependency_store::DependencyStore;
use super::{invalid, protocol, ExportInner, NativeBackupCapture, StorageResult};
use cloud_backup_protocol::transfer::{TransferInput, TransferStatus, TransferView};
use cloud_backup_protocol::{bytes_sha256, DecimalU64, Id, ObjectIdentity, ObjectKind, Version};
use std::sync::{atomic::Ordering, Arc};

impl NativeBackupCapture {
    /// Copy one complete authenticated external file into this job's private
    /// spool. One unconfirmed copy at a time bounds disk use across large backups.
    pub async fn prepare_external_file(
        &self,
        index: u64,
        account_key: &[u8; 32],
        source: Option<&AuthorizedFileSource>,
    ) -> StorageResult<BackupFileDependencyEntry> {
        let active = Arc::new(ActiveWork::acquire(&self.inner)?);
        let inner = Arc::clone(&self.inner);
        let key = zeroize::Zeroizing::new(*account_key);
        let phase = Arc::clone(&active);
        let plan = super::super::super::laststore::LastStoreKvStore::run_blocking(move || {
            let _phase = phase;
            begin_copy(&inner, index, &key)
        })
        .await?;
        let mut plan = match plan {
            CopyPlan::Complete(entry) => return Ok(*entry),
            CopyPlan::Copy(plan) => plan,
        };
        let bytes = match plan.local_bytes.take() {
            Some(bytes) => bytes,
            None => {
                source
                    .ok_or_else(|| invalid("external file needs an authorized source adapter"))?
                    .fetch(&plan.dependency)
                    .await?
            }
        };
        let inner = Arc::clone(&self.inner);
        super::super::super::laststore::LastStoreKvStore::run_blocking(move || {
            let _phase = active;
            finish_copy(&inner, *plan, &bytes)
        })
        .await
    }

    /// Supply only an authoritative authenticated transfer response. Save the
    /// exact Verified receipt before local removal. Pending retains exact bytes.
    pub fn confirm_file_transfer(&self, index: u64, response: &TransferView) -> StorageResult<()> {
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
        let FileCoverage::BackupOwned { input, receipt, .. } = &mut entry.coverage else {
            return Err(invalid("backup dependency has no frozen file transfer"));
        };
        response.validate_for(input).map_err(protocol)?;
        let TransferStatus::Verified { receipt: verified } = &response.state else {
            return Err(invalid(
                "unverified file transfer retains local source bytes",
            ));
        };
        if receipt.as_ref().is_some_and(|saved| saved != verified) {
            return Err(invalid("verified file transfer receipt changed"));
        }
        if receipt.is_none() && state.active_external != Some(index) {
            return Err(invalid("backup transfer has no exact active source intent"));
        }
        *receipt = Some(verified.clone());
        store.save_entry(&entry)?;
        if state.active_external == Some(index) {
            complete_active(&store, &mut state, index)?;
        } else {
            store.release_blob(index)?;
        }
        Ok(())
    }
}

fn exact_dependency(
    inner: &ExportInner,
    entry: &BackupFileDependencyEntry,
    key: &[u8; 32],
) -> StorageResult<files::FileDependency> {
    let raw = crate::schema::types::field::build_storage_key(
        entry.atom_scope.as_deref(),
        &crate::atom::atom_key_codec::body_key(&entry.dependency.atom_id),
    );
    let encoded = crate::storage::laststore::native_routing::encode(raw.as_bytes());
    let mut read = held_read(inner, 24 * 1024 * 1024)?;
    let body = read
        .get_bounded("atoms", &encoded, 4 * 1024 * 1024)
        .map_err(super::super::map)?
        .ok_or_else(|| invalid("backup dependency atom is absent"))?;
    drop(read);
    let atom = files::open_atom(key, &body)?;
    if atom.uuid() != entry.dependency.atom_id {
        return Err(invalid("backup dependency atom identity"));
    }
    let dependencies = files::expected(&atom)?;
    drop(atom);
    drop(body);
    dependencies
        .into_iter()
        .find(|dependency| dependency.record == entry.dependency)
        .ok_or_else(|| invalid("backup dependency differs from exact retained atom"))
}

fn local_recovery(
    inner: &ExportInner,
    dependency: &files::FileDependency,
) -> StorageResult<Option<Vec<u8>>> {
    let data = inner
        .runtime
        .home
        .path()
        .parent()
        .ok_or_else(|| invalid("backup node data root"))?;
    let home =
        Id::try_from(inner.runtime.home.descriptor().identity.to_string()).map_err(protocol)?;
    file_recovery::read_verified_ciphertext(
        data,
        &home,
        dependency.logical_pointer_sha256(),
        dependency.kind(),
        dependency.access(),
    )
}

fn reconcile(store: &DependencyStore, state: &mut DependencyState) -> StorageResult<()> {
    if let Some(index) = state.active_external {
        let entry = store.entry(&state.job, index)?;
        if matches!(
            entry.coverage,
            FileCoverage::BackupOwned {
                receipt: Some(_),
                ..
            }
        ) {
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
    let current_bytes = store.entry_bytes(index)?;
    state.catalog_bytes = state
        .catalog_bytes
        .checked_sub(state.active_entry_bytes)
        .and_then(|bytes| bytes.checked_add(current_bytes))
        .ok_or_else(|| invalid("backup catalog byte count differs"))?;
    state.external_confirmed += 1;
    state.active_external = None;
    state.active_entry_bytes = 0;
    store.save_state(state)
}

struct FileCopy {
    store: DependencyStore,
    entry: BackupFileDependencyEntry,
    dependency: files::FileDependency,
    local_bytes: Option<Vec<u8>>,
}

enum CopyPlan {
    Complete(Box<BackupFileDependencyEntry>),
    Copy(Box<FileCopy>),
}

fn begin_copy(inner: &ExportInner, index: u64, key: &[u8; 32]) -> StorageResult<CopyPlan> {
    let (job, current) = selection(inner)?;
    let store = DependencyStore::open(&inner.store, &job)?;
    let mut state = ready(&store, &job, &current)?;
    reconcile(&store, &mut state)?;
    if index >= state.entries {
        return Err(invalid("backup dependency does not exist"));
    }
    let entry = store.entry(&job, index)?;
    if matches!(entry.coverage, FileCoverage::NativeCovered { .. }) {
        return Err(invalid("native-covered file requires no external copy"));
    }
    if let FileCoverage::BackupOwned {
        receipt: Some(_), ..
    } = &entry.coverage
    {
        return Ok(CopyPlan::Complete(Box::new(entry)));
    }
    if state.active_external.is_some_and(|active| active != index) {
        return Err(invalid(
            "confirm the exact active file transfer before the next copy",
        ));
    }
    if let FileCoverage::BackupOwned { input, .. } = &entry.coverage {
        if super::dependency_io::verify_blob(&store, index, &input.object)? {
            return Ok(CopyPlan::Complete(Box::new(entry)));
        }
    }
    require_room(inner, &state, ENTRY_MAX)?;
    if state.active_external.is_none() {
        state.active_entry_bytes = store.entry_bytes(index)?;
    }
    state.active_external = Some(index);
    store.save_state(&state)?;
    let dependency = exact_dependency(inner, &entry, key)?;
    let local_bytes = match super::dependency_io::existing_blob(
        &store,
        index,
        dependency.expected_stored_bytes(),
    )? {
        Some(bytes) => Some(bytes),
        None => local_recovery(inner, &dependency)?,
    };
    Ok(CopyPlan::Copy(Box::new(FileCopy {
        store,
        entry,
        dependency,
        local_bytes,
    })))
}

fn finish_copy(
    inner: &ExportInner,
    mut plan: FileCopy,
    bytes: &[u8],
) -> StorageResult<BackupFileDependencyEntry> {
    let plain = file_recovery::open_remote_file(
        plan.dependency.access(),
        bytes,
        files::FILE_DEPENDENCY_MAX_PLAINTEXT_BYTES,
    )?;
    drop(plain);
    let object = ObjectIdentity {
        sha256: bytes_sha256(bytes),
        bytes: DecimalU64::new(bytes.len() as u64),
        kind: ObjectKind::BlobPart,
    };
    match &plan.entry.coverage {
        FileCoverage::BackupOwned { input, .. } if input.object != object => {
            return Err(invalid(
                "external file retry differs from frozen transfer bytes",
            ))
        }
        FileCoverage::ExternalNeeded => {
            let input = TransferInput {
                version: Version::V1,
                request_id: Id::try_from(uuid::Uuid::new_v4().to_string()).map_err(protocol)?,
                job: plan.entry.job.clone(),
                object,
            };
            plan.entry.coverage = FileCoverage::BackupOwned {
                input_sha256: input.input_sha256().map_err(protocol)?,
                input: Box::new(input),
                receipt: None,
            };
        }
        _ => {}
    }
    super::dependency_io::save_blob(inner, &plan.store, plan.entry.index, bytes)?;
    plan.store.save_entry(&plan.entry)?;
    Ok(plan.entry)
}
