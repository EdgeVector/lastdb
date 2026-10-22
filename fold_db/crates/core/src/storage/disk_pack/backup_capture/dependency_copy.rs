use super::super::{file_dependencies as files, file_recovery};
use super::dependencies::{
    held_read, ready, require_room, selection, ActiveWork, AuthorizedFileSource,
};
use super::dependency_state::*;
use super::dependency_store::DependencyStore;
use super::{invalid, protocol, ExportInner, NativeBackupCapture, StorageResult};
use cloud_backup_protocol::{DecimalU64, Digest, Id};
use laststore::disk_pack::maintenance::SpaceReservation;
use std::io::Read;
use std::sync::Arc;

impl NativeBackupCapture {
    /// Authenticate the complete original ciphertext on disk, then freeze its
    /// bounded transfer extents. One unconfirmed external file remains active.
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
        if let Some(mut local) = plan.local.take() {
            let phase = Arc::clone(&active);
            plan = super::super::super::laststore::LastStoreKvStore::run_blocking(move || {
                let _phase = phase;
                super::dependency_io::copy_local(
                    &plan.store,
                    plan.dependency.expected_stored_bytes(),
                    &mut local,
                )?;
                Ok(plan)
            })
            .await?;
        } else {
            plan.store.release_pending()?;
            source
                .ok_or_else(|| invalid("external file needs an authorized source adapter"))?
                .fetch_to_file(&plan.dependency, &plan.store.pending())
                .await?;
        }
        super::super::super::laststore::LastStoreKvStore::run_blocking(move || {
            let _phase = active;
            finish_copy(*plan)
        })
        .await
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
    let body = {
        let mut read = held_read(inner, 24 * 1024 * 1024)?;
        read.get_bounded("atoms", &encoded, 4 * 1024 * 1024)
            .map_err(super::super::map)?
            .ok_or_else(|| invalid("backup dependency atom is absent"))?
    };
    let atom = files::open_atom(key, &body)?;
    if atom.uuid() != entry.dependency.atom_id {
        return Err(invalid("backup dependency atom identity"));
    }
    let dependencies = files::expected(&atom)?;
    dependencies
        .into_iter()
        .find(|dependency| dependency.record == entry.dependency)
        .ok_or_else(|| invalid("backup dependency differs from exact retained atom"))
}

fn local_recovery(
    inner: &ExportInner,
    dependency: &files::FileDependency,
) -> StorageResult<Option<Box<dyn Read + Send>>> {
    let data = inner
        .runtime
        .home
        .path()
        .parent()
        .ok_or_else(|| invalid("backup node data root"))?;
    let home =
        Id::try_from(inner.runtime.home.descriptor().identity.to_string()).map_err(protocol)?;
    Ok(file_recovery::open_verified_ciphertext(
        data,
        &home,
        dependency.logical_pointer_sha256(),
        dependency.kind(),
        dependency.access(),
    )?
    .map(|file| Box::new(file) as Box<dyn Read + Send>))
}

struct FileCopy {
    store: DependencyStore,
    entry: BackupFileDependencyEntry,
    dependency: files::FileDependency,
    local: Option<Box<dyn Read + Send>>,
    _space: SpaceReservation,
}
enum CopyPlan {
    Complete(Box<BackupFileDependencyEntry>),
    Copy(Box<FileCopy>),
}

fn begin_copy(inner: &ExportInner, index: u64, key: &[u8; 32]) -> StorageResult<CopyPlan> {
    let (job, current) = selection(inner)?;
    let store = DependencyStore::open(&inner.store, &job)?;
    let mut state = ready(&store, &job, &current)?;
    super::dependency_confirm::reconcile(&store, &mut state)?;
    if index >= state.entries {
        return Err(invalid("backup dependency does not exist"));
    }
    let mut entry = store.entry(&job, index)?;
    let frozen = match &entry.coverage {
        FileCoverage::NativeCovered { .. } => {
            return Err(invalid("native-covered file requires no external copy"))
        }
        FileCoverage::BackupOwned { input, receipt, .. } => {
            if receipt.is_some() {
                return Ok(CopyPlan::Complete(Box::new(entry)));
            }
            Some(input.object.sha256.clone())
        }
        FileCoverage::BackupOwnedExtents {
            stored_source_sha256,
            parts,
            confirmed,
            ..
        } => {
            if parts == confirmed {
                return Ok(CopyPlan::Complete(Box::new(entry)));
            }
            Some(stored_source_sha256.clone())
        }
        FileCoverage::ExternalNeeded => None,
    };
    if entry
        .dependency
        .expected_stored_bytes
        .div_ceil(cloud_backup_protocol::limits::CLOUD_OBJECT_MAX_BYTES)
        > cloud_backup_protocol::limits::MANIFEST_PAGE_ENTRIES_MAX as u64
    {
        return Err(invalid(
            "external file exceeds existing manifest extent capacity",
        ));
    }
    if state.active_external.is_some_and(|active| active != index) {
        return Err(invalid(
            "confirm the active file parts before the next copy",
        ));
    }
    if let Some(digest) = frozen {
        if super::dependency_io::verify_blob(
            &store,
            index,
            entry.dependency.expected_stored_bytes,
            &digest,
        )? {
            // Repeat the barrier after an uncertain prior metadata publication.
            store.save_entry(&entry)?;
            return Ok(CopyPlan::Complete(Box::new(entry)));
        }
    }
    require_room(inner, &state, super::dependency_parts::metadata_reserve())?;
    if state.active_external.is_none() {
        state.active_entry_bytes = store.entry_bytes(index)?;
    }
    state.active_external = Some(index);
    store.save_state(&state)?;
    // A cancelled download is untrusted and owns no transfer identity. Remove
    // it before a full reservation, or its bytes can block every future retry.
    store.release_pending()?;
    let dependency = exact_dependency(inner, &entry, key)?;
    if let Some(mut file) =
        super::dependency_io::existing_blob(&store, index, dependency.expected_stored_bytes())?
    {
        let digest = file_recovery::verify_remote_ciphertext(dependency.access(), &mut file)?;
        set_source(&mut entry, &digest)?;
        freeze_and_save(&store, &entry, &mut file, &digest)?;
        return Ok(CopyPlan::Complete(Box::new(entry)));
    }
    let space = super::dependency_io::reserve(inner, dependency.expected_stored_bytes())?;
    let local = local_recovery(inner, &dependency)?;
    Ok(CopyPlan::Copy(Box::new(FileCopy {
        store,
        entry,
        dependency,
        local,
        _space: space,
    })))
}

fn finish_copy(mut plan: FileCopy) -> StorageResult<BackupFileDependencyEntry> {
    let bytes = plan.dependency.expected_stored_bytes();
    let mut pending = super::dependency_io::input(&plan.store.pending(), bytes)?;
    let digest = file_recovery::verify_remote_ciphertext(plan.dependency.access(), &mut pending)?;
    set_source(&mut plan.entry, &digest)?;
    drop(pending);
    let mut file = super::dependency_io::publish(&plan.store, plan.entry.index, bytes)?;
    freeze_and_save(&plan.store, &plan.entry, &mut file, &digest)?;
    Ok(plan.entry)
}

fn set_source(entry: &mut BackupFileDependencyEntry, digest: &Digest) -> StorageResult<()> {
    match &entry.coverage {
        FileCoverage::BackupOwned { input, .. } if input.object.sha256 != *digest => {
            return Err(invalid("external file retry differs from frozen source"))
        }
        FileCoverage::BackupOwnedExtents {
            stored_source_sha256,
            ..
        } if stored_source_sha256 != digest => {
            return Err(invalid("external file retry differs from frozen source"))
        }
        FileCoverage::ExternalNeeded => {
            let bytes = entry.dependency.expected_stored_bytes;
            entry.coverage = FileCoverage::BackupOwnedExtents {
                stored_source_sha256: digest.clone(),
                stored_source_bytes: DecimalU64::new(bytes),
                parts: bytes.div_ceil(cloud_backup_protocol::limits::CLOUD_OBJECT_MAX_BYTES) as u16,
                confirmed: 0,
            };
        }
        _ => {}
    }
    Ok(())
}

fn freeze_and_save(
    store: &DependencyStore,
    entry: &BackupFileDependencyEntry,
    file: &mut std::fs::File,
    digest: &Digest,
) -> StorageResult<()> {
    if matches!(entry.coverage, FileCoverage::BackupOwnedExtents { .. }) {
        super::dependency_parts::freeze(store, entry, file, digest)?;
    }
    store.save_entry(entry)
}
