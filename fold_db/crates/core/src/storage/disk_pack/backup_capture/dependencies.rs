//! Complete bounded file coverage over one retained native root.
use super::super::file_dependencies as files;
use super::dependency_state::*;
use super::dependency_status::{audit, status};
use super::dependency_store::DependencyStore;
use super::{invalid, ExportInner, NativeBackupCapture, StorageResult};
use cloud_backup_protocol::{BackupJob, Digest, FileDependencyKind};
use laststore::disk_pack::home::HomeRead;
use laststore::disk_pack::ReadBudget;
use std::sync::{atomic::Ordering, Arc};

/// Uses the existing authorized file APIs. It performs no request until an
/// explicit capture call needs bytes absent from the retained native root.
pub struct AuthorizedFileSource {
    pub auth: crate::sync::auth::AuthClient,
    pub s3: crate::sync::s3::S3Client,
}

impl AuthorizedFileSource {
    pub(super) async fn fetch(&self, dependency: &files::FileDependency) -> StorageResult<Vec<u8>> {
        let url = match dependency.kind() {
            FileDependencyKind::Main => {
                self.auth
                    .presign_file_download_owner_scope(
                        dependency.file_hash().as_str(),
                        dependency.access().owner_scope.as_deref(),
                    )
                    .await
            }
            FileDependencyKind::Thumbnail | FileDependencyKind::Poster => {
                self.auth
                    .presign_thumb_download(dependency.file_hash().as_str())
                    .await
            }
        }
        .map_err(|_| invalid("authorized backup file source request failed"))?;
        let bytes = self
            .s3
            .download_limited(&url, Some(dependency.expected_stored_bytes() as usize))
            .await
            .map_err(|_| invalid("authorized backup file source download failed"))?
            .ok_or_else(|| invalid("authorized backup file source is absent"))?;
        if bytes.len() as u64 != dependency.expected_stored_bytes() {
            return Err(invalid("authorized backup file source length differs"));
        }
        Ok(bytes)
    }
}

impl NativeBackupCapture {
    /// Certify every atom against its same-batch directory, then prove exact
    /// native CAS coverage. Legacy omissions refuse before cloud eligibility.
    pub async fn certify_file_dependencies(
        &self,
        account_key: &[u8; 32],
    ) -> StorageResult<BackupFileDependencyStatus> {
        let inner = Arc::clone(&self.inner);
        let key = zeroize::Zeroizing::new(*account_key);
        super::super::super::laststore::LastStoreKvStore::run_blocking(move || {
            let _active = ActiveWork::acquire(&inner)?;
            let (job, current) = selection(&inner)?;
            let store = DependencyStore::open(&inner.store, &job)?;
            if let Some(state) = store.state(&job, &current)? {
                if state.certified {
                    return status(&store, &state);
                }
                super::dependency_store::cleanup_owned(&inner.store, &job)?;
            }
            let store = DependencyStore::open(&inner.store, &job)?;
            let mut state = DependencyState {
                format: 1,
                job,
                current_sha256: current,
                certified: false,
                entries: 0,
                native_covered: 0,
                external_confirmed: 0,
                catalog_bytes: 0,
                active_external: None,
                active_entry_bytes: 0,
            };
            store.save_state(&state)?;
            certify(&inner, &store, &mut state, &key)?;
            state.certified = true;
            store.save_state(&state)?;
            status(&store, &state)
        })
        .await
    }

    pub fn file_dependency(&self, index: u64) -> StorageResult<BackupFileDependencyEntry> {
        let (job, current) = selection(&self.inner)?;
        let store = DependencyStore::open(&self.inner.store, &job)?;
        let state = ready(&store, &job, &current)?;
        if index >= state.entries {
            return Err(invalid("backup dependency does not exist"));
        }
        store.entry(&job, index)
    }

    pub fn file_dependency_status(&self) -> StorageResult<BackupFileDependencyStatus> {
        let (job, current) = selection(&self.inner)?;
        let store = DependencyStore::open(&self.inner.store, &job)?;
        let state = store
            .state(&job, &current)?
            .ok_or_else(|| invalid("backup file coverage is uncertified"))?;
        status(&store, &state)
    }

    /// Explicit full local catalog audit. Normal status reads only counters and
    /// the one active file; the coordinator must not poll this audit method.
    pub fn audit_file_dependencies(&self) -> StorageResult<BackupFileDependencyStatus> {
        let _active = ActiveWork::acquire(&self.inner)?;
        let (job, current) = selection(&self.inner)?;
        let store = DependencyStore::open(&self.inner.store, &job)?;
        let state = ready(&store, &job, &current)?;
        audit(&store, &state)
    }
}

fn certify(
    inner: &Arc<ExportInner>,
    store: &DependencyStore,
    state: &mut DependencyState,
    key: &[u8; 32],
) -> StorageResult<()> {
    for group in 0..1024u16 {
        let mut start = Vec::new();
        loop {
            let mut read = held_read(inner, files::ATOM_INVENTORY_READ_BYTES)?;
            let mut cursor = files::atom_inventory(&mut read, group, &start)?;
            let page = cursor
                .next_page_yielding(1, 0, 0)
                .map_err(super::super::map)?;
            let next = page.resume.clone().or_else(|| {
                page.records
                    .last()
                    .map(|record| record.key.as_bytes().to_vec())
            });
            drop(cursor);
            drop(read);
            for record in page.records {
                certify_atom(inner, store, state, key, record)?;
            }
            if page.exhausted {
                break;
            }
            let mut next = next.ok_or_else(|| invalid("backup atom continuation is missing"))?;
            if next.len() >= 65535 || (!start.is_empty() && next < start) {
                return Err(invalid("backup atom continuation differs"));
            }
            next.push(0);
            start = next;
        }
    }
    Ok(())
}

pub(super) fn held_read(inner: &ExportInner, bytes: u64) -> StorageResult<HomeRead> {
    let source = inner
        .source
        .as_ref()
        .ok_or_else(|| invalid("backup retained source is unavailable"))?;
    inner
        .runtime
        .home
        .read_from_writer(
            source.current().clone(),
            ReadBudget {
                pages: if bytes == 64 * 1024 * 1024 { 4096 } else { 512 },
                blocks: 16384,
                bytes,
                keys: if bytes == 128 * 1024 * 1024 { 1 } else { 10000 },
            },
            &inner.runtime.read_lease,
        )
        .map_err(super::super::map)
}

pub(super) fn selection(inner: &ExportInner) -> StorageResult<(BackupJob, Digest)> {
    let record = inner
        .record
        .lock()
        .map_err(|_| invalid("backup capture state lock failed"))?;
    record.require_ready()?;
    if inner.source.is_none() {
        return Err(invalid("backup retained source is unavailable"));
    }
    Ok((
        record.begin.job.clone(),
        record.begin.native_root.current_sha256.clone(),
    ))
}

pub(super) fn ready(
    store: &DependencyStore,
    job: &BackupJob,
    current: &Digest,
) -> StorageResult<DependencyState> {
    let state = store
        .state(job, current)?
        .ok_or_else(|| invalid("backup file coverage is uncertified"))?;
    if !state.certified {
        return Err(invalid("backup file coverage is incomplete"));
    }
    Ok(state)
}

pub(super) fn require_room(
    inner: &ExportInner,
    state: &DependencyState,
    extra: u64,
) -> StorageResult<()> {
    let record = inner
        .record
        .lock()
        .map_err(|_| invalid("backup capture state lock failed"))?;
    let reserved = state
        .entries
        .saturating_add(3)
        .saturating_mul(2 * ENTRY_MAX)
        .saturating_add(2 * files::FILE_DEPENDENCY_MAX_STORED_BYTES)
        .saturating_add(record.spool_bytes)
        .saturating_add(extra)
        .saturating_add(1024 * 1024);
    if state.entries >= ENTRIES_MAX
        || reserved > cloud_backup_protocol::limits::DURABLE_LOCAL_SPOOL_MAX_BYTES
    {
        return Err(invalid(
            "backup capture combined spool reservation exceeds its cap",
        ));
    }
    Ok(())
}

pub(super) struct ActiveWork {
    inner: Arc<ExportInner>,
    _work: tokio::sync::OwnedMutexGuard<()>,
}
impl ActiveWork {
    pub fn acquire(inner: &Arc<ExportInner>) -> StorageResult<Self> {
        let work = Arc::clone(&inner.dependency_work)
            .try_lock_owned()
            .map_err(|_| invalid("backup dependency work is active"))?;
        let record = inner
            .record
            .lock()
            .map_err(|_| invalid("backup capture state lock failed"))?;
        record.require_ready()?;
        if inner.readers.load(Ordering::Acquire)
            >= cloud_backup_protocol::limits::CONCURRENT_STREAMS_MAX
        {
            return Err(invalid("backup capture stream limit reached"));
        }
        inner.readers.fetch_add(1, Ordering::AcqRel);
        Ok(Self {
            inner: Arc::clone(inner),
            _work: work,
        })
    }
}
impl Drop for ActiveWork {
    fn drop(&mut self) {
        self.inner.readers.fetch_sub(1, Ordering::AcqRel);
    }
}

fn certify_atom(
    inner: &Arc<ExportInner>,
    store: &DependencyStore,
    state: &mut DependencyState,
    key: &[u8; 32],
    record: laststore::disk_pack::RangeRecord,
) -> StorageResult<()> {
    let Some((scope, id)) = files::atom_coordinate(&record.key)? else {
        return Ok(());
    };
    let atom = files::open_atom(key, &record.body)?;
    if atom.uuid() != id {
        return Err(invalid("backup atom identity differs"));
    }
    let dependencies = files::expected(&atom)?;
    drop(atom);
    drop(record);
    let mut read = held_read(inner, 8 * 1024 * 1024)?;
    files::read_directories(&mut read, scope.as_deref(), &id, &dependencies, key)?;
    drop(read);
    for dependency in dependencies {
        if let Some(index) = store.find(
            &state.job,
            dependency.logical_pointer_sha256(),
            dependency.kind(),
        )? {
            if index >= state.entries {
                return Err(invalid("backup dependency lookup exceeds saved inventory"));
            }
            let mut prior = store.entry(&state.job, index)?.dependency;
            prior.atom_id = dependency.record.atom_id.clone();
            if prior != dependency.record {
                return Err(invalid("same pointer dependency selectors differ"));
            }
            continue;
        }
        require_room(inner, state, ENTRY_MAX)?;
        let mut read = held_read(inner, 128 * 1024 * 1024)?;
        let native = files::read_native_cas(&mut read, &dependency)?;
        drop(read);
        let coverage = match native {
            Some(row) => FileCoverage::NativeCovered {
                proof: files::verify_native_cas(row, key, &dependency)?,
            },
            None => FileCoverage::ExternalNeeded,
        };
        let native = matches!(coverage, FileCoverage::NativeCovered { .. });
        let entry = BackupFileDependencyEntry {
            job: state.job.clone(),
            index: state.entries,
            atom_scope: scope.clone(),
            dependency: dependency.record.clone(),
            coverage,
        };
        store.save_entry(&entry)?;
        store.save_alias(&entry)?;
        state.catalog_bytes += store.entry_metadata_bytes(&entry)?;
        state.entries += 1;
        state.native_covered += u64::from(native);
        store.save_state(state)?;
    }
    Ok(())
}
