//! Complete bounded file coverage over one retained native root.
use super::super::file_dependencies as files;
use super::dependency_state::*;
use super::dependency_status::{audit, status};
use super::dependency_store::DependencyStore;
use super::{invalid, ExportInner, NativeBackupCapture, StorageResult};
use cloud_backup_protocol::{BackupJob, Digest, FileDependencyKind};
use lastdb_storage_policy::backup_decode as budget;
use laststore::disk_pack::home::HomeRead;
use laststore::disk_pack::ReadBudget;
use std::sync::{atomic::Ordering, Arc};

/// Uses the existing authorized file APIs. It performs no request until an
/// explicit capture call needs bytes absent from the retained native root.
pub struct AuthorizedFileSource {
    pub auth: crate::sync::auth::AuthClient,
    pub s3: crate::sync::s3::S3Client,
}

#[async_trait::async_trait]
pub trait FileSource: Send + Sync {
    async fn fetch_to_file(
        &self,
        dependency: &files::FileDependency,
        path: &std::path::Path,
    ) -> StorageResult<()>;
}

#[async_trait::async_trait]
impl FileSource for AuthorizedFileSource {
    async fn fetch_to_file(
        &self,
        dependency: &files::FileDependency,
        path: &std::path::Path,
    ) -> StorageResult<()> {
        Self::fetch_to_file(self, dependency, path).await
    }
}

impl AuthorizedFileSource {
    pub(super) async fn fetch_to_file(
        &self,
        dependency: &files::FileDependency,
        path: &std::path::Path,
    ) -> StorageResult<()> {
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
        self.s3
            .download_to_file_limited(&url, path, dependency.expected_stored_bytes())
            .await
            .map_err(|_| invalid("authorized backup file source download failed"))?
            .ok_or_else(|| invalid("authorized backup file source is absent"))?;
        Ok(())
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
            let active = ActiveWork::acquire(&inner)?;
            active.require_exclusive()?;
            let (job, current) = selection(&inner)?;
            let store = DependencyStore::open(&inner.store, &job)?;
            if let Some(mut state) = store.state(&job, &current)? {
                if state.certified {
                    if !state.metadata_covered {
                        certify_metadata(&inner, &key)?;
                        state.metadata_covered = true;
                        store.save_state(&state)?;
                    }
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
                metadata_covered: false,
                entries: 0,
                native_covered: 0,
                external_confirmed: 0,
                catalog_bytes: 0,
                active_external: None,
                active_entry_bytes: 0,
            };
            store.save_state(&state)?;
            certify(&inner, &store, &mut state, &key)?;
            certify_metadata(&inner, &key)?;
            state.metadata_covered = true;
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
        let active = ActiveWork::acquire(&self.inner)?;
        active.require_exclusive()?;
        let (job, current) = selection(&self.inner)?;
        let store = DependencyStore::open(&self.inner.store, &job)?;
        let state = ready(&store, &job, &current)?;
        audit(&store, &state)
    }
}

fn certify_metadata(inner: &ExportInner, key: &[u8; 32]) -> StorageResult<()> {
    let current = inner
        .source
        .as_ref()
        .ok_or_else(|| invalid("backup retained source is unavailable"))?
        .current()
        .clone();
    files::verify_metadata_coverage(&current, key, |budget| {
        inner
            .runtime
            .home
            .read_from_writer(current.clone(), budget, &inner.runtime.read_lease)
            .map_err(super::super::map)
    })?;
    Ok(())
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
                .next_page_yielding_bounded(1, 0, 0, budget::ATOM_BODY_MAX_BYTES)
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
                pages: if bytes == files::ATOM_INVENTORY_READ_BYTES {
                    budget::ATOM_INVENTORY_PAGES
                } else {
                    budget::NATIVE_STREAM_PAGES
                },
                blocks: budget::NATIVE_STREAM_BLOCKS,
                bytes,
                keys: if bytes == budget::NATIVE_STREAM_LOOKUP_READ_BYTES {
                    1
                } else {
                    10000
                },
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
    if !state.certified || !state.metadata_covered {
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
        .saturating_add(state.catalog_bytes)
        .saturating_add(super::dependency_parts::metadata_reserve())
        .saturating_add(record.spool_bytes)
        .saturating_add(inner.coordinator_spool_bytes.load(Ordering::Acquire))
        .saturating_add(inner.assembly_spool_bytes.load(Ordering::Acquire))
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
    _work: Option<tokio::sync::OwnedMutexGuard<()>>,
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
            _work: Some(work),
        })
    }

    pub fn require_exclusive(&self) -> StorageResult<()> {
        if self.inner.readers.load(Ordering::Acquire) != 1 {
            return Err(invalid(
                "backup readers must stop before dependency changes",
            ));
        }
        Ok(())
    }

    /// An opened immutable file keeps the reader count and session alive, but
    /// releases the transition mutex so other bounded readers can open.
    pub fn release_work(&mut self) {
        self._work = None;
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
    let mut read = held_read(inner, budget::DIRECTORY_READ_BYTES)?;
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
        let native = super::dependency_native::verify(inner, store, key, &dependency)?;
        let coverage = match native {
            Some(proof) => FileCoverage::NativeCovered { proof },
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
