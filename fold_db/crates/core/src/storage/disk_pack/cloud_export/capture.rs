use super::state::{ExportPhase, ExportRecord};
use super::store::{hex, LocalStore};
use super::{
    catalog, invalid, protocol, Admission, ExportInner, NativeCloudExport,
    NativeCloudExportRequest, PackRuntime, StorageResult,
};
use cloud_backup_protocol::jobs::JobBeginInput;
use cloud_backup_protocol::{bytes_sha256, DecimalU64, Id, NativeRoot, Version};
use laststore::disk_pack::backup::export::SnapshotExport;
use laststore::disk_pack::lifetime::{DurableReference, ReferencePurpose};
use laststore::disk_pack::Current;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

impl PackRuntime {
    /// Select a complete native cut and durable reference before the first
    /// network operation. Hashing and catalogue output run after gate release.
    pub async fn prepare_native_cloud_export(
        self: &Arc<Self>,
        request: NativeCloudExportRequest,
    ) -> StorageResult<NativeCloudExport> {
        if super::super::transaction::PendingBatch::current(self).is_some() {
            return Err(invalid(
                "native export cannot capture inside a pending mutation",
            ));
        }
        request.job.validate().map_err(protocol)?;
        request
            .recovery
            .validate_for(&request.job.stream.scope)
            .map_err(protocol)?;
        let admission = Admission::acquire()?;
        let gate = self.enter_write().await;
        let runtime = Arc::clone(self);
        super::super::super::laststore::LastStoreKvStore::run_blocking(move || {
            runtime.require_initialized()?;
            if runtime.failed.load(Ordering::Acquire) {
                return Err(invalid("native export requires complete writer recovery"));
            }
            if request.job.stream.source_home.as_str()
                != runtime.home.descriptor().identity.to_string()
            {
                return Err(invalid("native export stream has a different source home"));
            }
            let store = LocalStore::open(runtime.home.path())?;
            if let Some(previous) = store.load()? {
                if !matches!(
                    previous.phase,
                    ExportPhase::Terminal {
                        source_released: true,
                        ..
                    }
                ) {
                    return Err(invalid(
                        "saved native export requires resume or terminal confirmation",
                    ));
                }
                store.cleanup_pages(&previous)?;
                if previous.begin.job == request.job {
                    return Err(invalid("terminal native export identity cannot be reused"));
                }
            }
            let writer = runtime
                .journal
                .lock()
                .map_err(|_| invalid("native export journal lock failed"))?;
            let current = writer.current().clone();
            let begin = JobBeginInput {
                version: Version::V1,
                job: request.job,
                native_root: root(&runtime, &current)?,
                recovery: request.recovery,
                predecessor: request.predecessor,
            };
            begin.validate().map_err(protocol)?;
            let record = ExportRecord {
                format: 1,
                begin,
                reference_id: uuid::Uuid::new_v4(),
                current_bytes: current.encode().map_err(super::super::map)?.to_vec(),
                phase: ExportPhase::Preparing,
                pages: Vec::new(),
                native_files: 0,
                native_bytes: 0,
                spool_bytes: 0,
            };
            let _space = writer
                .reserve_space(512 * 1024, &runtime.space_policy)
                .map_err(super::super::map)?;
            // The named hold identity and exact cut precede reference creation.
            // A crash cannot leave an unowned durable source hold.
            store.save(&record)?;
            let reference = retain(&runtime, &record)?;
            drop(writer);
            drop(_space);
            drop(gate);
            finish(runtime, store, record, Some(reference), admission)
        })
        .await
    }

    /// Resume only the saved cut. Never replace it with the current live root.
    pub async fn resume_native_cloud_export(self: &Arc<Self>) -> StorageResult<NativeCloudExport> {
        let admission = Admission::acquire()?;
        let runtime = Arc::clone(self);
        super::super::super::laststore::LastStoreKvStore::run_blocking(move || {
            runtime.require_initialized()?;
            let store = LocalStore::open(runtime.home.path())?;
            let record = store
                .load()?
                .ok_or_else(|| invalid("no saved native cloud export exists"))?;
            // A missing source must still allow status and exact remote close.
            // The frozen begin identity is sufficient for that recovery path.
            if record.begin.job.stream.source_home.as_str()
                != runtime.home.descriptor().identity.to_string()
            {
                return Err(invalid("saved native export belongs to another home"));
            }
            let resumed = resume_source(&runtime, &store, &record);
            let (record, source, source_error) = match resumed {
                Ok((record, source)) => (record, source, None),
                Err(error) => (
                    store
                        .load()?
                        .ok_or_else(|| invalid("saved native export disappeared"))?,
                    None,
                    Some(error.to_string()),
                ),
            };
            assemble(runtime, store, record, source, source_error, admission)
        })
        .await
    }
}

fn finish(
    runtime: Arc<PackRuntime>,
    store: LocalStore,
    mut record: ExportRecord,
    reference: Option<DurableReference>,
    admission: Admission,
) -> StorageResult<NativeCloudExport> {
    let source = reference
        .map(SnapshotExport::open)
        .transpose()
        .map_err(super::super::map)?;
    if let Some(source) = &source {
        if matches!(record.phase, ExportPhase::Preparing) {
            catalog::build(&runtime, source, &store, &mut record)?;
        }
        validate_inventory(&store, &record, source)?;
    }
    assemble(runtime, store, record, source, None, admission)
}

fn resume_source(
    runtime: &PackRuntime,
    store: &LocalStore,
    record: &ExportRecord,
) -> StorageResult<(ExportRecord, Option<SnapshotExport>)> {
    let mut record = record.clone();
    validate_cut(runtime, &record)?;
    let reference = match record.phase {
        ExportPhase::Terminal { .. } => return Ok((record, None)),
        ExportPhase::Preparing => retain(runtime, &record)?,
        ExportPhase::Ready => DurableReference::open(runtime.home.path(), record.reference_id)
            .map_err(super::super::map)?,
    };
    let source = SnapshotExport::open(reference).map_err(super::super::map)?;
    if matches!(record.phase, ExportPhase::Preparing) {
        catalog::build(runtime, &source, store, &mut record)?;
    }
    validate_inventory(store, &record, &source)?;
    Ok((record, Some(source)))
}

fn assemble(
    runtime: Arc<PackRuntime>,
    store: LocalStore,
    record: ExportRecord,
    source: Option<SnapshotExport>,
    source_error: Option<String>,
    admission: Admission,
) -> StorageResult<NativeCloudExport> {
    let export = NativeCloudExport {
        inner: Arc::new(ExportInner {
            runtime,
            store,
            record: Mutex::new(record),
            source,
            source_error,
            readers: AtomicUsize::new(0),
            _admission: admission,
        }),
    };
    export.finish_terminal_release()?;
    Ok(export)
}

fn retain(runtime: &PackRuntime, record: &ExportRecord) -> StorageResult<DurableReference> {
    let current = validate_cut(runtime, record)?;
    DurableReference::create_identified(
        runtime.home.path(),
        &current,
        ReferencePurpose::CloudRecovery,
        0,
        record.reference_id,
    )
    .map_err(super::super::map)
}

fn validate_cut(runtime: &PackRuntime, record: &ExportRecord) -> StorageResult<Current> {
    record.validate()?;
    let bytes: &[u8; 4096] = record
        .current_bytes
        .as_slice()
        .try_into()
        .map_err(|_| invalid("native export CURRENT size"))?;
    let current = Current::decode(bytes).map_err(super::super::map)?;
    if record.begin.native_root != root(runtime, &current)? {
        return Err(invalid("saved native export cut changed"));
    }
    Ok(current)
}

fn root(runtime: &PackRuntime, current: &Current) -> StorageResult<NativeRoot> {
    let root = NativeRoot {
        home_id: Id::try_from(runtime.home.descriptor().identity.to_string()).map_err(protocol)?,
        home_digest: hex(current.home_digest),
        current_sha256: bytes_sha256(&current.encode().map_err(super::super::map)?),
        generation: DecimalU64::new(current.generation),
        frontier: DecimalU64::new(current.frontier),
        journal_frontier: DecimalU64::new(current.journal_frontier),
        required_features: DecimalU64::new(current.required_features),
    };
    root.validate().map_err(protocol)?;
    if current.home_digest
        != runtime
            .view
            .read()
            .map_err(|_| invalid("native export view lock failed"))?
            .current
            .home_digest
    {
        return Err(invalid(
            "native export home descriptor differs from its source",
        ));
    }
    Ok(root)
}

fn validate_inventory(
    store: &LocalStore,
    record: &ExportRecord,
    source: &SnapshotExport,
) -> StorageResult<()> {
    let mut expected = source.files().iter();
    for descriptor in &record.pages {
        let page = store.read_page(&record.begin.job, descriptor)?;
        for object in page.objects {
            for file in object.files {
                let native = expected
                    .next()
                    .ok_or_else(|| invalid("native export catalogue has an extra path"))?;
                if native.path.to_str() != Some(file.path.as_str())
                    || native.bytes != file.bytes.get()
                {
                    return Err(invalid(
                        "native export catalogue differs from held inventory",
                    ));
                }
            }
        }
    }
    if expected.next().is_some()
        || record.native_bytes != source.bytes()
        || record.native_files != source.files().len() as u64
    {
        return Err(invalid("native export catalogue omits a held native file"));
    }
    Ok(())
}
