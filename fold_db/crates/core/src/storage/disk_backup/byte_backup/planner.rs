//! The snapshot plan over the frozen run files. The shared planner
//! (`cloud_backup_plan`) cuts the objects; this file keeps them in the job
//! store, and `metadata` adds the client pages and root. No network authority.
use super::{
    invalid,
    model::{ObjectRecord, Segment, State},
    protocol, Inner, StorageError, StorageResult,
};
use cloud_backup_plan::{PlanError, PlanFile, PlannedObject};
use cloud_backup_protocol::v2::{
    transfer::{ObjectReceipt, TransferInput},
    Object, Version,
};
use cloud_backup_protocol::{Id, RelativePath};
use laststore::disk_run::home::SnapshotFile;

pub(super) mod metadata;

pub(super) fn build(inner: &Inner, state: &mut State, key: &[u8; 32]) -> StorageResult<()> {
    let inventory = inner.files()?;
    state.native_files = inventory.len() as u64;
    state.native_bytes = inventory
        .iter()
        .try_fold(0u64, |n, f| n.checked_add(f.bytes))
        .ok_or_else(|| invalid("byte file size overflow"))?;
    state.metadata_pack_objects = 0;
    state.multi_file_pack_objects = 0;
    inner.store.write("job.json", state)?;
    let files = inventory
        .iter()
        .enumerate()
        .map(|(id, file)| {
            Ok(PlanFile {
                id,
                path: native_relative_path(file)?,
                bytes: file.bytes,
                destination: inner.destination(&file.path)?,
            })
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let scope = state.begin.job.stream.scope.clone();
    let totals = cloud_backup_plan::plan(
        &files,
        &scope,
        |file| inner.file_reader(&inventory[file.id].path),
        |index, planned| {
            state.metadata_pack_objects += u64::from(planned.pack);
            state.multi_file_pack_objects += u64::from(planned.pack && planned.segments.len() > 1);
            let object = record(inner, state, index, &files, planned)?;
            save(inner, state, index, object)
        },
    )?;
    metadata::finish(inner, state, key, totals.objects)?;
    state.ready = true;
    state.validate()?;
    inner.store.write("job.json", state)
}

/// Both planners use one relative UTF-8 path.
fn native_relative_path(file: &SnapshotFile) -> StorageResult<RelativePath> {
    RelativePath::try_from(
        file.path
            .to_str()
            .ok_or_else(|| invalid("byte native UTF8 path"))?
            .to_owned(),
    )
    .map_err(protocol)
}

/// A snapshot segment names its file by path; the plan names it by id.
fn record(
    inner: &Inner,
    state: &State,
    index: u64,
    files: &[PlanFile],
    planned: PlannedObject,
) -> StorageResult<ObjectRecord> {
    let segments = planned
        .segments
        .iter()
        .map(|part| Segment {
            path: files[part.file].path.clone(),
            offset: part.offset,
            bytes: part.bytes,
        })
        .collect();
    Ok(ObjectRecord {
        input: input(inner, state, index, planned.object)?,
        segments,
        native_files: planned
            .native_files
            .into_iter()
            .map(|(_, file)| file)
            .collect(),
        metadata_pack: planned.pack,
        ciphertext: None,
        receipt: None,
    })
}
pub(super) fn input(
    inner: &Inner,
    state: &State,
    index: u64,
    object: Object,
) -> StorageResult<TransferInput> {
    if index >= super::model::MAX_OBJECTS {
        return Err(invalid("byte source object count bound"));
    }
    if inner.store.exists(&super::model::object_name(index))? {
        let prior: ObjectRecord = inner.store.read(&super::model::object_name(index))?;
        prior.validate(&state.begin)?;
        if prior.input.object != object {
            return Err(invalid("byte frozen object bytes differ"));
        }
        return Ok(prior.input);
    }
    let input = TransferInput {
        version: Version::V2,
        request_id: Id::try_from(uuid::Uuid::new_v4().to_string()).map_err(protocol)?,
        job: state.begin.job.clone(),
        object,
    };
    input.validate().map_err(protocol)?;
    Ok(input)
}
pub(super) fn save(
    inner: &Inner,
    state: &mut State,
    index: u64,
    mut object: ObjectRecord,
) -> StorageResult<()> {
    let name = super::model::object_name(index);
    object.validate(&state.begin)?;
    if inner.store.exists(&name)? {
        let prior: ObjectRecord = inner.store.read(&name)?;
        prior.validate(&state.begin)?;
        if prior.input != object.input
            || prior.segments != object.segments
            || prior.native_files != object.native_files
            || prior.metadata_pack != object.metadata_pack
            || prior.ciphertext != object.ciphertext
        {
            return Err(invalid("byte frozen object plan differs"));
        }
        object.receipt = prior.receipt;
    }
    inner.store.write(&name, &object)?;
    state.objects = state.objects.max(index + 1);
    inner.store.write("job.json", state)
}
pub(super) fn expected_receipt(input: &TransferInput) -> StorageResult<ObjectReceipt> {
    cloud_backup_plan::expected_receipt(input).map_err(protocol)
}

impl From<PlanError> for StorageError {
    fn from(error: PlanError) -> Self {
        match error {
            PlanError::Invalid(reason) => invalid(reason),
            PlanError::SourceLength => invalid("byte native source length changed"),
            PlanError::ExtentBound => invalid("byte native file extent metadata bound"),
            PlanError::Protocol(error) => protocol(error),
            PlanError::Io(error) => Self::from(error),
        }
    }
}
