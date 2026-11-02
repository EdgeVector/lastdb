//! The snapshot plan over the frozen run files. The shared planner
//! (`cloud_backup_plan`) cuts the objects; this file keeps them in the job
//! store, and `metadata` adds the client pages and root. No network authority.
use super::{
    invalid,
    model::{ObjectRecord, Segment, State},
    protocol, Inner, StorageError, StorageResult,
};
use cloud_backup_plan::{Chunk, PlanError, PlanFile, PlanState, PlannedObject, Range, Step};
use cloud_backup_protocol::v2::{
    transfer::{ObjectReceipt, TransferInput},
    Object, Version,
};
use cloud_backup_protocol::{Id, RelativePath};
use laststore::disk_run::home::SnapshotFile;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

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
    let totals = drive(inner, state, &files, inventory, &scope)?;
    metadata::finish(inner, state, key, totals.objects)?;
    state.ready = true;
    state.validate()?;
    inner.store.write("job.json", state)
}

fn drive(
    inner: &Inner,
    state: &mut State,
    files: &[PlanFile],
    inventory: &[SnapshotFile],
    scope: &cloud_backup_protocol::CloudScope,
) -> StorageResult<cloud_backup_plan::PlanTotals> {
    let mut plan = PlanState::start(files, scope);
    let mut source = None;
    let mut buffer = [0u8; cloud_backup_plan::READ_BYTES as usize];
    let mut pending = None;
    loop {
        let step = match pending.take() {
            Some((file, offset, len)) => plan.step(Some(Chunk {
                file,
                offset,
                bytes: &buffer[..len],
            }))?,
            None => plan.step(None)?,
        };
        match step {
            Step::Done(totals) => return Ok(totals),
            Step::Need(range) => {
                let len = read_range(&mut source, inner, inventory, &range, &mut buffer)?;
                pending = Some((range.file, range.offset, len));
            }
            Step::Save { index, object } => {
                let request_id =
                    Id::try_from(uuid::Uuid::new_v4().to_string()).map_err(protocol)?;
                state.metadata_pack_objects += u64::from(object.pack);
                state.multi_file_pack_objects +=
                    u64::from(object.pack && object.segments.len() > 1);
                let record = record(inner, state, index, files, object, request_id)?;
                save(inner, state, index, record)?;
            }
        }
    }
}

struct OpenFile {
    id: usize,
    reader: File,
    pos: u64,
}

fn read_range(
    open: &mut Option<OpenFile>,
    inner: &Inner,
    inventory: &[SnapshotFile],
    range: &Range,
    buffer: &mut [u8],
) -> StorageResult<usize> {
    if range.bytes > buffer.len() as u64 {
        return Err(invalid("byte plan range"));
    }
    let want = range.bytes as usize;
    if open.as_ref().is_none_or(|source| source.id != range.file) {
        let path = &inventory
            .get(range.file)
            .ok_or_else(|| invalid("byte plan range"))?
            .path;
        *open = Some(OpenFile {
            id: range.file,
            reader: inner.file_reader(path)?,
            pos: 0,
        });
    }
    let Some(source) = open.as_mut() else {
        return Err(invalid("byte plan range"));
    };
    if source.pos != range.offset {
        source.reader.seek(SeekFrom::Start(range.offset))?;
        source.pos = range.offset;
    }
    let read = if range.exact {
        source.reader.read_exact(&mut buffer[..want])?;
        want
    } else {
        read_upto(&mut source.reader, &mut buffer[..want])?
    };
    source.pos += read as u64;
    Ok(read)
}

fn read_upto(reader: &mut impl Read, buffer: &mut [u8]) -> StorageResult<usize> {
    let mut filled = 0;
    while filled < buffer.len() {
        let count = reader.read(&mut buffer[filled..])?;
        if count == 0 {
            break;
        }
        filled += count;
    }
    Ok(filled)
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
    request_id: Id,
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
        input: input(inner, state, index, planned.object, request_id)?,
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
    request_id: Id,
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
        request_id,
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
        }
    }
}
