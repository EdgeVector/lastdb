//! Snapshot plan over frozen run files and client page/root metadata.

use super::{
    codec, invalid, object_name, protocol, ClientPage, ClientPageReference, ClientRoot, Inner,
    ObjectRecord, Segment, State, StorageError, StorageResult, MAX_OBJECTS,
};
use crate::storage::cloud_backup_plan::{
    Chunk, PlanError, PlanFile, PlanState, PlannedObject, Range, Step,
};
use cloud_backup_protocol::v2::{
    transfer::{ObjectReceipt, TransferInput},
    Destination, Object, Version,
};
use cloud_backup_protocol::{bytes_sha256, DecimalU64, Id, RelativePath};
use laststore::disk_run::home::SnapshotFile;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

pub(in crate::storage::disk_backup::byte_backup) fn build(
    inner: &Inner,
    state: &mut State,
    key: &[u8; 32],
) -> StorageResult<()> {
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
    finish_metadata(inner, state, key, totals.objects)?;
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
) -> StorageResult<crate::storage::cloud_backup_plan::PlanTotals> {
    let mut plan = PlanState::start(files, scope);
    let mut source = None;
    let mut buffer = [0u8; crate::storage::cloud_backup_plan::READ_BYTES as usize];
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

fn native_relative_path(file: &SnapshotFile) -> StorageResult<RelativePath> {
    RelativePath::try_from(
        file.path
            .to_str()
            .ok_or_else(|| invalid("byte native UTF8 path"))?
            .to_owned(),
    )
    .map_err(protocol)
}

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

fn input(
    inner: &Inner,
    state: &State,
    index: u64,
    object: Object,
    request_id: Id,
) -> StorageResult<TransferInput> {
    if index >= MAX_OBJECTS {
        return Err(invalid("byte source object count bound"));
    }
    if inner.store.exists(&object_name(index))? {
        let prior: ObjectRecord = inner.store.read(&object_name(index))?;
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
        blob: None,
    };
    input.validate().map_err(protocol)?;
    Ok(input)
}

fn save(
    inner: &Inner,
    state: &mut State,
    index: u64,
    mut object: ObjectRecord,
) -> StorageResult<()> {
    let name = object_name(index);
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

pub(in crate::storage::disk_backup::byte_backup) fn expected_receipt(
    input: &TransferInput,
) -> StorageResult<ObjectReceipt> {
    crate::storage::cloud_backup_plan::expected_receipt(input).map_err(protocol)
}

fn finish_metadata(
    inner: &Inner,
    state: &mut State,
    key: &[u8; 32],
    content: u64,
) -> StorageResult<()> {
    let mut index = content;
    let mut pages = Vec::new();
    let mut page = ClientPage {
        version: codec::CLIENT_VERSION,
        page_index: 0,
        begin_sha256: state.begin.input_sha256().map_err(protocol)?,
        descriptor_sha256: state.begin.descriptor.commitment().map_err(protocol)?,
        native_files: Vec::new(),
    };
    let mut files = 0u64;
    let mut bytes = 0u64;
    for ordinal in 0..content {
        let object: ObjectRecord = inner.store.read(&object_name(ordinal))?;
        object.validate(&state.begin)?;
        for file in object.native_files {
            if page.native_files.len() == codec::MAX_PAGE_FILES {
                pages.push(freeze_page(inner, state, key, index, &page)?);
                index += 1;
                page.page_index += 1;
                page.native_files.clear();
            }
            files = files
                .checked_add(1)
                .ok_or_else(|| invalid("byte native count overflow"))?;
            bytes = bytes
                .checked_add(file.native_bytes.get())
                .ok_or_else(|| invalid("byte native size overflow"))?;
            page.native_files.push(file);
        }
    }
    if !page.native_files.is_empty() {
        pages.push(freeze_page(inner, state, key, index, &page)?);
        index += 1;
    }
    if files != state.native_files || bytes != state.native_bytes {
        return Err(invalid("byte exact native inventory totals"));
    }
    let root = ClientRoot {
        version: page.version,
        begin_sha256: page.begin_sha256,
        descriptor_sha256: page.descriptor_sha256,
        native_root: state.native_root.clone(),
        recovery: state.recovery.clone(),
        native_file_count: files,
        native_bytes: bytes,
        pages,
        run: state.run.clone(),
    };
    root.validate_for(&state.begin)?;
    freeze(inner, state, key, index, &codec::encode(&root)?)?;
    if state.objects != index + 1 {
        return Err(invalid("byte frozen source object total differs"));
    }
    Ok(())
}

fn freeze_page(
    inner: &Inner,
    state: &mut State,
    key: &[u8; 32],
    index: u64,
    page: &ClientPage,
) -> StorageResult<ClientPageReference> {
    page.validate_for(&state.begin)?;
    let object = freeze(inner, state, key, index, &codec::encode(page)?)?;
    Ok(ClientPageReference {
        page_index: page.page_index,
        receipt_id: expected_receipt(&object.input)?.receipt_id,
        sha256: object.input.object.sha256,
        bytes: object.input.object.bytes,
    })
}

fn freeze(
    inner: &Inner,
    state: &mut State,
    key: &[u8; 32],
    index: u64,
    plain: &[u8],
) -> StorageResult<ObjectRecord> {
    let name = format!("payload-{index:08}.enc");
    let encrypted = if inner.store.exists(&name)? {
        let bytes = inner.store.raw(&name, codec::CODEC_ENVELOPE_MAX_BYTES)?;
        let old = zeroize::Zeroizing::new(
            crate::crypto::decrypt_envelope(key, &bytes)
                .map_err(|_| invalid("byte saved metadata encryption"))?,
        );
        if old.as_slice() != plain {
            return Err(invalid("byte frozen metadata differs"));
        }
        bytes
    } else {
        let bytes = crate::crypto::encrypt_envelope(key, plain)
            .map_err(|_| invalid("byte metadata encryption"))?;
        inner.store.write_raw(&name, &bytes)?;
        bytes
    };
    let request_id = Id::try_from(uuid::Uuid::new_v4().to_string()).map_err(protocol)?;
    let input = input(
        inner,
        state,
        index,
        Object {
            sha256: bytes_sha256(&encrypted),
            bytes: DecimalU64::new(encrypted.len() as u64),
            destination: Destination::R2,
        },
        request_id,
    )?;
    let record = ObjectRecord {
        input,
        segments: Vec::new(),
        native_files: Vec::new(),
        metadata_pack: false,
        ciphertext: Some(name),
        receipt: None,
    };
    save(inner, state, index, record.clone())?;
    Ok(record)
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
