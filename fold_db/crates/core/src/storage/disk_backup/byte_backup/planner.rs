//! Snapshot plan over frozen run files and client page/root metadata.

use super::{
    codec, invalid, object_name, protocol, ClientPage, ClientPageReference, ClientRoot, Inner,
    ObjectRecord, Segment, State, StorageError, StorageResult, MAX_OBJECTS,
};
use crate::storage::cloud_backup_plan::{expected_receipt, plan_files, PlanError, PlanFile};
use cloud_backup_protocol::v2::{transfer::TransferInput, Destination, Object, Version};
use cloud_backup_protocol::{bytes_sha256, DecimalU64, Id, RelativePath};
use laststore::disk_run::home::SnapshotFile;

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
    let totals = plan_files(
        &files,
        &scope,
        |file| inner.file_reader(&inventory[file.id].path),
        |index, planned| {
            state.metadata_pack_objects += u64::from(planned.pack);
            state.multi_file_pack_objects += u64::from(planned.pack && planned.segments.len() > 1);
            let segments = planned
                .segments
                .into_iter()
                .map(|part| Segment {
                    path: files[part.file].path.clone(),
                    offset: part.offset,
                    bytes: part.bytes,
                })
                .collect();
            save(
                inner,
                state,
                index,
                ObjectRecord {
                    input: dummy_input(state, planned.object)?,
                    segments,
                    native_files: planned.native_files,
                    metadata_pack: planned.pack,
                    ciphertext: None,
                    receipt: None,
                },
            )
            .map(|_| ())
        },
    )?;
    finish_metadata(inner, state, key, totals.objects)?;
    state.ready = true;
    state.validate()?;
    inner.store.write("job.json", state)
}

fn native_relative_path(file: &SnapshotFile) -> StorageResult<RelativePath> {
    let text = file
        .path
        .to_str()
        .ok_or_else(|| invalid("byte native UTF8 path"))?;
    RelativePath::try_from(text.to_owned()).map_err(protocol)
}

fn dummy_input(state: &State, object: Object) -> StorageResult<TransferInput> {
    let input = TransferInput {
        version: Version::V2,
        request_id: Id::try_from(uuid::Uuid::new_v4().to_string()).map_err(protocol)?,
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
    mut record: ObjectRecord,
) -> StorageResult<ObjectRecord> {
    if index >= MAX_OBJECTS {
        return Err(invalid("byte source object count bound"));
    }
    let name = object_name(index);
    if inner.store.exists(&name)? {
        let prior: ObjectRecord = inner.store.read(&name)?;
        prior.validate(&state.begin)?;
        if prior.input.object != record.input.object
            || prior.segments != record.segments
            || prior.native_files != record.native_files
            || prior.metadata_pack != record.metadata_pack
            || prior.ciphertext != record.ciphertext
        {
            return Err(invalid("byte frozen object plan differs"));
        }
        record.input = prior.input;
        record.receipt = prior.receipt;
    }
    record.validate(&state.begin)?;
    inner.store.write(&name, &record)?;
    state.objects = state.objects.max(index + 1);
    inner.store.write("job.json", state)?;
    Ok(record)
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
    freeze(inner, state, key, index, &codec::encode_metadata(&root)?)?;
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
    let object = freeze(inner, state, key, index, &codec::encode_metadata(page)?)?;
    Ok(ClientPageReference {
        page_index: page.page_index,
        receipt_id: expected_receipt(&object.input)
            .map_err(protocol)?
            .receipt_id,
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
    let object = Object {
        sha256: bytes_sha256(&encrypted),
        bytes: DecimalU64::new(encrypted.len() as u64),
        destination: Destination::R2,
    };
    save(
        inner,
        state,
        index,
        ObjectRecord {
            input: dummy_input(state, object)?,
            segments: Vec::new(),
            native_files: Vec::new(),
            metadata_pack: false,
            ciphertext: Some(name),
            receipt: None,
        },
    )
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
