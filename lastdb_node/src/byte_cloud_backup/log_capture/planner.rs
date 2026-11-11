//! The mutation log plan and encrypted metadata page/root freezing.
use super::*;
use crate::byte_cloud_log::{LogKind, MutationLogPage, MutationLogRoot, VERSION};
use cloud_backup_protocol::v2::{transfer::TransferInput, Destination, Object, Version};
use cloud_backup_protocol::{DecimalU64, Id};
use fold_db::cloud_backup_plan::{Chunk, ClientNativeFile, PlanFile, PlanState, Range, Step};
use fold_db::storage::disk_pack::byte_backup::{ClientPageReference, MAX_PAGE_FILES};
use fold_db::storage::disk_pack::mutation_log::MutationLogReader;
use std::io::Read;

pub(super) fn build(
    capture: &mut LogCapture,
    recovery: &RecoveryDescriptor,
    key: &[u8; 32],
) -> Result<()> {
    let mut files: Vec<_> = capture
        .frozen
        .files
        .iter()
        .enumerate()
        .map(|(id, file)| PlanFile {
            id,
            path: file.path.clone(),
            bytes: file.bytes,
            destination: destination(file),
        })
        .collect();
    files.sort_by(|a, b| a.path.as_str().cmp(b.path.as_str()));
    let scope = capture.frozen.begin.job.stream.scope.clone();
    let totals = drive(capture, &files, &scope)?;
    capture.plan.packs = totals.packs;
    capture.plan.multi_file_packs = totals.multi_file_packs;
    finish(capture, recovery, key, totals.objects)?;
    capture.plan.ready = true;
    capture.store.write("log-plan.json", &capture.plan)
}

fn drive(
    capture: &mut LogCapture,
    files: &[PlanFile],
    scope: &cloud_backup_protocol::CloudScope,
) -> Result<fold_db::cloud_backup_plan::PlanTotals> {
    let mut plan = PlanState::start(files, scope);
    let mut source = None;
    let mut buffer = [0u8; fold_db::cloud_backup_plan::READ_BYTES as usize];
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
                let len = read_range(&mut source, capture, files, &range, &mut buffer)?;
                pending = Some((range.file, range.offset, len));
            }
            Step::Save { index, object } => {
                let request_id = Id::try_from(uuid::Uuid::new_v4().to_string())?;
                let input = input(capture, index, object.object, request_id)?;
                for (id, file) in object.native_files {
                    capture.store.write(&file_name(id), &file)?;
                }
                let segments = object
                    .segments
                    .into_iter()
                    .map(|part| Segment {
                        file: part.file,
                        offset: part.offset,
                        bytes: part.bytes,
                    })
                    .collect();
                save(
                    capture,
                    index,
                    ObjectRecord {
                        input,
                        segments,
                        payload: None,
                        receipt: None,
                    },
                )?;
            }
        }
    }
}

struct OpenFile {
    id: usize,
    reader: MutationLogReader,
    pos: u64,
}

fn read_range(
    open: &mut Option<OpenFile>,
    capture: &LogCapture,
    files: &[PlanFile],
    range: &Range,
    buffer: &mut [u8],
) -> Result<usize> {
    if range.bytes > buffer.len() as u64 {
        return Err(BackupError::Invalid);
    }
    let want = range.bytes as usize;
    if open.as_ref().is_none_or(|source| source.id != range.file) {
        let declared = files
            .iter()
            .find(|file| file.id == range.file)
            .ok_or(BackupError::Invalid)?
            .bytes;
        *open = Some(OpenFile {
            id: range.file,
            reader: capture.core.reader(range.file, 0, declared)?,
            pos: 0,
        });
    }
    let Some(source) = open.as_mut() else {
        return Err(BackupError::Invalid);
    };
    if source.pos != range.offset {
        return Err(BackupError::Invalid);
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

fn read_upto(reader: &mut impl Read, buffer: &mut [u8]) -> Result<usize> {
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

fn input(
    capture: &LogCapture,
    index: u64,
    object: Object,
    request_id: Id,
) -> Result<TransferInput> {
    if index >= MAX_OBJECTS {
        return Err(BackupError::Limit);
    }
    if let Some(prior) = capture.store.read::<ObjectRecord>(&object_name(index))? {
        prior.validate(&capture.frozen, index)?;
        if prior.input.object != object {
            return Err(BackupError::Identity);
        }
        return Ok(prior.input);
    }
    let input = TransferInput {
        version: Version::V2,
        request_id,
        job: capture.frozen.begin.job.clone(),
        object,
        blob: None,
    };
    input.validate()?;
    Ok(input)
}

fn save(capture: &mut LogCapture, index: u64, mut record: ObjectRecord) -> Result<()> {
    record.validate(&capture.frozen, index)?;
    if let Some(prior) = capture.store.read::<ObjectRecord>(&object_name(index))? {
        prior.validate(&capture.frozen, index)?;
        record.receipt = prior.receipt.clone();
        if prior != record {
            return Err(BackupError::Identity);
        }
    }
    capture.store.write(&object_name(index), &record)?;
    capture.plan.objects = capture.plan.objects.max(index + 1);
    capture.budget(capture.store.bytes())?;
    capture.store.write("log-plan.json", &capture.plan)
}

fn finish(
    capture: &mut LogCapture,
    recovery: &RecoveryDescriptor,
    key: &[u8; 32],
    mut index: u64,
) -> Result<()> {
    let mut order: Vec<_> = (0..capture.frozen.files.len()).collect();
    order.sort_by(|a, b| {
        capture.frozen.files[*a]
            .path
            .as_str()
            .cmp(capture.frozen.files[*b].path.as_str())
    });
    let mut page = MutationLogPage {
        version: VERSION,
        page_index: 0,
        begin_sha256: capture.frozen.begin.input_sha256()?,
        descriptor_sha256: capture.frozen.begin.descriptor.commitment()?,
        files: Vec::new(),
    };
    let mut pages = Vec::new();
    for file_index in order {
        let file: ClientNativeFile = capture
            .store
            .read(&file_name(file_index))?
            .ok_or(BackupError::Missing)?;
        if file.path != capture.frozen.files[file_index].path {
            return Err(BackupError::Identity);
        }
        if page.files.len() == MAX_PAGE_FILES {
            pages.push(freeze_page(capture, key, index, &page)?);
            index += 1;
            page.page_index += 1;
            page.files.clear();
        }
        page.files.push(file);
    }
    if !page.files.is_empty() {
        pages.push(freeze_page(capture, key, index, &page)?);
        index += 1;
    }
    let root = MutationLogRoot {
        version: VERSION,
        kind: LogKind::NativeMutationLog,
        begin_sha256: page.begin_sha256,
        descriptor_sha256: page.descriptor_sha256,
        recovery: recovery.clone(),
        base: capture.plan.context.base.clone(),
        previous: capture.plan.context.previous.clone(),
        anchor: capture.frozen.anchor.clone(),
        from: capture.frozen.from.clone(),
        through: capture.frozen.through.clone(),
        entries: capture.frozen.entries.clone(),
        native_file_count: capture.plan.files,
        native_bytes: capture.plan.native_bytes,
        pages,
    };
    root.validate_for(&capture.frozen.begin)
        .map_err(|_| BackupError::Invalid)?;
    freeze(capture, key, index, &root)?;
    if capture.plan.objects != index + 1 {
        return Err(BackupError::Identity);
    }
    Ok(())
}

fn freeze_page(
    capture: &mut LogCapture,
    key: &[u8; 32],
    index: u64,
    page: &MutationLogPage,
) -> Result<ClientPageReference> {
    page.validate_for(&capture.frozen.begin)
        .map_err(|_| BackupError::Invalid)?;
    let object = freeze(capture, key, index, page)?;
    Ok(ClientPageReference {
        page_index: page.page_index,
        receipt_id: expected(&object.input)?.receipt_id,
        sha256: object.input.object.sha256,
        bytes: object.input.object.bytes,
    })
}

fn freeze(
    capture: &mut LogCapture,
    key: &[u8; 32],
    index: u64,
    value: &impl serde::Serialize,
) -> Result<ObjectRecord> {
    let name = payload_name(index);
    let plain =
        zeroize::Zeroizing::new(serde_json::to_vec(value).map_err(|_| BackupError::Invalid)?);
    if plain.len() as u64 > fold_db::storage::disk_pack::byte_backup::CODEC_PLAINTEXT_MAX_BYTES {
        return Err(BackupError::Limit);
    }
    let encrypted = if let Some(bytes) = capture.store.raw(&name)? {
        let old = zeroize::Zeroizing::new(
            fold_db::crypto::decrypt_envelope(key, &bytes).map_err(|_| BackupError::Identity)?,
        );
        if old.as_slice() != plain.as_slice() {
            return Err(BackupError::Identity);
        }
        bytes
    } else {
        let bytes = crate::byte_cloud_log::encode(value, key).map_err(|_| BackupError::Invalid)?;
        capture.store.write_raw(&name, &bytes)?;
        bytes
    };
    let request_id = Id::try_from(uuid::Uuid::new_v4().to_string())?;
    let input = input(
        capture,
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
        payload: Some(name),
        receipt: None,
    };
    save(capture, index, record.clone())?;
    Ok(record)
}
