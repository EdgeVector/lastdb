//! The mutation log plan. The shared planner (`cloud_backup_plan`) cuts the
//! objects in path order; this file keeps them and the file records in the
//! job store.
use super::*;
use cloud_backup_plan::{Chunk, PlanFile, PlanState, Range, Step};
use cloud_backup_protocol::v2::{transfer::TransferInput, Object, Version};
use cloud_backup_protocol::Id;
use fold_db::storage::disk_pack::mutation_log::MutationLogReader;
use model::{destination, ObjectRecord, Segment};
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
    metadata::finish(capture, recovery, key, totals.objects)?;
    capture.plan.ready = true;
    capture.store.write("log-plan.json", &capture.plan)
}

fn drive(
    capture: &mut LogCapture,
    files: &[PlanFile],
    scope: &cloud_backup_protocol::CloudScope,
) -> Result<cloud_backup_plan::PlanTotals> {
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
                let len = read_range(&mut source, capture, files, &range, &mut buffer)?;
                pending = Some((range.file, range.offset, len));
            }
            Step::Save { index, object } => {
                let request_id = Id::try_from(uuid::Uuid::new_v4().to_string())?;
                let input = input(capture, index, object.object, request_id)?;
                for (id, file) in object.native_files {
                    capture.store.write(&model::file_name(id), &file)?;
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

pub(super) fn input(
    capture: &LogCapture,
    index: u64,
    object: Object,
    request_id: Id,
) -> Result<TransferInput> {
    if index >= model::MAX_OBJECTS {
        return Err(BackupError::Limit);
    }
    if let Some(prior) = capture
        .store
        .read::<ObjectRecord>(&model::object_name(index))?
    {
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
    };
    input.validate()?;
    Ok(input)
}
pub(super) fn save(capture: &mut LogCapture, index: u64, mut record: ObjectRecord) -> Result<()> {
    record.validate(&capture.frozen, index)?;
    if let Some(prior) = capture
        .store
        .read::<ObjectRecord>(&model::object_name(index))?
    {
        prior.validate(&capture.frozen, index)?;
        record.receipt = prior.receipt.clone();
        if prior != record {
            return Err(BackupError::Identity);
        }
    }
    capture.store.write(&model::object_name(index), &record)?;
    capture.plan.objects = capture.plan.objects.max(index + 1);
    capture.budget(capture.store.bytes())?;
    capture.store.write("log-plan.json", &capture.plan)
}
