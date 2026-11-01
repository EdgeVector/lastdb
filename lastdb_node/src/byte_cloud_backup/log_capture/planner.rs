//! The mutation log plan. The shared planner (`cloud_backup_plan`) cuts the
//! objects in path order; this file keeps them and the file records in the
//! job store.
use super::*;
use cloud_backup_plan::PlanFile;
use cloud_backup_protocol::v2::{transfer::TransferInput, Object, Version};
use cloud_backup_protocol::Id;
use model::{destination, ObjectRecord, Segment};

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
            destination: Some(destination(file)),
        })
        .collect();
    files.sort_by(|a, b| a.path.as_str().cmp(b.path.as_str()));
    let core = Arc::clone(&capture.core);
    let scope = capture.frozen.begin.job.stream.scope.clone();
    let totals = cloud_backup_plan::plan(
        &files,
        &scope,
        |file| {
            core.reader(file.id, 0, file.bytes)
                .map_err(BackupError::from)
        },
        |index, planned| {
            let input = input(capture, index, planned.object)?;
            for (id, file) in planned.native_files {
                capture.store.write(&model::file_name(id), &file)?;
            }
            let segments = planned
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
            )
        },
    )?;
    capture.plan.packs = totals.packs;
    capture.plan.multi_file_packs = totals.multi_file_packs;
    metadata::finish(capture, recovery, key, totals.objects)?;
    capture.plan.ready = true;
    capture.store.write("log-plan.json", &capture.plan)
}

pub(super) fn input(capture: &LogCapture, index: u64, object: Object) -> Result<TransferInput> {
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
        request_id: Id::try_from(uuid::Uuid::new_v4().to_string())?,
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
