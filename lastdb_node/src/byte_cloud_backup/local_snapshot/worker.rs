//! The ordinary native capture, encrypted root, and exact local cancellation.

use super::*;
use fold_db::storage::disk_pack::byte_backup::{
    ByteBackupCapture, ClientRoot, CODEC_ENVELOPE_MAX_BYTES,
};
use std::io::Read;
use std::sync::atomic::{AtomicBool, Ordering};

pub(super) async fn run(mut inputs: Inputs, pause: &AtomicBool) -> Result<LocalSnapshotReport> {
    if inputs.intent.released {
        return Ok(released(&inputs.intent.begin.job.commit_id));
    }
    if pause.load(Ordering::Acquire) {
        return Err(BackupError::Shutdown);
    }
    if matches!(inputs.action, LocalSnapshotAction::Release) {
        inputs
            .runtime
            .cancel_unsubmitted_byte_backup_capture(&inputs.intent.begin, &inputs.source.key)
            .await?;
        inputs.intent.released = true;
        inputs.intent.save(&inputs.home)?;
        return Ok(released(&inputs.intent.begin.job.commit_id));
    }
    let capture = if matches!(inputs.action, LocalSnapshotAction::Prepare) {
        inputs
            .runtime
            .prepare_unsubmitted_record_backup_capture(
                inputs.intent.begin.clone(),
                &inputs.source.key,
                inputs.source.recovery.clone(),
            )
            .await?
    } else {
        inputs
            .runtime
            .resume_byte_backup_capture(&inputs.source.key)
            .await?
    };
    if capture.begin()? != inputs.intent.begin {
        return Err(BackupError::Identity);
    }
    let mut report = report(&capture, &inputs)?;
    if let Some(candidate) = &inputs.candidate {
        report.adoption_plan = Some(capture.snapshot_adoption_plan(candidate)?);
    }
    Ok(report)
}

fn report(capture: &ByteBackupCapture, inputs: &Inputs) -> Result<LocalSnapshotReport> {
    let index = capture
        .object_count()?
        .checked_sub(1)
        .ok_or(BackupError::Invalid)?;
    let object = capture.object(index)?;
    if object.input.object.bytes.get() > CODEC_ENVELOPE_MAX_BYTES {
        return Err(BackupError::Limit);
    }
    let mut encrypted = Vec::new();
    capture
        .reader(index)?
        .take(CODEC_ENVELOPE_MAX_BYTES + 1)
        .read_to_end(&mut encrypted)?;
    if encrypted.len() as u64 != object.input.object.bytes.get() {
        return Err(BackupError::Invalid);
    }
    let root = ClientRoot::decode(&encrypted, &inputs.source.key, &inputs.intent.begin)?;
    let inclusion = capture.logical_inclusion()?;
    let profile = capture.run_profile()?;
    if root.logical_inclusion != inclusion || root.run.as_ref() != Some(&profile) {
        return Err(BackupError::Identity);
    }
    Ok(LocalSnapshotReport {
        request_id: inputs.intent.begin.job.commit_id.clone(),
        released: false,
        remote_submitted: false,
        root_object_index: Some(index),
        root_ciphertext_sha256: Some(object.input.object.sha256),
        root_ciphertext_bytes: Some(object.input.object.bytes.get()),
        native_root: Some(root.native_root),
        run_profile: Some(profile),
        logical_inclusion: inclusion,
        adoption_plan: None,
    })
}

fn released(id: &Id) -> LocalSnapshotReport {
    LocalSnapshotReport {
        request_id: id.clone(),
        released: true,
        remote_submitted: false,
        root_object_index: None,
        root_ciphertext_sha256: None,
        root_ciphertext_bytes: None,
        native_root: None,
        run_profile: None,
        logical_inclusion: None,
        adoption_plan: None,
    }
}
