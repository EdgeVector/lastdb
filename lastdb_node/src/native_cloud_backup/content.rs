use super::{transfer, worker::Work, BackupError, Phase, Result};
use cloud_backup_protocol::transfer::ObjectReceipt;
use fold_db::storage::disk_pack::backup_capture::FileCoverage;
use fold_db::storage::disk_pack::backup_manifest::ManifestAssembly;

pub(super) async fn native(work: &mut Work<'_>, assembly: &mut ManifestAssembly<'_>) -> Result<()> {
    work.checkpoint(Phase::UploadingNative)?;
    let pages = work.capture.status()?.page_count;
    let mut ordinal = 0u64;
    let mut metadata_packs = 0u64;
    let mut multi_file_packs = 0u64;
    for page_index in 0..pages {
        let page = work.capture.page(page_index as u32)?;
        for (batch_index, objects) in page
            .objects
            .chunks(cloud_backup_protocol::transfer::MAX_BATCH_ITEMS)
            .enumerate()
        {
            work.budget(assembly.spool_bytes())?;
            work.check_close()?;
            let mut missing = Vec::new();
            let mut positions = Vec::new();
            for (index, object) in objects.iter().enumerate() {
                if let Some(view) = assembly.native_transfer(ordinal + index as u64)? {
                    assembly.feed_native(ordinal + index as u64, object, &view)?;
                } else {
                    missing.push(object.input.clone());
                    positions.push(index);
                }
            }
            if !missing.is_empty() {
                let views = transfer::batch(work, missing, |index| {
                    Ok(Box::new(work.capture.open_object(
                        page_index as u32,
                        batch_index * cloud_backup_protocol::transfer::MAX_BATCH_ITEMS
                            + positions[index],
                    )?))
                })
                .await?;
                for (position, view) in positions.into_iter().zip(views) {
                    assembly.feed_native(ordinal + position as u64, &objects[position], &view)?;
                }
            }
            ordinal += objects.len() as u64;
            for object in objects {
                if object.input.object.kind == cloud_backup_protocol::ObjectKind::MetadataPack {
                    metadata_packs += 1;
                    multi_file_packs += u64::from(object.files.len() > 1);
                }
            }
            work.saved.native_objects_verified = ordinal;
            work.saved.metadata_pack_objects_verified = Some(metadata_packs);
            work.saved.multi_file_pack_objects_verified = Some(multi_file_packs);
            work.budget(assembly.spool_bytes())?;
            work.store.save(work.saved)?;
        }
    }
    Ok(())
}

pub(super) async fn external(
    work: &mut Work<'_>,
    assembly: &mut ManifestAssembly<'_>,
) -> Result<()> {
    work.checkpoint(Phase::CopyingFiles)?;
    let entries = work.capture.file_dependency_status()?.entries;
    let scope = work
        .saved
        .begin
        .as_ref()
        .ok_or(BackupError::Invalid)?
        .job
        .stream
        .scope
        .clone();
    let source = work.client.authorized_file_source(scope);
    let mut confirmed = 0;
    for index in 0..entries {
        work.check_close()?;
        work.budget(assembly.spool_bytes())?;
        let entry = work.capture.file_dependency(index)?;
        if matches!(entry.coverage, FileCoverage::NativeCovered { .. }) {
            continue;
        }
        // A saved frozen source can need exact reconstruction after process loss.
        let entry = work
            .capture
            .prepare_external_file(index, &work.source.key, Some(&source))
            .await?;
        let parts = match entry.coverage {
            FileCoverage::BackupOwned { .. } => 1,
            FileCoverage::BackupOwnedExtents { parts, .. } => parts,
            _ => return Err(BackupError::Source),
        };
        for start in (0..parts).step_by(cloud_backup_protocol::transfer::MAX_BATCH_ITEMS) {
            let mut missing = Vec::new();
            let mut numbers = Vec::new();
            for part in
                start..parts.min(start + cloud_backup_protocol::transfer::MAX_BATCH_ITEMS as u16)
            {
                let selected = work.capture.file_part(index, part)?;
                if selected.receipt.is_none() {
                    missing.push(selected.input);
                    numbers.push(part);
                }
            }
            if missing.is_empty() {
                continue;
            }
            let views = transfer::batch(work, missing, |position| {
                Ok(Box::new(
                    work.capture.open_file_part(index, numbers[position])?,
                ))
            })
            .await?;
            for (part, view) in numbers.into_iter().zip(views) {
                work.capture.confirm_file_part(index, part, &view)?;
            }
            work.budget(assembly.spool_bytes())?;
        }
        let dependency = work
            .capture
            .external_file_dependency(index)?
            .ok_or(BackupError::Invalid)?;
        let mut receipts: Vec<ObjectReceipt> = Vec::with_capacity(parts as usize);
        for part in 0..parts {
            receipts.push(
                work.capture
                    .file_part(index, part)?
                    .receipt
                    .ok_or(BackupError::Invalid)?,
            );
        }
        assembly.feed_external(index, &dependency, &receipts)?;
        confirmed += 1;
        work.saved.external_files_verified = confirmed;
        work.budget(assembly.spool_bytes())?;
        work.store.save(work.saved)?;
    }
    Ok(())
}
