use super::*;
use crate::byte_cloud_log::{LogKind, MutationLogPage, MutationLogRoot, VERSION};
use cloud_backup_protocol::v2::Object;
use cloud_backup_protocol::DecimalU64;
use fold_db::storage::disk_pack::byte_backup::{
    ClientNativeFile, ClientPageReference, MAX_PAGE_FILES,
};

pub(super) fn finish(
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
            .read(&model::file_name(file_index))?
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
        receipt_id: model::expected(&object.input)?.receipt_id,
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
    let name = model::payload_name(index);
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
    let input = planner::input(
        capture,
        index,
        Object {
            sha256: bytes_sha256(&encrypted),
            bytes: DecimalU64::new(encrypted.len() as u64),
            destination: Some(cloud_backup_protocol::v2::Destination::R2),
        },
    )?;
    let record = ObjectRecord {
        input,
        segments: Vec::new(),
        payload: Some(name),
        receipt: None,
    };
    planner::save(capture, index, record.clone())?;
    Ok(record)
}
