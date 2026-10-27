use super::*;
use crate::storage::disk_pack::byte_backup::{codec, ClientPage, ClientPageReference, ClientRoot};

pub(super) fn finish(
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
        external_dependencies: Vec::new(),
    };
    let mut files = 0u64;
    let mut bytes = 0u64;
    for ordinal in 0..content {
        let object: ObjectRecord = inner
            .store
            .read(&super::super::model::object_name(ordinal))?;
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
        recovery: state
            .recovery
            .clone()
            .ok_or_else(|| invalid("run recovery missing"))?,
        native_file_count: files,
        native_bytes: bytes,
        external_dependency_count: 0,
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
pub(in crate::storage::disk_pack::byte_backup) fn freeze_page(
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
pub(in crate::storage::disk_pack::byte_backup) fn freeze(
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
    let input = input(
        inner,
        state,
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
        native_files: Vec::new(),
        metadata_pack: false,
        ciphertext: Some(name),
        receipt: None,
    };
    save(inner, state, index, record.clone())?;
    Ok(record)
}
