use super::super::model::State;
use super::super::{codec, invalid, model, planner, ClientPage, ClientRoot, Inner, StorageResult};

pub(super) fn metadata(
    inner: &Inner,
    state: &mut State,
    key: &[u8; 32],
    content: u64,
) -> StorageResult<()> {
    let mut index = content;
    let mut references = Vec::new();
    let mut page = ClientPage {
        version: 2,
        page_index: 0,
        begin_sha256: state.begin.input_sha256().map_err(super::super::protocol)?,
        descriptor_sha256: state
            .begin
            .descriptor
            .commitment()
            .map_err(super::super::protocol)?,
        native_files: Vec::new(),
        external_dependencies: Vec::new(),
        stored_chunks: Vec::new(),
    };
    for ordinal in 0..content {
        let object: model::ObjectRecord = inner.store.read(&model::object_name(ordinal))?;
        object.validate(&state.begin)?;
        let chunk = object
            .client_stream
            .ok_or_else(|| invalid("record stream chunk missing"))?;
        page.stored_chunks.push(chunk);
        if page.stored_chunks.len() == cloud_backup_protocol::v2::limits::MAX_PAGE_ENTRIES {
            references.push(planner::metadata::freeze_page(
                inner, state, key, index, &page,
            )?);
            index += 1;
            page.page_index += 1;
            page.stored_chunks.clear();
        }
    }
    if !page.stored_chunks.is_empty() {
        references.push(planner::metadata::freeze_page(
            inner, state, key, index, &page,
        )?);
        index += 1;
    }
    let root = ClientRoot {
        version: 2,
        begin_sha256: page.begin_sha256,
        descriptor_sha256: page.descriptor_sha256,
        native_root: state.native_root.clone(),
        recovery: state
            .recovery
            .clone()
            .ok_or_else(|| invalid("record recovery identity missing"))?,
        native_file_count: state.native_files,
        native_bytes: state.native_bytes,
        external_dependency_count: 0,
        pages: references,
        records: state.record_profile.clone(),
    };
    root.validate_for(&state.begin)?;
    planner::metadata::freeze(inner, state, key, index, &codec::encode(&root)?)?;
    Ok(())
}
