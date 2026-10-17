//! A descriptor names each actual result field's separately held logical path.

use super::*;
use laststore::disk_pack::home::HomeRead;

const MAX_PROJECTIONS: usize = 4;
const MAX_DESCRIPTOR_BYTES: usize = 16 * 1024;

pub(super) fn linked_body(
    context: &ReadContext,
    call: &mut HomeRead,
    logical: &mut LogicalResidentSet,
    query: &str,
    field: &str,
) -> StorageResult<Option<(String, PathBody)>> {
    let Some((collection, id)) = logical.linked_record_target(LINKS, &alias(query, field)) else {
        return Ok(None);
    };
    let Some(Some(length)) = logical.fetched_record_bytes(&collection, &id) else {
        return Ok(None);
    };
    context.hold_record(logical, &collection, &id)?;
    call.account_memory_record(id.len(), length).map_err(map)?;
    logical
        .fetched_record(&collection, &id)
        .flatten()
        .map(|bytes| {
            serde_json::from_slice(&bytes)
                .map(|body| (id, body))
                .map_err(|e| serialization(&e))
        })
        .transpose()
}

pub(super) fn remember(
    runtime: &PackRuntime,
    context: &ReadContext,
    query: &Query,
    access: Option<&AccessContext>,
    key: &KeyValue,
    results: &FieldResults,
) -> StorageResult<()> {
    let mut ids = BTreeMap::new();
    for (field, values) in results {
        let Some(value) = values.get(key).filter(|_| values.len() == 1) else {
            return Ok(());
        };
        let Some(id) =
            context
                .paths
                .lock()
                .expect("native resolved paths")
                .one(field, key, &value.atom_uuid)
        else {
            return Ok(());
        };
        ids.insert(field.clone(), id);
    }
    let mut call = crate::read_receipts::lock(&context.call);
    let mut logical = runtime.logical.lock().expect("native query projection");
    save(
        context,
        &mut call,
        &mut logical,
        &query_alias(query, access)?,
        &query.fields,
        &ids,
    )
}

pub(super) fn save(
    context: &ReadContext,
    call: &mut HomeRead,
    logical: &mut LogicalResidentSet,
    query: &str,
    requested: &[String],
    ids: &BTreeMap<String, String>,
) -> StorageResult<()> {
    if ids.is_empty()
        || ids.len() > 10_000
        || requested.iter().any(|field| !ids.contains_key(field))
    {
        return Ok(());
    }
    let Some(anchor) = requested.first().and_then(|field| ids.get(field)) else {
        return Ok(());
    };
    let Some(Some(length)) = logical.fetched_record_bytes(PATH_COLLECTION, anchor) else {
        return Ok(());
    };
    call.account_memory_record(anchor.len(), length)
        .map_err(map)?;
    let Some(Some(bytes)) = logical.fetched_record(PATH_COLLECTION, anchor) else {
        return Ok(());
    };
    let mut body: PathBody = serde_json::from_slice(&bytes).map_err(|e| serialization(&e))?;
    if body.provenance.as_ref().map(|(version, _)| *version) != Some(context.version)
        || (!body.projections.contains_key(query) && body.projections.len() >= MAX_PROJECTIONS)
    {
        return Ok(());
    }
    body.projections
        .insert(query.to_string(), ids.keys().cloned().collect());
    if serde_json::to_vec(&body.projections)
        .map_err(|e| serialization(&e))?
        .len()
        > MAX_DESCRIPTOR_BYTES
    {
        return Ok(());
    }
    for (field, id) in ids {
        if !logical.link_record(LINKS, &alias(query, field), PATH_COLLECTION, id) {
            return Ok(());
        }
    }
    let bytes = serde_json::to_vec(&body).map_err(|e| serialization(&e))?;
    call.account_memory_record(anchor.len(), bytes.len())
        .map_err(map)?;
    let epoch = logical.record_epoch(anchor);
    logical.admit_record(PATH_COLLECTION, anchor, Some(bytes), epoch);
    Ok(())
}
