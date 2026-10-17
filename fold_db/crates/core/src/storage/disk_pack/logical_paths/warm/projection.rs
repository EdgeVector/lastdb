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
    owned_body(context, call, logical, &collection, &id).map(|body| body.map(|body| (id, body)))
}

pub(super) fn owned_body(
    context: &ReadContext,
    call: &mut HomeRead,
    logical: &mut LogicalResidentSet,
    collection: &str,
    id: &str,
) -> StorageResult<Option<PathBody>> {
    let Some(Some(length)) = logical.fetched_record_bytes(collection, id) else {
        return Ok(None);
    };
    context.hold_record(logical, collection, id)?;
    call.account_memory_record(id.len(), length).map_err(map)?;
    logical
        .fetched_record(collection, id)
        .flatten()
        .map(|bytes| serde_json::from_slice(&bytes).map_err(|e| serialization(&e)))
        .transpose()
}

pub(super) fn remember(
    runtime: &PackRuntime,
    context: &ReadContext,
    query: &Query,
    access: Option<&AccessContext>,
    keys: &[KeyValue],
    results: &FieldResults,
) -> StorageResult<()> {
    if results.len().saturating_mul(keys.len()) > 10_000
        || results.values().any(|values| values.len() != keys.len())
    {
        return Ok(());
    }
    let mut rows = Vec::new();
    for key in keys {
        let mut ids = BTreeMap::new();
        for (field, values) in results {
            let Some(value) = values.get(key) else {
                return Ok(());
            };
            let Some(id) = context.paths.lock().expect("native resolved paths").one(
                field,
                key,
                &value.atom_uuid,
            ) else {
                return Ok(());
            };
            ids.insert(field.clone(), id);
        }
        rows.push((key, ids));
    }
    let query_alias = query_alias(query, access)?;
    let mut call = crate::read_receipts::lock(&context.call);
    let mut logical = runtime.logical.lock().expect("native query projection");
    for (key, ids) in rows {
        save(
            context,
            &mut call,
            &mut logical,
            &query_alias,
            key,
            &query.fields,
            &ids,
        )?;
    }
    Ok(())
}

pub(super) fn save(
    context: &ReadContext,
    call: &mut HomeRead,
    logical: &mut LogicalResidentSet,
    query: &str,
    key: &KeyValue,
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
    let Some(mut body) = owned_body(context, call, logical, PATH_COLLECTION, anchor)? else {
        return Ok(());
    };
    if body.provenance.as_ref().map(|(version, _)| *version) != Some(context.version) {
        return Ok(());
    }
    body.projections
        .insert(query.to_string(), ids.keys().cloned().collect());
    let mut removed = Vec::new();
    while body.projections.len() > MAX_PROJECTIONS
        || serde_json::to_vec(&body.projections)
            .map_err(|e| serialization(&e))?
            .len()
            > MAX_DESCRIPTOR_BYTES
    {
        let Some(old) = body
            .projections
            .keys()
            .find(|old| old.as_str() != query)
            .cloned()
        else {
            return Ok(());
        };
        let fields = body
            .projections
            .remove(&old)
            .expect("selected old descriptor");
        removed.push((old, fields));
    }
    let bytes = serde_json::to_vec(&body).map_err(|e| serialization(&e))?;
    call.account_memory_record(anchor.len(), bytes.len())
        .map_err(map)?;
    for (old, fields) in removed {
        let old_row = row_alias(&old, key);
        for field in fields {
            logical.unlink_record(LINKS, &alias(&old_row, &field));
        }
    }
    let row = row_alias(query, key);
    for (field, id) in ids {
        if !logical.link_record(LINKS, &alias(&row, field), PATH_COLLECTION, id) {
            return Ok(());
        }
    }
    let epoch = logical.record_epoch(anchor);
    logical.admit_record(PATH_COLLECTION, anchor, Some(bytes), epoch);
    Ok(())
}
