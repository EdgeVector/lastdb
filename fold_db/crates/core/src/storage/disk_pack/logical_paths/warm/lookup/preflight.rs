//! Validate the complete warm result without copying field content or charging
//! a partial lookup that may fall back to a necessary disk read.

use super::*;

#[derive(Deserialize)]
struct Metadata {
    provenance: Option<(super::super::super::body::ReadVersion, FieldValue)>,
    wire: Option<WireMetadata>,
    witness: Option<u64>,
    #[serde(default)]
    projections: BTreeMap<String, Vec<String>>,
}

pub(in crate::storage::disk_pack::logical_paths::warm) fn ready(
    context: &ReadContext,
    call: &HomeRead,
    logical: &LogicalResidentSet,
    query: &Query,
    query_alias: &str,
    keys: &[KeyValue],
) -> Option<(u64, u64)> {
    let mut records = 1u64;
    let mut bytes = keys.iter().try_fold(0u64, |total, key| {
        total.checked_add(
            (key.hash.as_ref().map_or(0, String::len) + key.range.as_ref().map_or(0, String::len))
                as u64,
        )
    })?;
    if !projection::optional_fits(call, records, bytes) {
        return None;
    }
    let mut fields: Option<Vec<String>> = None;
    let mut wire = HashMap::new();
    for key in keys {
        let row = row_alias(query_alias, key);
        let anchor = query.fields.first()?;
        let anchor_id = target(logical, &row, anchor)?;
        let (_, anchor_body) = metadata(logical, &anchor_id, call, records, bytes)?;
        let selected = anchor_body.projections.get(query_alias)?;
        if query.fields.iter().any(|field| !selected.contains(field))
            || fields.as_ref().is_some_and(|fields| fields != selected)
        {
            return None;
        }
        fields.get_or_insert_with(|| selected.clone());
        for field in selected {
            let id = target(logical, &row, field)?;
            let (length, body) = metadata(logical, &id, call, records, bytes)?;
            if !context.provenance_valid(body.provenance.as_ref()?.0, body.witness) {
                return None;
            }
            let flags = body.wire.filter(|flags| {
                flags.query == query_alias && context.provenance_valid(flags.version, flags.witness)
            })?;
            if wire.get(field).is_some_and(|old| old != &flags) {
                return None;
            }
            wire.insert(field.clone(), flags);
            records = records.checked_add(1)?;
            bytes = bytes.checked_add((id.len() + length) as u64)?;
        }
    }
    let first = wire.values().next()?;
    wire.values()
        .all(|entry| {
            entry.conflict_flags == first.conflict_flags && entry.key_form == first.key_form
        })
        .then_some((records, bytes))
}

fn target(logical: &LogicalResidentSet, row: &str, field: &str) -> Option<String> {
    logical
        .linked_record_target(LINKS, &alias(row, field))
        .filter(|(collection, _)| collection == PATH_COLLECTION)
        .map(|(_, id)| id)
}

fn metadata(
    logical: &LogicalResidentSet,
    id: &str,
    call: &HomeRead,
    records: u64,
    bytes: u64,
) -> Option<(usize, Metadata)> {
    let length = logical.fetched_record_bytes(PATH_COLLECTION, id)??;
    if !projection::optional_fits(
        call,
        records.checked_add(1)?,
        bytes.checked_add(id.len().checked_add(length)? as u64)?,
    ) {
        return None;
    }
    logical.inspect_record(PATH_COLLECTION, id, |bytes| {
        let bytes = bytes?;
        serde_json::from_slice(bytes)
            .ok()
            .map(|metadata| (bytes.len(), metadata))
    })?
}
