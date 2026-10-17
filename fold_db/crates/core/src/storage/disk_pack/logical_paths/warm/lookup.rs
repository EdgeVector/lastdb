//! One positive public row reuses only its held canonical path owners.

use super::*;
use laststore::disk_pack::home::HomeRead;

mod preflight;
pub(super) use preflight::ready;

pub(super) fn row(
    context: &ReadContext,
    call: &mut HomeRead,
    logical: &mut LogicalResidentSet,
    query: &Query,
    query_alias: &str,
    key: &KeyValue,
) -> StorageResult<Option<WarmQueryFields>> {
    let row_alias = row_alias(query_alias, key);
    let anchor = &query.fields[0];
    let Some((id, body)) = projection::linked_body(context, call, logical, &row_alias, anchor)?
    else {
        return Ok(None);
    };
    let Some(selected) = body.projections.get(query_alias).cloned() else {
        return Ok(None);
    };
    if query.fields.iter().any(|field| !selected.contains(field)) {
        return Ok(None);
    }
    let mut first = Some((id, body));
    let mut result = WarmQueryFields {
        fields: HashMap::new(),
        wire: HashMap::new(),
    };
    for field in selected {
        let found = if &field == anchor {
            first.take()
        } else {
            projection::linked_body(context, call, logical, &row_alias, &field)?
        };
        let Some((id, body)) = found else {
            return Ok(None);
        };
        let Some(flags) = body.wire.clone().filter(|wire| wire.query == query_alias) else {
            return Ok(None);
        };
        let Some(value) = body.field(context.version) else {
            return Ok(None);
        };
        context.paths.lock().expect("native resolved paths").note(
            &field,
            key,
            &value.atom_uuid,
            id,
        );
        result
            .fields
            .insert(field.clone(), HashMap::from([(key.clone(), value)]));
        result.wire.insert(field, flags);
    }
    Ok(Some(result))
}
