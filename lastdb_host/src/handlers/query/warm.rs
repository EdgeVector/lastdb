//! A warm exact-key read uses only the logical records this query requests.

use super::*;

pub(super) async fn read<H: HostNode>(
    host: &H,
    query: &Query,
    limit: Option<usize>,
    offset: Option<usize>,
    cursor: Option<&KeyValue>,
    ctx: &AccessContext,
) -> Result<Option<Value>, HostError> {
    if !ctx.is_owner || offset.unwrap_or(0) != 0 || cursor.is_some() {
        return Ok(None);
    }
    let Some(count) = fold_db::storage::disk_pack::WarmQueryFields::requested_key_count(query)
    else {
        return Ok(None);
    };
    let limit = clamp_request_limit(limit);
    if limit < count {
        return Ok(None);
    }
    let Some(runtime) = host
        .fold_db()
        .db_ops()
        .namespaced_store()
        .disk_pack_runtime()
    else {
        return Ok(None);
    };
    let _permit = acquire_read_permit(host, Lane::for_read_rows(count)).await?;
    let Some(warm) = fold_db::storage::disk_pack::warm_wire_query(Some(runtime), query, ctx)
        .map_err(|error| HostError::new(503, error.to_string()))?
    else {
        return Ok(None);
    };
    let mut rows = format_rows(&warm.fields, query.sort_order.as_ref(), None, None);
    let flags = warm.field_flags();
    for row in &mut rows {
        if let Some(metadata) = row.get_mut("metadata").and_then(Value::as_object_mut) {
            for (field, flag) in &flags {
                if let Some(value) = metadata.get_mut(field).and_then(Value::as_object_mut) {
                    value.insert("has_conflicts".to_string(), flag.clone());
                }
            }
        }
    }
    let drops = RowDrops {
        conflict_flags: if warm.conflict_flags() == Some("known") {
            "known"
        } else {
            "unknown"
        },
        key_form: match warm.key_form() {
            Some("api") => Some(fold_db::db_operations::KeyForm::Api),
            Some("opaque") => Some(fold_db::db_operations::KeyForm::Opaque),
            Some("mixed") => Some(fold_db::db_operations::KeyForm::Mixed),
            _ => None,
        },
        ..RowDrops::default()
    };
    Ok(Some(cursor_payload(
        rows,
        matches!(query.sort_order.as_ref(), Some(SortOrder::Desc)).then_some(count),
        limit,
        0,
        drops,
    )))
}
