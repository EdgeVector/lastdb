//! Themed module split from the parent.

use super::*;

/// Execute a query under the given access context and return the formatted JSON
/// rows — `{ key, fields, metadata, author_pub_key }` — with the deterministic
/// `(range, hash)` total order, post-hoc value filtering, and per-field
/// merge-conflict annotation. This is the single copy of what both hosts'
/// `execute_query_json_with_context` used to do.
///
/// # Errors
/// Returns a [`HostError`] `500` on a query-executor failure.
pub async fn execute_query_rows<H: HostNode>(
    host: &H,
    query: Query,
    ctx: &AccessContext,
) -> Result<Vec<Value>, HostError> {
    execute_query_rows_with_unresolved(host, query, ctx)
        .await
        .map(|(rows, _)| rows)
}

/// Page slots this request consumed but did not deliver, in the units
/// `has_more` needs.
///
/// Both members were counted by `total_count` and both cost the window a slot,
/// so both have to be added back before a page is called the last one — see
/// [`page_payload`].
#[derive(Debug, Clone, Copy)]
pub(in crate::handlers) struct RowDrops {
    /// Rows whose tip pointed at an atom body that could not be resolved.
    pub(in crate::handlers) unresolved: usize,
    /// Rows whose atom body is a content tombstone.
    pub(in crate::handlers) tombstoned: usize,
    /// How to read this page's `key.hash` values, or `None` when the query
    /// emitted no classifiable page key. Deliberately NOT part of
    /// [`RowDrops::total`]: these rows WERE delivered.
    pub(in crate::handlers) key_form: Option<fold_db::db_operations::KeyForm>,
    /// `"known"` or `"unknown"`. The default is `"unknown"` so a forgotten
    /// set cannot look clean. A missing `has_conflicts` is clean only when
    /// this field is `"known"`.
    pub(in crate::handlers) conflict_flags: &'static str,
}

impl Default for RowDrops {
    fn default() -> Self {
        Self {
            unresolved: 0,
            tombstoned: 0,
            key_form: None,
            conflict_flags: "unknown",
        }
    }
}

impl RowDrops {
    pub(in crate::handlers) fn total(self) -> usize {
        self.unresolved.saturating_add(self.tombstoned)
    }
}

impl From<fold_db::db_operations::QueryRowDrops> for RowDrops {
    fn from(drops: fold_db::db_operations::QueryRowDrops) -> Self {
        Self {
            unresolved: drops.unresolved.try_into().unwrap_or(usize::MAX),
            tombstoned: drops.tombstoned.try_into().unwrap_or(usize::MAX),
            key_form: drops.key_form(),
            conflict_flags: "unknown",
        }
    }
}

pub(in crate::handlers) async fn execute_query_rows_with_unresolved<H: HostNode>(
    host: &H,
    query: Query,
    ctx: &AccessContext,
) -> Result<(Vec<Value>, RowDrops), HostError> {
    execute_query_rows_windowed(host, query, None, ctx).await
}

/// [`execute_query_rows_with_unresolved`], optionally hydrating only `window`
/// of the matched rows.
///
/// `window` is `(offset, limit)` over the canonical key order and is applied
/// inside the query executor, before atom bodies load — it is the push-down
/// itself, not a slice of the result. Callers pass `None` to materialize every
/// matched row, which is what the value-filtered and two-pass shapes need.
pub(in crate::handlers) async fn execute_query_rows_windowed<H: HostNode>(
    host: &H,
    query: Query,
    window: Option<KeyWindow>,
    ctx: &AccessContext,
) -> Result<(Vec<Value>, RowDrops), HostError> {
    let sort_order = query.sort_order.clone();
    let order_by = query.order_by.clone();
    let value_filters = query.value_filters.clone();

    // Scope the tally to this request. Atom hydration happens inside
    // `query_with_access`, so the scope has to wrap that call and nothing
    // wider — a node-lifetime counter sampled before/after would attribute
    // concurrent requests' dangling edges to this one.
    let executor = host.fold_db();
    let executor = executor.query_executor();
    // `hydrate` spans the executor call (where atom bodies load) through
    // `format_rows` (envelope build + total-order sort). Conflict annotation
    // is deliberately NOT included: it is its own phase below, because it is
    // the one step whose cost is a storage-plane artifact rather than a
    // function of how many rows the caller asked for.
    //
    // The span is ALSO reported as four sub-phases that partition it exactly
    // (`hydrate_atoms`/`hydrate_format`/`hydrate_sort`/`hydrate_filter`), so
    // that a slow read says WHICH of storage, rendering, ordering or
    // post-filtering is slow. `hydrate` itself stays because it is the series
    // every rollup written before the carve carries.
    let hydrate_started = std::time::Instant::now();
    let (result, drops) = match window {
        Some(window) => match window {
            KeyWindow::Offset { offset, limit } => {
                fold_db::db_operations::with_query_row_drop_tally(
                    executor.query_with_access_windowed(query, (offset, limit), ctx),
                )
                .await
            }
            KeyWindow::After { after, limit } => {
                fold_db::db_operations::with_query_row_drop_tally(
                    executor.query_with_access_keyset_windowed(query, after, limit, ctx),
                )
                .await
            }
        },
        None => {
            fold_db::db_operations::with_query_row_drop_tally(
                executor.query_with_access(query, ctx),
            )
            .await
        }
    };
    // Charge the hydration that DID happen before propagating a failure —
    // otherwise a slow query that ends in an error reports no phases at all,
    // which is exactly the case an operator most wants decomposed.
    // Charged before the error check so a failed executor call still reports
    // where its time went, for the same reason `hydrate` is charged below it.
    request_phases::add_phase(RequestPhase::HydrateAtoms, hydrate_started.elapsed());
    let result_map = match result.map_err(HostError::from) {
        Ok(map) => map,
        Err(err) => {
            request_phases::add_phase(RequestPhase::Hydrate, hydrate_started.elapsed());
            return Err(err);
        }
    };

    let mut results = format_rows(
        &result_map,
        sort_order.as_ref(),
        order_by.as_ref(),
        value_filters.as_deref(),
    );
    request_phases::add_phase(RequestPhase::Hydrate, hydrate_started.elapsed());

    let annotate_started = std::time::Instant::now();
    let mut drops = RowDrops::from(drops);
    drops.conflict_flags = annotate_conflict_flags(host, &mut results).await;
    request_phases::add_phase(RequestPhase::Annotate, annotate_started.elapsed());

    Ok((results, drops))
}

/// Format a query result map into the `/api/query` row envelopes, imposing the
/// deterministic `(range, hash)` total order and post-hoc value filtering. Split
/// out so the (rare) result-union path can format each pool separately.
pub(in crate::handlers) fn format_rows(
    result_map: &HashMap<String, HashMap<KeyValue, fold_db::schema::types::field::FieldValue>>,
    sort_order: Option<&SortOrder>,
    order_by: Option<&QueryOrderBy>,
    value_filters: Option<&[ValueFilter]>,
) -> Vec<Value> {
    let format_started = std::time::Instant::now();
    let records_map = fold_db::fold_db_core::query::records_from_field_map(result_map);
    let mut results: Vec<Value> = records_map
        .into_iter()
        .map(|(key, record)| {
            let author_pub_key = record
                .metadata
                .values()
                .find_map(|m| m.writer_pubkey.clone())
                .filter(|s| !s.is_empty());
            serde_json::json!({
                "key": key,
                "fields": record.fields,
                "metadata": record.metadata,
                "author_pub_key": author_pub_key,
            })
        })
        .collect();

    request_phases::add_phase(RequestPhase::HydrateFormat, format_started.elapsed());

    // Deterministic total order: results come from a randomized HashMap
    // iteration, so offset/limit pagination would otherwise overlap/drop rows.
    // Primary key is `range` (direction follows sort_order); `hash` is an
    // always-ascending tiebreaker (opaque content hash — reversing it is churn).
    let sort_started = std::time::Instant::now();
    results.sort_by(|a, b| {
        let primary = order_by
            .and_then(|order_by| {
                let af = a.get("fields")?.get(&order_by.field);
                let bf = b.get("fields")?.get(&order_by.field);
                Some(compare_json_values(af, bf, order_by.order.as_ref()))
            })
            .unwrap_or_else(|| {
                let a_range = a["key"]["range"].as_str().unwrap_or("");
                let b_range = b["key"]["range"].as_str().unwrap_or("");
                match sort_order {
                    Some(SortOrder::Desc) => b_range.cmp(a_range),
                    _ => a_range.cmp(b_range),
                }
            });
        primary
            .then_with(|| {
                let a_range = a["key"]["range"].as_str().unwrap_or("");
                let b_range = b["key"]["range"].as_str().unwrap_or("");
                a_range.cmp(b_range)
            })
            .then_with(|| {
                let a_hash = a["key"]["hash"].as_str().unwrap_or("");
                let b_hash = b["key"]["hash"].as_str().unwrap_or("");
                a_hash.cmp(b_hash)
            })
    });

    request_phases::add_phase(RequestPhase::HydrateSort, sort_started.elapsed());

    // Value filters (AND). A filter whose target field is absent from a row is
    // treated as "does not apply", not an automatic failure — the executor's
    // `apply_value_filters` already dropped every row whose CONCRETE value
    // failed the predicate, so a stricter rule here would blank sparse or
    // partially-access-stripped rows.
    let filter_started = std::time::Instant::now();
    if let Some(filters) = value_filters {
        results.retain(|record| {
            let Some(fields) = record.get("fields") else {
                return false;
            };
            filters
                .iter()
                .all(|filter| match fields.get(filter.field_name()) {
                    Some(v) => filter.matches(v),
                    None => true,
                })
        });
    }
    request_phases::add_phase(RequestPhase::HydrateFilter, filter_started.elapsed());

    results
}

pub(in crate::handlers) fn compare_json_values(
    a: Option<&Value>,
    b: Option<&Value>,
    order: Option<&SortOrder>,
) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    let ordering = match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some(Value::Number(a)), Some(Value::Number(b))) => a
            .as_f64()
            .partial_cmp(&b.as_f64())
            .unwrap_or(Ordering::Equal),
        (Some(Value::String(a)), Some(Value::String(b))) => a.cmp(b),
        (Some(Value::Bool(a)), Some(Value::Bool(b))) => a.cmp(b),
        (Some(a), Some(b)) => value_rank(a)
            .cmp(&value_rank(b))
            .then_with(|| a.to_string().cmp(&b.to_string())),
    };
    match order {
        Some(SortOrder::Desc) => ordering.reverse(),
        _ => ordering,
    }
}

pub(in crate::handlers) fn value_rank(value: &Value) -> u8 {
    match value {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Number(_) => 2,
        Value::String(_) => 3,
        Value::Array(_) => 4,
        Value::Object(_) => 5,
    }
}
