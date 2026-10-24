//! Themed module split from the parent.

use super::*;

/// Execute a query under the given access context and return the formatted JSON
/// rows — `{ key, fields, metadata, author_pub_key }` — with the deterministic
/// `(range, hash)` total order and per-field
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

/// Integrity counters and unfilled output slots for one request.
/// Internal candidate refills retain their integrity counters without making
/// an exhausted range claim another page — see [`page_payload`].
#[derive(Debug, Clone, Copy)]
pub(in crate::handlers) struct RowDrops {
    /// Rows whose tip pointed at an atom body that could not be resolved.
    pub(in crate::handlers) unresolved: usize,
    /// Rows whose atom body is a content tombstone.
    pub(in crate::handlers) tombstoned: usize,
    /// Drops that still consume a slot after any internal candidate refill.
    unfilled_slots: usize,
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
            unfilled_slots: 0,
            key_form: None,
            conflict_flags: "unknown",
        }
    }
}

impl RowDrops {
    pub(in crate::handlers) fn total(self) -> usize {
        self.unfilled_slots
    }
}

impl From<fold_db::db_operations::QueryRowDrops> for RowDrops {
    fn from(drops: fold_db::db_operations::QueryRowDrops) -> Self {
        Self {
            unresolved: drops.unresolved.try_into().unwrap_or(usize::MAX),
            tombstoned: drops.tombstoned.try_into().unwrap_or(usize::MAX),
            unfilled_slots: drops.total().try_into().unwrap_or(usize::MAX),
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
    let sort_helper = unprojected_sort_field(&query);

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
    // Three sub-phases separate core reads, formatting and final ordering.
    // HydrateFilter is a nested detail inside the core's first-pass reads.
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
        result_map,
        sort_order.as_ref(),
        order_by.as_ref(),
        sort_helper.as_deref(),
    );
    request_phases::add_phase(RequestPhase::Hydrate, hydrate_started.elapsed());

    let annotate_started = std::time::Instant::now();
    let mut drops = RowDrops::from(drops);
    drops.conflict_flags = annotate_conflict_flags(host, &mut results).await;
    request_phases::add_phase(RequestPhase::Annotate, annotate_started.elapsed());

    Ok((results, drops))
}

/// Format a query result map into the `/api/query` row envelopes, imposing the
/// deterministic `(range, hash)` total order. Conditions already selected the
/// keys in the core's captured first pass.
pub(in crate::handlers) fn format_rows(
    result_map: HashMap<String, HashMap<KeyValue, fold_db::schema::types::field::FieldValue>>,
    sort_order: Option<&SortOrder>,
    order_by: Option<&QueryOrderBy>,
    sort_helper: Option<&str>,
) -> Vec<Value> {
    let format_started = std::time::Instant::now();
    let records_map = fold_db::fold_db_core::query::records_from_field_map(result_map);
    let mut results: Vec<Value> = records_map
        .into_iter()
        .map(|(key, mut record)| {
            if let Some(field) = sort_helper {
                record.metadata.remove(field);
            }
            let author_pub_key = record
                .metadata
                .values()
                .find_map(|m| m.writer_pubkey.as_deref())
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

    // Keep the deterministic response order inside the existing sort phase.
    let sort_started = std::time::Instant::now();
    results.sort_by(|a, b| super::rules::compare_rows(a, b, sort_order, order_by));

    request_phases::add_phase(RequestPhase::HydrateSort, sort_started.elapsed());

    if let Some(field) = sort_helper {
        for row in &mut results {
            if let Some(fields) = row.get_mut("fields").and_then(Value::as_object_mut) {
                fields.remove(field);
            }
        }
    }
    results
}

/// The core retains this sort value without changing projection key discovery.
fn unprojected_sort_field(query: &Query) -> Option<String> {
    let field = &query.order_by.as_ref()?.field;
    if query.fields.is_empty() || query.fields.contains(field) {
        return None;
    }
    Some(field.clone())
}
