//! Query read flow. Response builders and row rules live in sibling modules.

use super::*;
use fold_db::schema::types::key_comparison::KeyComparisonContext;

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

/// Read query rows, optionally hydrating only `window` of the matched rows.
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
    let comparison = super::capture_key_comparison(host, &query.schema_name).await?;
    let future = execute_rows_captured(host, query, window, ctx);
    match comparison {
        Some(comparison) => comparison.run(future).await,
        None => future.await,
    }
}

async fn execute_rows_captured<H: HostNode>(
    host: &H,
    query: Query,
    window: Option<KeyWindow>,
    ctx: &AccessContext,
) -> Result<(Vec<Value>, RowDrops), HostError> {
    let sort_order = query.sort_order.clone();
    let order_by = query.order_by.clone();
    let sort_helper = super::row_rules::unprojected_sort_field(&query);

    // Scope the tally to this request. Atom hydration happens inside
    // `query_with_access`, so the scope has to wrap that call and nothing
    // wider — a node-lifetime counter sampled before/after would attribute
    // concurrent requests' dangling edges to this one.
    let executor = host.fold_db();
    let executor = executor.query_executor();
    // `hydrate` spans the executor call (where atom bodies load) through
    // response construction, total-order sort, and projection. Conflict annotation
    // is deliberately NOT included: it is its own phase below, because it is
    // the one step whose cost is a storage-plane artifact rather than a
    // function of how many rows the caller asked for.
    //
    // Three sub-phases separate core reads, formatting and final ordering.
    // HydrateFilter is a nested detail inside the core's first-pass reads.
    let hydrate_started = std::time::Instant::now();
    let (result, drops) =
        match window {
            Some(KeyWindow::Offset { offset, limit }) => {
                fold_db::db_operations::with_query_row_drop_tally(
                    executor.query_with_access_windowed(query, (offset, limit), ctx),
                )
                .await
            }
            Some(KeyWindow::After { after, limit }) => {
                fold_db::db_operations::with_query_row_drop_tally(
                    executor.query_with_access_keyset_windowed(query, after, limit, ctx),
                )
                .await
            }
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

    let format_started = std::time::Instant::now();
    let mut results = format_rows(result_map, sort_helper.as_deref());
    request_phases::add_phase(RequestPhase::HydrateFormat, format_started.elapsed());

    let sort_started = std::time::Instant::now();
    let sorted = {
        let range_comparison = KeyComparisonContext::capture_current_range_comparison();
        super::row_rules::sort_rows(
            &mut results,
            sort_order.as_ref(),
            order_by.as_ref(),
            range_comparison.as_ref(),
        )
    };
    request_phases::add_phase(RequestPhase::HydrateSort, sort_started.elapsed());
    sorted?;

    super::row_rules::remove_sort_helper(&mut results, sort_helper.as_deref());
    request_phases::add_phase(RequestPhase::Hydrate, hydrate_started.elapsed());

    let annotate_started = std::time::Instant::now();
    let mut drops = RowDrops::from(drops);
    drops.conflict_flags = annotate_conflict_flags(host, &mut results).await;
    request_phases::add_phase(RequestPhase::Annotate, annotate_started.elapsed());

    Ok((results, drops))
}
