//! Bind cursor continuations and final row order to the core's captured types.

use super::*;

pub(super) async fn execute<H: HostNode>(
    host: &H,
    mut query: Query,
    limit: Option<usize>,
    offset: Option<usize>,
    cursor: Option<QueryCursor>,
    ctx: &AccessContext,
) -> Result<Value, HostError> {
    rules::validate_query_options(&query, cursor.is_some())?;
    let comparison = capture(host, &query.schema_name).await?;
    if let Some(comparison) = &comparison {
        query.filter = comparison
            .canonical_filter(query.filter.take())
            .map_err(HostError::from)?;
    }
    let binding = comparison
        .as_ref()
        .map(|comparison| {
            comparison
                .cursor_binding(&(
                    &query.filter,
                    &query.fields,
                    query.include_tombstones,
                    &query.sort_order,
                    &ctx.db_locator,
                    &ctx.storage_prefix,
                ))
                .map_err(HostError::from)
        })
        .transpose()?;
    if let Some(cursor) = cursor.as_ref() {
        rules::validate_cursor_comparison(cursor, binding.as_deref(), comparison.as_deref())?;
    }
    let cursor = cursor.map(QueryCursor::into_key);
    let cursor = match comparison.as_ref() {
        Some(comparison) => cursor
            .map(|key| comparison.canonical_key(key))
            .transpose()
            .map_err(HostError::from)?,
        None => cursor,
    };
    let run = execute::QueryRun {
        host,
        ctx,
        limit: clamp_request_limit(limit),
        offset: offset.unwrap_or(0),
        cursor,
        comparison: binding,
    };
    let future = execute_run(run, query);
    match comparison {
        Some(comparison) => comparison.run(future).await,
        None => future.await,
    }
}

pub(super) async fn capture<H: HostNode>(
    host: &H,
    name: &str,
) -> Result<
    Option<std::sync::Arc<fold_db::schema::types::key_comparison::KeyComparisonContext>>,
    HostError,
> {
    let Some(schema) = host
        .fold_db()
        .schema_manager()
        .get_schema_following_supersession_for_read(name)
        .await
        .map_err(HostError::from)?
    else {
        return Ok(None);
    };
    host.fold_db()
        .db_ops()
        .atoms()
        .key_comparison_context(&schema)
        .await
        .map(Some)
        .map_err(HostError::from)
}

async fn execute_run<H: HostNode>(
    run: execute::QueryRun<'_, H>,
    query: Query,
) -> Result<Value, HostError> {
    if let Some(payload) = run.caller_supplied_page(&query).await? {
        return Ok(payload);
    }
    if rules::can_push_down_key_restricted(&query) {
        return run.key_restricted(query).await;
    }
    if rules::can_push_down(&query) {
        if let Some(payload) = run.unfiltered_pushdown(&query).await? {
            return Ok(payload);
        }
    }
    run.bounded_fallback(query).await
}
