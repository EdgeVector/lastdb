//! Bind cursor continuations and final row order to the core's captured types.

use super::*;

pub(super) async fn execute<H: HostNode>(
    host: &H,
    query: Query,
    limit: Option<usize>,
    offset: Option<usize>,
    cursor: Option<QueryCursor>,
    ctx: &AccessContext,
) -> Result<Value, HostError> {
    rules::validate_query_options(&query, cursor.is_some())?;
    let comparison = capture(host, &query.schema_name).await?;
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
    if let Some(received) = cursor
        .as_ref()
        .and_then(|cursor| cursor.comparison.as_ref())
    {
        if binding.as_ref() != Some(received) {
            return Err(HostError::new(
                409,
                "cursor comparison context changed; restart from the first page",
            ));
        }
    }
    if cursor
        .as_ref()
        .is_some_and(|cursor| cursor.comparison.is_none())
        && comparison
            .as_ref()
            .is_some_and(|comparison| !comparison.accepts_legacy_cursor())
    {
        return Err(HostError::new(
            409,
            "legacy cursor cannot continue this key order; restart from the first page",
        ));
    }
    let cursor = cursor.map(QueryCursor::into_key);
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
