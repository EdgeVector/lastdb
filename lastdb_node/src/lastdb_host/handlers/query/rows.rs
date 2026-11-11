//! Query read flow. Response builders and row rules live in sibling modules.

use super::*;
use fold_db::db_operations::PageEnd;
use fold_db::schema::types::key_comparison::KeyComparisonContext;

/// The page flags of one request: how to read its page keys. See
/// [`page_payload`].
#[derive(Debug, Clone, Copy, Default)]
pub(in crate::lastdb_host::handlers) struct PageFlags {
    /// How to read this page's `key.hash` values, or `None` when the query
    /// emitted no classifiable page key.
    pub(in crate::lastdb_host::handlers) key_form: Option<fold_db::db_operations::KeyForm>,
}

impl From<fold_db::db_operations::PageKeyForms> for PageFlags {
    fn from(forms: fold_db::db_operations::PageKeyForms) -> Self {
        Self {
            key_form: forms.key_form(),
        }
    }
}

/// Read query rows, optionally hydrating only `window` of the matched rows.
pub(in crate::lastdb_host::handlers) async fn execute_query_rows_windowed<H: HostNode>(
    host: &H,
    query: Query,
    window: Option<KeyWindow>,
) -> Result<(Vec<Value>, PageFlags, PageEnd), HostError> {
    let comparison = super::capture_key_comparison(host, &query.schema_name).await?;
    let future = execute_rows_captured(host, query, window);
    match comparison {
        Some(comparison) => comparison.run(future).await,
        None => future.await,
    }
}

/// The core read of one query, with the key forms of its page and, when the
/// read can continue a page that ends early, where the page ended.
async fn read_records(
    executor: &fold_db::fold_db_core::query::QueryExecutor,
    query: Query,
    window: Option<KeyWindow>,
) -> (
    Result<fold_db::fold_db_core::query::QueryRecords, fold_db::schema::SchemaError>,
    fold_db::db_operations::PageKeyForms,
    PageEnd,
) {
    let resumable = window.is_some() && !matches!(query.sort_order, Some(SortOrder::Desc));
    match window {
        Some(window) if resumable => {
            fold_db::db_operations::with_query_page_tally(executor.query_window(query, window))
                .await
        }
        Some(window) => {
            let (result, forms) =
                fold_db::db_operations::with_query_tally(executor.query_window(query, window))
                    .await;
            (result, forms, PageEnd::default())
        }
        None => {
            let (result, forms) =
                fold_db::db_operations::with_query_tally(executor.query(query)).await;
            (result, forms, PageEnd::default())
        }
    }
}

async fn execute_rows_captured<H: HostNode>(
    host: &H,
    query: Query,
    window: Option<KeyWindow>,
) -> Result<(Vec<Value>, PageFlags, PageEnd), HostError> {
    let sort_order = query.sort_order.clone();

    let executor = host.fold_db();
    let executor = executor.query_executor();
    let (result, forms, end) = read_records(executor, query, window).await;
    let records = result.map_err(HostError::from)?;

    let compact = QueryResponseFormat::current() == QueryResponseFormat::Values;
    let mut results = format_rows(records, compact);

    let range_comparison = KeyComparisonContext::capture_current_range_comparison();
    super::row_rules::sort_rows(&mut results, sort_order.as_ref(), range_comparison.as_ref())?;

    Ok((results, PageFlags::from(forms), end))
}
