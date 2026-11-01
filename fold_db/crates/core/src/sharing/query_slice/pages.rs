//! The read of one leg of a slice, in pages.
//!
//! One request examines at most 8,192 record keys, and a deleted record is a
//! key. A leg whose filter selects a key set
//! ([`HashRangeFilter::is_key_restricted`]) and that has no post-key work reads
//! its rows in keyset pages, under the page tally of the query
//! (`with_query_page_tally`). A page that a run of deleted records ends early
//! names the last key it examined, and the next page starts after that key. So
//! a delivery over a run of deleted records of any length costs pages; it does
//! not fail at the read limit. The pages run in the key comparison context of
//! the schema, so the cursor follows the order of the executor.
//!
//! Another leg keeps one read. A condition, an order or a limit decides its
//! rows after the key set is read, and needs the complete key set.

use crate::access::AccessContext;
use crate::db_operations::{with_query_page_tally, PageResume};
use crate::fold_db_core::query::QueryRecords;
use crate::fold_db_core::FoldDB;
use crate::schema::types::field::{HashRangeFilter, KeyWindow};
use crate::schema::types::operations::Query;
use crate::schema::types::{KeyValue, KeyValuePageOrder};
use crate::schema::{Schema, SchemaError};

/// Rows that one page of a leg asks for, at most.
const LEG_PAGE_ROWS: usize = 256;

/// The rows of one leg: in keyset pages for a keyed leg with no post-key
/// work, in one read for another leg.
pub(super) async fn read_leg(
    db: &FoldDB,
    schema: &Schema,
    query: Query,
    access: &AccessContext,
) -> Result<QueryRecords, SchemaError> {
    let paged = query
        .filter
        .as_ref()
        .is_some_and(HashRangeFilter::is_key_restricted)
        && !query.requires_post_key_work();
    if !paged {
        return db.query_executor().query_with_access(query, access).await;
    }
    let comparison = db.db_ops().atoms().key_comparison_context(schema).await?;
    comparison.run(read_pages(db, query, access)).await
}

/// Read the pages of a keyed leg until a page has no row and did not end
/// early. Each page starts after the greatest key of the page before it: its
/// last row, or the last key that a run of deleted records let it examine.
async fn read_pages(
    db: &FoldDB,
    query: Query,
    access: &AccessContext,
) -> Result<QueryRecords, SchemaError> {
    let executor = db.query_executor();
    let mut rows = QueryRecords::new();
    let mut after: Option<KeyValue> = None;
    loop {
        let window = after.clone().map_or_else(
            || KeyWindow::offset(0, LEG_PAGE_ROWS),
            |after| KeyWindow::after(after, LEG_PAGE_ROWS),
        );
        let (page, _, end) =
            with_query_page_tally(executor.query_with_access_window(query.clone(), window, access))
                .await;
        let page = page?;
        let resume = match end.resume {
            Some(PageResume::Key(key)) => Some(key),
            Some(PageResume::Record(_)) => {
                return Err(SchemaError::InvalidData(
                    "a keyed slice read ended at a record walk position".into(),
                ))
            }
            None => None,
        };
        let next = page
            .keys()
            .chain(resume.as_ref())
            .max_by(|a, b| a.cmp_page_order(b))
            .cloned();
        rows.extend(page);
        let Some(next) = next else {
            return Ok(rows);
        };
        if after
            .as_ref()
            .is_some_and(|previous| !next.cmp_page_order(previous).is_gt())
        {
            return Err(SchemaError::InvalidData(
                "a keyed slice read did not advance its cursor".into(),
            ));
        }
        after = Some(next);
    }
}
