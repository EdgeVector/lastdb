use fold_db::schema::types::field::HashRangeFilter;
use fold_db::schema::types::operations::{Query, ValueFilter};

use super::guard::SCHEMA;

pub(crate) fn point_query(schema: &str, field: &str, hash_key: String) -> Query {
    Query {
        schema_name: schema.to_string(),
        fields: vec![field.to_string()],
        filter: Some(HashRangeFilter::HashKey(hash_key)),
        as_of: None,
        rehydrate_depth: None,
        sort_order: None,
        value_filters: None,
        field_predicates: None,
        order_by: None,
        predicate_limit: None,
        expected_total_count: None,
        include_tombstones: false,
        secondary_concurrency: None,
    }
}

pub(crate) fn full_query() -> Query {
    Query {
        schema_name: SCHEMA.to_string(),
        fields: vec!["email".to_string()],
        filter: None,
        as_of: None,
        rehydrate_depth: None,
        sort_order: None,
        value_filters: None,
        field_predicates: None,
        order_by: None,
        predicate_limit: None,
        expected_total_count: None,
        include_tombstones: false,
        secondary_concurrency: None,
    }
}

pub(crate) fn page_query(limit: usize) -> Query {
    Query {
        schema_name: SCHEMA.to_string(),
        fields: vec!["email".to_string()],
        filter: Some(HashRangeFilter::Page { offset: 0, limit }),
        as_of: None,
        rehydrate_depth: None,
        sort_order: None,
        value_filters: None,
        field_predicates: None,
        order_by: None,
        predicate_limit: None,
        expected_total_count: None,
        include_tombstones: false,
        secondary_concurrency: None,
    }
}

/// A value-filter SCAN query: a full-span `Page` key filter (so the WHOLE
/// `score` field is materialized, NOT capped at the default unfiltered page of
/// 100) plus a numeric `value_filters` predicate applied post-fetch. This is the
/// "load every candidate record, then drop the rows whose value fails the
/// predicate" path (`QueryExecutor::apply_value_filters` runs AFTER the field
/// load) — and the explicit large page is exactly the `SampleN(10_000)` bulk
/// shape the 2026-06-20 wedge exploited.
///
/// (A bare `filter: None` would silently cap at `DEFAULT_UNFILTERED_PAGE_LIMIT`
/// = 100 rows, so the scan would NOT grow with corpus and the bench would
/// measure nothing — the full-span page is what makes this the O(corpus) scan.)
pub(crate) fn value_filter_query() -> Query {
    Query {
        schema_name: "Reviews".to_string(),
        fields: vec!["score".to_string()],
        filter: Some(HashRangeFilter::Page {
            offset: 0,
            limit: usize::MAX,
        }),
        as_of: None,
        rehydrate_depth: None,
        sort_order: None,
        // Matches the fixed `[0, 1)` subset seeded by `seeded_scored_db`.
        value_filters: Some(vec![ValueFilter::LessThan {
            field: "score".to_string(),
            value: 1.0,
        }]),
        field_predicates: None,
        order_by: None,
        predicate_limit: None,
        expected_total_count: None,
        include_tombstones: false,
        secondary_concurrency: None,
    }
}
