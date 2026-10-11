use std::collections::HashMap;

use criterion::{black_box, BenchmarkId, Criterion};
use fold_db::access::AccessContext;
use fold_db::benchmark_support::BenchmarkSchemaBuilder;
use fold_db::fold_db_core::FoldDB;
use fold_db::schema::types::operations::{MutationType, Query};
use fold_db::schema::types::{KeyValue, Mutation};
use serde_json::json;
use tokio::runtime::Runtime;

use super::guard::{configure_guard_group, requested_benchmark_filter_matches};

/// `count_query_rows` fields swept by `bench_count_rows_vs_field_count` — the
/// `total_count` computation for an unfiltered "list all" query
/// (`HashRangeQueryProcessor::count_rows`). The query includes the schema's
/// key field, so count_rows can resolve that single count-bearing field instead
/// of multiplying a full live-row resolution by every requested data field.
/// This pins the field-count axis from the `/api/query` latency-floor report.
pub(crate) const COUNT_ROWS_FIELD_COUNTS: [usize; 3] = [1, 3, 5];

/// Fixed corpus for the field-count sweep, chosen to keep one bench iteration
/// well under a second post-fix while still being large enough that a
/// regression back to the fully-serial per-row `get_per_key` loop in
/// `load_hash_range_page_from_index` is unmistakable in the timings.
pub(crate) const COUNT_ROWS_CORPUS: usize = 2_000;

/// Corpus sizes swept by `bench_count_rows_vs_row_count`. This covers the
/// separate row-count axis: exact tombstone-correct counts may still inspect
/// the count-bearing field's live values, but they must not reintroduce one
/// async storage round trip per row or multiply by requested field count.
pub(crate) const COUNT_ROWS_CARDINALITIES: [usize; 3] = [100, 1_000, 5_000];

pub(crate) const COUNT_ROWS_SCHEMA: &str = "CountRowsWide";

/// Data field names beyond the hash/range keys, so the schema has enough
/// fields to exercise `COUNT_ROWS_FIELD_COUNTS`.
pub(crate) const COUNT_ROWS_DATA_FIELDS: [&str; 5] = ["d0", "d1", "d2", "d3", "d4"];

/// Build a `FoldDB` with a HashRange schema carrying
/// [`COUNT_ROWS_DATA_FIELDS`] (5 independent data fields, all populated for
/// every row) plus `n` records written through the real mutation path.
pub(crate) async fn seeded_count_rows_db(n: usize) -> FoldDB {
    let dir = tempfile::tempdir().expect("temp dir").keep();
    let db = FoldDB::new(dir.to_str().expect("utf-8 path"))
        .await
        .expect("create FoldDB");
    let mut fields = vec!["shard".to_string(), "doc_id".to_string()];
    fields.extend(COUNT_ROWS_DATA_FIELDS.iter().map(ToString::to_string));
    db.load_schema_from_json(
        &BenchmarkSchemaBuilder::new(COUNT_ROWS_SCHEMA)
            .fields(&fields.iter().map(String::as_str).collect::<Vec<_>>())
            .hash_key("shard")
            .range_key("doc_id")
            .build_json(),
    )
    .await
    .expect("load schema");

    let mut muts = Vec::with_capacity(n);
    for i in 0..n {
        let shard = format!("shard{}", i % 4);
        let doc_id = format!("doc{i:08}");
        let mut fv = HashMap::new();
        fv.insert("shard".to_string(), json!(shard));
        fv.insert("doc_id".to_string(), json!(doc_id));
        for data_field in COUNT_ROWS_DATA_FIELDS {
            fv.insert(data_field.to_string(), json!(format!("{data_field}-{i}")));
        }
        muts.push(Mutation::new(
            COUNT_ROWS_SCHEMA.to_string(),
            fv,
            KeyValue::new(Some(shard), Some(doc_id)),
            "pk".to_string(),
            MutationType::Create,
        ));
    }
    db.mutation_manager()
        .write_mutations_batch_async(muts, None)
        .await
        .expect("seed records");
    db
}

pub(crate) fn count_query(fields: &[&str]) -> Query {
    Query {
        schema_name: COUNT_ROWS_SCHEMA.to_string(),
        fields: fields.iter().map(ToString::to_string).collect(),
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

pub(crate) fn count_query_fields(field_count: usize) -> Vec<&'static str> {
    let mut fields = vec!["shard"];
    fields.extend(
        COUNT_ROWS_DATA_FIELDS[..field_count.saturating_sub(1)]
            .iter()
            .copied(),
    );
    fields
}

/// `count_query_rows` (the exact `total_count` for an unfiltered list query)
/// against a FIXED corpus, sweeping the NUMBER OF FIELDS requested. Each
/// requested field set includes the key field, so `count_rows` should choose
/// that count-bearing field and stay near-flat as more data fields are
/// requested.
pub(crate) fn bench_count_rows_vs_field_count(c: &mut Criterion) {
    if !requested_benchmark_filter_matches("count_rows_vs_field_count") {
        return;
    }

    std::env::set_var("FOLD_DISABLE_NATIVE_INDEX", "1");
    let rt = Runtime::new().expect("tokio runtime");

    let db = rt.block_on(seeded_count_rows_db(COUNT_ROWS_CORPUS));
    let ctx = AccessContext::owner("bench-owner");

    let mut group = c.benchmark_group("count_rows_vs_field_count");
    configure_guard_group(&mut group, 20);
    for field_count in COUNT_ROWS_FIELD_COUNTS {
        let fields = count_query_fields(field_count);
        // Sanity: the count must equal the seeded corpus size exactly.
        let counted = rt
            .block_on(
                db.query_executor()
                    .count_query_rows(&count_query(&fields), &ctx),
            )
            .expect("count_query_rows")
            .expect("schema found");
        assert_eq!(
            counted, COUNT_ROWS_CORPUS,
            "count_rows must report the exact live row count"
        );
        group.bench_with_input(
            BenchmarkId::from_parameter(field_count),
            &field_count,
            |b, _| {
                b.to_async(&rt).iter(|| async {
                    let res = db
                        .query_executor()
                        .count_query_rows(&count_query(&fields), &ctx)
                        .await
                        .expect("count_query_rows");
                    black_box(res);
                });
            },
        );
    }
    group.finish();
}

/// `count_query_rows` against a single count-bearing field while the corpus
/// grows. This guards the row-count axis independently from the field-count
/// sweep above.
pub(crate) fn bench_count_rows_vs_row_count(c: &mut Criterion) {
    if !requested_benchmark_filter_matches("count_rows_vs_row_count") {
        return;
    }

    std::env::set_var("FOLD_DISABLE_NATIVE_INDEX", "1");
    let rt = Runtime::new().expect("tokio runtime");
    let ctx = AccessContext::owner("bench-owner");

    let mut group = c.benchmark_group("count_rows_vs_row_count");
    configure_guard_group(&mut group, 10);
    for n in COUNT_ROWS_CARDINALITIES {
        let db = rt.block_on(seeded_count_rows_db(n));
        let fields = vec!["shard"];
        let counted = rt
            .block_on(
                db.query_executor()
                    .count_query_rows(&count_query(&fields), &ctx),
            )
            .expect("count_query_rows")
            .expect("schema found");
        assert_eq!(
            counted, n,
            "count_rows must report the exact live row count"
        );
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.to_async(&rt).iter(|| async {
                let res = db
                    .query_executor()
                    .count_query_rows(&count_query(&fields), &ctx)
                    .await
                    .expect("count_query_rows");
                black_box(res);
            });
        });
    }
    group.finish();
}
