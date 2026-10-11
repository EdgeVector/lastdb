use std::collections::HashMap;

use fold_db::benchmark_support::BenchmarkSchemaBuilder;
use fold_db::fold_db_core::FoldDB;
use fold_db::schema::types::operations::MutationType;
use fold_db::schema::types::{KeyValue, Mutation};
use serde_json::json;

use super::guard::SCHEMA;

/// A representative ~280-byte message body so the realistic-shape group writes
/// records with genuine heft (not trivial scalars), the way a messages/memory
/// schema would.
pub(crate) const MESSAGE_BODY: &str =
    "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do \
eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis \
nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat duis aute.";

/// Build a `FoldDB` with a HashRange `Contacts` schema and `n` records written
/// through the real mutation path. The temp dir is leaked so the path stays
/// valid for the lifetime of the bench group.
pub(crate) async fn seeded_db(n: usize) -> FoldDB {
    let dir = tempfile::tempdir().expect("temp dir").keep();
    let db = FoldDB::new(dir.to_str().expect("utf-8 path"))
        .await
        .expect("create FoldDB");
    db.load_schema_from_json(
        &BenchmarkSchemaBuilder::new(SCHEMA)
            .fields(&["full_name", "email", "content_hash"])
            .hash_key("full_name")
            .range_key("content_hash")
            .build_json(),
    )
    .await
    .expect("load schema");

    let mut muts = Vec::with_capacity(n);
    for i in 0..n {
        let full_name = format!("Person {i}");
        let content_hash = format!("ch{i}");
        let mut fv = HashMap::new();
        fv.insert("full_name".to_string(), json!(full_name));
        fv.insert("content_hash".to_string(), json!(content_hash));
        fv.insert("email".to_string(), json!(format!("p{i}@example.com")));
        muts.push(Mutation::new(
            SCHEMA.to_string(),
            fv,
            KeyValue::new(Some(full_name), Some(content_hash)),
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

/// Number of rows the `value_filter_scan` predicate is tuned to match,
/// independent of corpus size. The scan loads the WHOLE `score` field, then
/// `apply_value_filters` keeps only the rows whose `score` lands in a fixed
/// window — so the RESULT stays ~constant (`VALUE_FILTER_RESULT_ROWS`) while the
/// corpus grows. That fixed-result-in-a-growing-corpus shape is what makes the
/// O(corpus) scan cost visible and what the baseline guards against.
pub(crate) const VALUE_FILTER_RESULT_ROWS: usize = 100;

/// Build a `FoldDB` whose `Reviews` schema carries a NUMERIC `score` field, with
/// `n` records written through the real mutation path. `score` is assigned so
/// that exactly `VALUE_FILTER_RESULT_ROWS` rows fall in `[0.0, 1.0)` regardless
/// of `n`: the first `VALUE_FILTER_RESULT_ROWS` rows get a fractional score in
/// `[0, 1)`, every later row gets a score `>= 10.0`. A `value_filters` predicate
/// of `LessThan { score, 1.0 }` therefore matches a FIXED ~100-row subset while
/// the corpus (and thus the scanned field) grows — isolating the scan cost from
/// the result size, the shape behind the 2026-06-20 `SampleN(10_000)` abuse.
pub(crate) async fn seeded_scored_db(n: usize) -> FoldDB {
    const REVIEW_SCHEMA: &str = "Reviews";
    let dir = tempfile::tempdir().expect("temp dir").keep();
    let db = FoldDB::new(dir.to_str().expect("utf-8 path"))
        .await
        .expect("create FoldDB");
    db.load_schema_from_json(
        &BenchmarkSchemaBuilder::new(REVIEW_SCHEMA)
            .fields(&["review_id", "score", "created_at"])
            .hash_key("review_id")
            .range_key("created_at")
            .build_json(),
    )
    .await
    .expect("load schema");

    let mut muts = Vec::with_capacity(n);
    for i in 0..n {
        let review_id = format!("review-{i}");
        let created_at = format!("2026-06-{:02}T{:02}:00:00Z", i % 28 + 1, i % 24);
        // First VALUE_FILTER_RESULT_ROWS rows score in [0, 1); the rest score
        // >= 10.0, so `score < 1.0` always matches exactly that fixed subset.
        let score = if i < VALUE_FILTER_RESULT_ROWS {
            (i as f64) / (VALUE_FILTER_RESULT_ROWS as f64)
        } else {
            10.0 + (i % 90) as f64
        };
        let mut fv = HashMap::new();
        fv.insert("review_id".to_string(), json!(review_id));
        fv.insert("score".to_string(), json!(score));
        fv.insert("created_at".to_string(), json!(created_at.clone()));
        muts.push(Mutation::new(
            REVIEW_SCHEMA.to_string(),
            fv,
            KeyValue::new(Some(review_id), Some(created_at)),
            "pk".to_string(),
            MutationType::Create,
        ));
    }
    db.mutation_manager()
        .write_mutations_batch_async(muts, None)
        .await
        .expect("seed scored records");
    db
}

/// Realistic message-shaped corpus: a high-cardinality `Messages` schema where
/// each record carries a `sender`, a `sent_at` range key, and a ~280-byte
/// `body`. This mirrors a real messages / memory-atom workload (heavier records,
/// distinct keys) so the point-lookup-stays-flat claim is validated against a
/// realistic shape, not just tiny scalar rows.
pub(crate) async fn seeded_messages(n: usize) -> FoldDB {
    const MSG_SCHEMA: &str = "Messages";
    let dir = tempfile::tempdir().expect("temp dir").keep();
    let db = FoldDB::new(dir.to_str().expect("utf-8 path"))
        .await
        .expect("create FoldDB");
    db.load_schema_from_json(
        &BenchmarkSchemaBuilder::new(MSG_SCHEMA)
            .fields(&["msg_id", "sender", "sent_at", "body"])
            .hash_key("msg_id")
            .range_key("sent_at")
            .build_json(),
    )
    .await
    .expect("load schema");

    let mut muts = Vec::with_capacity(n);
    for i in 0..n {
        let msg_id = format!("msg-{i}");
        let sent_at = format!(
            "2026-06-{:02}T{:02}:{:02}:{:02}Z",
            i % 28 + 1,
            i % 24,
            i % 60,
            i % 60
        );
        let mut fv = HashMap::new();
        fv.insert("msg_id".to_string(), json!(msg_id));
        fv.insert("sender".to_string(), json!(format!("user{}", i % 5_000)));
        fv.insert("sent_at".to_string(), json!(sent_at.clone()));
        fv.insert("body".to_string(), json!(format!("[{i}] {MESSAGE_BODY}")));
        muts.push(Mutation::new(
            MSG_SCHEMA.to_string(),
            fv,
            KeyValue::new(Some(msg_id), Some(sent_at)),
            "pk".to_string(),
            MutationType::Create,
        ));
    }
    db.mutation_manager()
        .write_mutations_batch_async(muts, None)
        .await
        .expect("seed messages");
    db
}

/// Fixed corpus size for the schema-count sweep. Held small + constant so the
/// only axis that varies is the registry size, not the field cardinality.
pub(crate) const SCHEMA_COUNT_CORPUS: usize = 1_000;

/// Build a `FoldDB` with the target `Contacts` schema + a fixed `corpus`, then
/// load `extra_schemas` ADDITIONAL distinct schemas into the registry — and
/// supersede a fraction of them so the `superseded_by` version chain also grows.
/// This reproduces the production load shape behind the 2026-06-20 :9001 wedge:
/// Tom's brain holds dozens–hundreds of schema versions, and a query that
/// deep-clones the whole registry per call scales with THAT count, not corpus.
///
/// The extra schemas are real, Available, queryable schemas (one hash + one
/// range field each, like the target) so they land in exactly the same registry
/// map `get_schema_following_supersession` clones on the hot path.
pub(crate) async fn seeded_db_with_schemas(corpus: usize, extra_schemas: usize) -> FoldDB {
    let db = seeded_db(corpus).await;

    // Load `extra_schemas` distinct schemas so the registry the query path
    // clones grows from 1 to 1 + extra_schemas entries.
    for k in 0..extra_schemas {
        let name = format!("Extra{k}");
        db.load_schema_from_json(
            &BenchmarkSchemaBuilder::new(&name)
                .fields(&["k", "v"])
                .hash_key("k")
                .range_key("v")
                .build_json(),
        )
        .await
        .expect("load extra schema");
    }

    // Grow the superseded_by version chain too: supersede every 5th extra schema
    // onto its predecessor. This exercises the version-count axis (superseded
    // versions still occupy the registry + the superseded_by map the resolver
    // consults) without redirecting the TARGET schema the bench queries.
    let schema_core = db.schema_manager();
    let mut prev: Option<String> = None;
    for k in 0..extra_schemas {
        let name = format!("Extra{k}");
        if k % 5 == 0 {
            if let Some(ref successor) = prev {
                // Block `name`, redirecting it to a still-Available successor.
                schema_core
                    .block_and_supersede(&name, successor)
                    .await
                    .expect("supersede extra schema");
            }
        }
        prev = Some(name);
    }

    db
}
