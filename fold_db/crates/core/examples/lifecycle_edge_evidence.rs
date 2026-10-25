//! One-time approved generated-data evidence. Close this PR without merge.

#![recursion_limit = "512"]

use fold_db::benchmark_support::BenchmarkSchemaBuilder;
use fold_db::lifecycle_ci_evidence::{inject_wrong_ages, open};
use fold_db::schema::types::{field::HashRangeFilter, operations::Query, KeyValue, Mutation};
use fold_db::schema::SchemaRetentionPolicy;
use fold_db::security::Ed25519KeyPair;
use fold_db::storage::{
    disk_pack::{NativeMaintenanceOptions, PackNamespacedStore},
    traits::NamespacedStore,
};
use fold_db::{FoldDB, MutationType};
use rand::RngCore;
use serde_json::{json, Value};
use std::{collections::HashMap, path::Path, sync::Arc, time::Duration};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const SCHEMA: &str = "CiWrongHashAges";

#[tokio::main]
async fn main() -> Result<()> {
    if std::env::var("GITHUB_ACTIONS").as_deref() != Ok("true") {
        return Err("lifecycle edge evidence is CI only".into());
    }
    for format in [1, 2] {
        let result = exercise(format).await?;
        println!("{}", serde_json::to_string(&result)?);
    }
    Ok(())
}

async fn exercise(format: u8) -> Result<Value> {
    let temp = tempfile::tempdir()?;
    let home = temp.path().join("generated-home");
    let mut key = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut key);
    let signer = Arc::new(Ed25519KeyPair::generate()?);
    let store = if format == 2 {
        PackNamespacedStore::create_run(&home, &key)?
    } else {
        PackNamespacedStore::open(&home)?
    };
    let runtime = store.disk_pack_runtime().ok_or("native runtime missing")?;
    let merge_before = runtime.ci_run_status();
    let db = open(Arc::new(store.clone()), Arc::clone(&signer)).await?;
    stage(format, "opened");
    let schema = BenchmarkSchemaBuilder::new(SCHEMA)
        .fields(&["id", "value"])
        .hash_key("id")
        .build_json();
    db.load_schema_from_json(&schema).await?;
    let mutations: Vec<_> = (0..32).map(|n| mutation(n, &signer)).collect();
    db.mutation_manager()
        .write_mutations_batch_async(mutations, None)
        .await?;
    db.flush_accepted_mutations(Duration::from_secs(30)).await?;
    db.db_ops()
        .set_schema_retention_policy(SCHEMA, SchemaRetentionPolicy::operator(3600, vec![]))
        .await?;
    stage(format, "wrote_keys");
    let keys: Vec<_> = (0..32)
        .map(|n| KeyValue::new(Some(format!("key-{n:04}")), None))
        .collect();
    let clocks = inject_wrong_ages(&db, SCHEMA, &keys).await?;
    if clocks.len() != keys.len() {
        return Err("did not read every generated tip clock".into());
    }
    db.db_ops()
        .schemas()
        .ci_check_ages(SCHEMA, &clocks, false)
        .await?;
    stage(format, "wrong_ages_observed");
    let repaired = db.backfill_schema_retention_page(SCHEMA, None, 64).await?;
    if repaired["tracked"] != 32 || repaired["complete"] != true {
        return Err("incomplete age backfill".into());
    }
    db.db_ops()
        .schemas()
        .ci_check_ages(SCHEMA, &clocks, true)
        .await?;
    // A second owner pass must preserve original clocks rather than refresh them.
    db.backfill_schema_retention_page(SCHEMA, None, 64).await?;
    db.db_ops()
        .schemas()
        .ci_check_ages(SCHEMA, &clocks, true)
        .await?;
    check_values(&db).await?;
    stage(format, "age_repair_repeat_values");
    let personal = db
        .db_ops()
        .atoms()
        .ci_generation_cleanup_evidence(None)
        .await?;
    stage(format, "personal_generation_cleanup");
    let scope = "a".repeat(64);
    let scoped = db
        .db_ops()
        .atoms()
        .ci_generation_cleanup_evidence(Some(&scope))
        .await?;
    stage(format, "scoped_generation_cleanup");
    let maintenance = maintenance(&runtime, format).await?;
    let merged = wait_for_merge(&runtime, format, &merge_before).await?;
    check_values(&db).await?;
    db.shutdown_local_only().await?;
    drop((db, runtime, store));
    let store = reopen(&home, &key).await?;
    let db = open(Arc::new(store.clone()), signer).await?;
    db.db_ops()
        .schemas()
        .ci_check_ages(SCHEMA, &clocks, true)
        .await?;
    check_values(&db).await?;
    db.shutdown_local_only().await?;
    drop((db, store));
    temp.close()?;
    Ok(json!({"complete": true, "format": format, "wrong_ages": 32,
        "repaired_forward_reverse": 32, "repeat_unchanged": true, "values_after_restart": 32,
        "personal": personal, "scoped": scoped, "maintenance": maintenance,
        "merge_before": merge_before, "merge_after": merged, "generated_home_removed": true}))
}

fn mutation(n: usize, signer: &Ed25519KeyPair) -> Mutation {
    let key = format!("key-{n:04}");
    let mut mutation = Mutation::new(
        SCHEMA.into(),
        HashMap::from([
            ("id".into(), json!(key)),
            ("value".into(), json!(format!("value-{n:04}"))),
        ]),
        KeyValue::new(Some(key), None),
        signer.public_key_base64(),
        MutationType::Create,
    );
    mutation.synchronous = Some(true);
    mutation
}

async fn check_values(db: &FoldDB) -> Result<()> {
    let keys = (0..32)
        .map(|n| (format!("key-{n:04}"), String::new()))
        .collect();
    let rows = db
        .query_executor()
        .query(Query::new_with_filter(
            SCHEMA.into(),
            vec!["id".into(), "value".into()],
            Some(HashRangeFilter::HashRangeKeys(keys)),
        ))
        .await?;
    for field in ["id", "value"] {
        let values = rows.get(field).ok_or("missing generated field")?;
        if values.len() != 32 {
            return Err("incomplete generated keyed values".into());
        }
        for n in 0..32 {
            let key = KeyValue::new(Some(format!("key-{n:04}")), None);
            let value = values.get(&key).ok_or("missing generated key")?;
            let expected = if field == "id" {
                format!("key-{n:04}")
            } else {
                format!("value-{n:04}")
            };
            if value.value != expected {
                return Err("generated value changed".into());
            }
        }
    }
    Ok(())
}

async fn wait_for_merge(
    runtime: &Arc<fold_db::storage::disk_pack::PackRuntime>,
    format: u8,
    before: &Value,
) -> Result<Value> {
    for _ in 0..300 {
        let observed = runtime.ci_run_status();
        if format == 1
            || observed["merges"].as_u64().unwrap_or(0) > before["merges"].as_u64().unwrap_or(0)
        {
            return Ok(observed);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err("no actual v2 background run merge completed".into())
}

async fn reopen(home: &Path, key: &[u8; 32]) -> Result<PackNamespacedStore> {
    for _ in 0..100 {
        match PackNamespacedStore::open_keyed(home, Some(key)) {
            Ok(store) => return Ok(store),
            Err(error) if error.to_string().contains("another owner") => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Err("generated home retained an owner after shutdown".into())
}

async fn maintenance(
    runtime: &Arc<fold_db::storage::disk_pack::PackRuntime>,
    format: u8,
) -> Result<Value> {
    let compact = runtime
        .maintain_native(NativeMaintenanceOptions {
            dry_run: false,
            reclaim_only: false,
            full_copy: true,
            grace_ms: None,
            report_usage: false,
            reclaim_generations: None,
        })
        .await;
    let result = if format == 2 {
        match compact {
            Err(error)
                if error
                    .to_string()
                    .contains("this operation is not in format v2") =>
            {
                json!({"v1_compaction_refused": error.to_string()})
            }
            _ => return Err("v2 silently accepted unsupported v1 compaction".into()),
        }
    } else {
        let report = compact?;
        if !report.executed {
            return Err("v1 full compaction did not execute".into());
        }
        serde_json::to_value(report)?
    };
    Ok(result)
}

fn stage(format: u8, name: &str) {
    println!("{}", json!({"stage": name, "format": format}));
}
