//! Read and update the one catalog record in a source database.

use serde_json::{json, Value};

use crate::saved_query::{QueryCatalog, CATALOG_KEY, SAVED_QUERY_SCHEMA};

use super::store::TargetStore;

const ATTEMPTS: u8 = 4;

pub(super) async fn ids(source: &TargetStore<'_>) -> Result<Vec<String>, String> {
    load(source).await.map(|catalog| catalog.ids)
}

pub(super) async fn remember(source: &TargetStore<'_>, id: &str) -> Result<(), String> {
    for attempt in 0..ATTEMPTS {
        let mut catalog = load(source).await?;
        if !catalog
            .insert(id)
            .map_err(|error| error.as_str().to_owned())?
        {
            return Ok(());
        }
        match store_catalog(source, &catalog).await {
            Ok(()) => return Ok(()),
            Err(error) if error.contains("409") && attempt + 1 < ATTEMPTS => continue,
            Err(error) => return Err(error),
        }
    }
    Err("saved query catalog changed".into())
}

pub(super) async fn write_hash(
    source: &TargetStore<'_>,
    schema: &str,
    hash: &str,
    record: &Value,
    revision: Option<&str>,
) -> Result<(), String> {
    let (operation, expected) = match revision {
        None => ("create", json!({"type": "absent"})),
        Some(revision) => ("replace", json!({"type": "revision", "revision": revision})),
    };
    let body = json!({
        "type": "mutation",
        "schema": schema,
        "key_value": {"hash": hash},
        "record": record,
        "alternates": {},
        "mutation_type": operation,
        "expected": expected,
        "durability": "durable",
        "convergence": "sync",
    });
    source.mutate(&body).await?;
    Ok(())
}

async fn load(source: &TargetStore<'_>) -> Result<QueryCatalog, String> {
    let rows = source
        .query(SAVED_QUERY_SCHEMA, &json!({"HashKey": CATALOG_KEY}))
        .await?;
    match rows.as_slice() {
        [] => Ok(QueryCatalog::empty()),
        [row] => QueryCatalog::from_row(row).map_err(|error| error.as_str().to_owned()),
        _ => Err("saved query catalog returned more than one record".into()),
    }
}

async fn store_catalog(source: &TargetStore<'_>, catalog: &QueryCatalog) -> Result<(), String> {
    write_hash(
        source,
        SAVED_QUERY_SCHEMA,
        CATALOG_KEY,
        &catalog
            .fields()
            .map_err(|error| error.as_str().to_owned())?,
        catalog.revision.as_deref(),
    )
    .await
}
