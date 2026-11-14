//! Read and update the one catalog record in a source database.

use serde_json::{json, Value};

use crate::saved_query::{QueryCatalog, CATALOG_KEY, SAVED_QUERY_SCHEMA};

use super::store::QueryStore;

const ATTEMPTS: u8 = 4;

pub(super) async fn ids<S: QueryStore>(store: &S, source: &S::Db) -> Result<Vec<String>, String> {
    load(store, source).await.map(|catalog| catalog.ids)
}

pub(super) async fn remember<S: QueryStore>(
    store: &S,
    source: &S::Db,
    id: &str,
) -> Result<(), String> {
    for attempt in 0..ATTEMPTS {
        let mut catalog = load(store, source).await?;
        if !catalog
            .insert(id)
            .map_err(|error| error.as_str().to_owned())?
        {
            return Ok(());
        }
        match store_catalog(store, source, &catalog).await {
            Ok(()) => return Ok(()),
            Err(error) if error.contains("409") && attempt + 1 < ATTEMPTS => continue,
            Err(error) => return Err(error),
        }
    }
    Err("saved query catalog changed".into())
}

pub(super) async fn write_hash<S: QueryStore>(
    store: &S,
    source: &S::Db,
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
    store.mutate(source, &body).await?;
    Ok(())
}

async fn load<S: QueryStore>(store: &S, source: &S::Db) -> Result<QueryCatalog, String> {
    let rows = store
        .query(source, SAVED_QUERY_SCHEMA, &json!({"HashKey": CATALOG_KEY}))
        .await?;
    match rows.as_slice() {
        [] => Ok(QueryCatalog::empty()),
        [row] => QueryCatalog::from_row(row).map_err(|error| error.as_str().to_owned()),
        _ => Err("saved query catalog returned more than one record".into()),
    }
}

async fn store_catalog<S: QueryStore>(
    store: &S,
    source: &S::Db,
    catalog: &QueryCatalog,
) -> Result<(), String> {
    write_hash(
        store,
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
