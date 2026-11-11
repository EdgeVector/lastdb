//! Read and update the one catalog record in a source database.

use serde_json::{json, Value};

use crate::saved_query::{QueryCatalog, CATALOG_KEY, SAVED_QUERY_SCHEMA};

use super::http::{DatabaseTarget, Store};

const ATTEMPTS: u8 = 4;

pub(super) fn ids(store: &Store, source: &DatabaseTarget) -> Result<Vec<String>, String> {
    load(store, source).map(|catalog| catalog.ids)
}

pub(super) fn remember(store: &Store, source: &DatabaseTarget, id: &str) -> Result<(), String> {
    for attempt in 0..ATTEMPTS {
        let mut catalog = load(store, source)?;
        if !catalog
            .insert(id)
            .map_err(|error| error.as_str().to_owned())?
        {
            return Ok(());
        }
        match store_catalog(store, source, &catalog) {
            Ok(()) => return Ok(()),
            Err(error) if error.contains("409") && attempt + 1 < ATTEMPTS => continue,
            Err(error) => return Err(error),
        }
    }
    Err("saved query catalog changed".into())
}

pub(super) fn write_hash(
    store: &Store,
    source: &DatabaseTarget,
    schema: &str,
    hash: &str,
    record: Value,
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
    store.post(source, "/api/mutation", &body)?;
    Ok(())
}

fn load(store: &Store, source: &DatabaseTarget) -> Result<QueryCatalog, String> {
    let rows = store.query(source, SAVED_QUERY_SCHEMA, json!({"HashKey": CATALOG_KEY}))?;
    match rows.as_slice() {
        [] => Ok(QueryCatalog::empty()),
        [row] => QueryCatalog::from_row(row).map_err(|error| error.as_str().to_owned()),
        _ => Err("saved query catalog returned more than one record".into()),
    }
}

fn store_catalog(
    store: &Store,
    source: &DatabaseTarget,
    catalog: &QueryCatalog,
) -> Result<(), String> {
    write_hash(
        store,
        source,
        SAVED_QUERY_SCHEMA,
        CATALOG_KEY,
        catalog
            .fields()
            .map_err(|error| error.as_str().to_owned())?,
        catalog.revision.as_deref(),
    )
}
