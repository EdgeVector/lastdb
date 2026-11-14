//! Save and reconcile one keyed query directly against the node's stores.
//!
//! Saved queries live in an owner-node source database. `reconcile_catalog`
//! runs the same reconcile as the manual command for every id in the source
//! catalog without opening a loopback socket connection.

use std::collections::BTreeSet;

use database_policy::SpecBinding;
use serde_json::{json, Value};

use crate::host::Host;
use crate::saved_query::{
    authorize, destination_binding, label_ok, saved_query_declare, DeclareSchema, Definition,
    RawSchema, Receipt, RecordKey, SavedQuery, CATALOG_KEY, SAVED_QUERY_SCHEMA,
};

mod apply;
mod catalog;
mod select;
mod store;

use apply::{apply, read_live};
use catalog::write_hash;
use select::select;
use store::{find_database, list_databases, ListedDatabase, TargetStore};

/// Arguments for saving a keyed query on the owner node.
pub(crate) struct SaveRequest {
    pub id: String,
    pub app: String,
    pub source: String,
    pub destination: String,
    pub spec: String,
    pub spec_version: u32,
    pub definition_json: String,
}

/// Receipt returned after saving a keyed query.
pub(crate) struct SaveReceipt {
    pub id: String,
    pub source: String,
    pub destination: String,
    pub spec_name: String,
    pub spec_version: u32,
}

pub(crate) async fn save(host: &Host, request: &SaveRequest) -> Result<SaveReceipt, String> {
    let rows = list_databases(host)?;
    let destination = publish_target(
        &rows,
        &request.destination,
        &request.spec,
        request.spec_version,
    )?;
    refuse_same_database(&rows, &request.source, &destination.id)?;
    let mut query = SavedQuery::create(
        &request.id,
        &request.app,
        &destination.id,
        &request.spec,
        request.spec_version,
        &request.definition_json,
    )
    .map_err(|error| error.as_str().to_owned())?;
    require_source_owner(&rows, &request.source)?;
    let source = TargetStore::open(host, &request.source)?;
    let binding = binding_for(&destination, &request.spec, request.spec_version)?;
    prepare_source(&source, &request.app, &binding, &query.definition).await?;
    if let Some(existing) = load_saved(&source, &request.id).await? {
        if !existing.same_target(&destination.id, &request.spec, request.spec_version) {
            return Err("saved query destination or spec cannot change".into());
        }
        query.ledger = existing.ledger;
        query.revision = existing.revision;
    }
    store_query(&source, &query).await?;
    catalog::remember(&source, &request.id).await?;
    Ok(SaveReceipt {
        id: request.id.clone(),
        source: request.source.clone(),
        destination: destination.id,
        spec_name: request.spec.clone(),
        spec_version: request.spec_version,
    })
}

pub(crate) async fn reconcile(host: &Host, id: &str, source_name: &str) -> Result<Receipt, String> {
    reconcile_query(host, id, source_name, true).await
}

/// Reconcile every saved query in `source` after that store commits.
///
/// A missing saved-query schema means this store has no catalog. That is
/// a quiet success, including on a destination store and before the first
/// save. One failed query does not skip the others and does not write its
/// ledger.
pub(crate) async fn reconcile_catalog(host: &Host, source: &str) -> Result<(), String> {
    let database = TargetStore::open(host, source)?;
    let ids = match catalog::ids(&database).await {
        Ok(ids) => ids,
        Err(error) if catalog_absent(&error) => return Ok(()),
        Err(error) => return Err(error),
    };
    for id in ids {
        if let Err(error) = reconcile_query(host, &id, source, false).await {
            tracing::warn!(
                target: "lastdbd::saved_query",
                query_id = %id,
                error = %error,
                "saved query was not reconciled"
            );
        }
    }
    Ok(())
}

fn catalog_absent(error: &str) -> bool {
    error.contains("App schema not loaded") || error.contains("Schema not found")
}

async fn reconcile_query(
    host: &Host,
    id: &str,
    source_name: &str,
    enroll: bool,
) -> Result<Receipt, String> {
    if id == CATALOG_KEY || !label_ok(id) {
        return Err("saved query name is invalid".into());
    }
    let rows = list_databases(host)?;
    require_source_owner(&rows, source_name)?;
    let source = TargetStore::open(host, source_name)?;
    let mut saved = load_saved(&source, id)
        .await?
        .ok_or("saved query is not in the source database")?;
    let destination = publish_target(
        &rows,
        &saved.destination,
        &saved.spec_name,
        saved.spec_version,
    )?;
    refuse_same_database(&rows, source_name, &destination.id)?;
    let binding = binding_for(&destination, &saved.spec_name, saved.spec_version)?;
    let declared = prepare_source(&source, &saved.app_id, &binding, &saved.definition).await?;
    let dest = TargetStore::open(host, &destination.id)?;
    declare_all(&dest, &declared).await?;
    let desired = select(&source, &saved.definition, &declared).await?;
    let keys = live_keys(&desired, &saved);
    let live = read_live(&dest, &keys).await?;
    let plan = crate::saved_query::plan(desired, &live, &saved.ledger)
        .map_err(|error| error.as_str().to_owned())?;
    for step in &plan.steps {
        apply(&source, &dest, step).await?;
    }
    if plan.ledger_changed {
        saved.ledger = plan.ledger;
        store_query(&source, &saved).await?;
    }
    if enroll {
        catalog::remember(&source, id).await?;
    }
    Ok(plan.receipt)
}

async fn prepare_source(
    source: &TargetStore<'_>,
    app_id: &str,
    binding: &SpecBinding,
    definition: &Definition,
) -> Result<Vec<DeclareSchema>, String> {
    let installed = load_installed(source, definition).await?;
    let declared = authorize(app_id, binding, &installed, definition)
        .map_err(|error| error.as_str().to_owned())?;
    declare_all(source, &[saved_query_declare()]).await?;
    Ok(declared)
}

fn publish_target(
    rows: &[Value],
    selector: &str,
    spec_name: &str,
    spec_version: u32,
) -> Result<ListedDatabase, String> {
    let row = find_database(rows, selector)?;
    binding_for(&row, spec_name, spec_version)?;
    Ok(row)
}

fn binding_for(
    row: &ListedDatabase,
    spec_name: &str,
    spec_version: u32,
) -> Result<SpecBinding, String> {
    match row.kind.as_deref() {
        None => return Err("destination database has no kind".into()),
        Some("shareable") => {}
        Some(_) => return Err("destination database is not a shareable database".into()),
    }
    if row.name.is_none() {
        return Err("destination database is not an owned named database".into());
    }
    if row.access.as_deref() != Some("owner") {
        return Err("destination database is not an owned database".into());
    }
    if row.state.as_deref() != Some("ready") {
        return Err("destination database is not ready".into());
    }
    let bound_name = row
        .spec_name
        .as_deref()
        .ok_or("destination database has no spec binding")?;
    let bound_version = row
        .spec_version
        .ok_or("destination database has no spec binding")?;
    if bound_name != spec_name || bound_version != spec_version {
        return Err("saved query spec does not match the destination database".into());
    }
    destination_binding(bound_name, bound_version).map_err(|error| error.as_str().to_owned())
}

fn require_source_owner(rows: &[Value], source: &str) -> Result<(), String> {
    match find_database(rows, source) {
        Ok(row) if row.access.as_deref() == Some("owner") => Ok(()),
        Ok(_) => Err("source database is not an owned database".into()),
        Err(_) if source == "personal" => Ok(()),
        Err(error) => Err(error),
    }
}

fn refuse_same_database(rows: &[Value], source: &str, destination_id: &str) -> Result<(), String> {
    let source_id = find_database(rows, source).ok().map(|row| row.id);
    if source == destination_id || source_id.as_deref() == Some(destination_id) {
        Err("saved query source and destination are the same database".into())
    } else {
        Ok(())
    }
}

async fn load_installed(
    source: &TargetStore<'_>,
    definition: &Definition,
) -> Result<Vec<RawSchema>, String> {
    let mut installed = Vec::new();
    for (namespace, name) in definition.schema_refs() {
        let qualified = format!("{namespace}/{name}");
        let value = source.schema(&qualified).await?;
        installed.push(
            RawSchema::from_installed(namespace, name, &value)
                .map_err(|error| error.as_str().to_owned())?,
        );
    }
    Ok(installed)
}

async fn declare_all(database: &TargetStore<'_>, schemas: &[DeclareSchema]) -> Result<(), String> {
    for schema in schemas {
        let response = database.declare(&schema.body()).await?;
        if response.get("canonical").and_then(Value::as_str) != Some(schema.qualified().as_str()) {
            return Err("schema declaration resolved to an unexpected name".into());
        }
    }
    Ok(())
}

async fn load_saved(source: &TargetStore<'_>, id: &str) -> Result<Option<SavedQuery>, String> {
    let rows = source
        .query(SAVED_QUERY_SCHEMA, &json!({"HashKey": id}))
        .await?;
    match rows.as_slice() {
        [] => Ok(None),
        [row] => SavedQuery::from_row(id, row)
            .map(Some)
            .map_err(|error| error.as_str().to_owned()),
        _ => Err("saved query id returned more than one record".into()),
    }
}

async fn store_query(source: &TargetStore<'_>, query: &SavedQuery) -> Result<(), String> {
    write_hash(
        source,
        SAVED_QUERY_SCHEMA,
        &query.id,
        &query.fields().map_err(|error| error.as_str().to_owned())?,
        query.revision.as_deref(),
    )
    .await
}

fn live_keys(desired: &[crate::saved_query::Projected], saved: &SavedQuery) -> Vec<RecordKey> {
    let mut seen = BTreeSet::new();
    let mut keys = Vec::new();
    for row in desired {
        if seen.insert(row.key.clone()) {
            keys.push(row.key.clone());
        }
    }
    for entry in &saved.ledger {
        let key = entry.key();
        if seen.insert(key.clone()) {
            keys.push(key);
        }
    }
    keys
}
