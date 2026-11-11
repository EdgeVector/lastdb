//! `lastdb share queries save` and `lastdb share queries reconcile`.
//!
//! Both commands use the ordinary owner-socket routes. A sibling destination
//! is scoped to `/api/databases/<id>/...`, so the daemon opens that store
//! with the content key A derived for that id. This path does not read
//! identity seeds or personal content keys, and it is the same in every
//! environment.

use std::collections::BTreeSet;
use std::path::Path;

use database_policy::SpecBinding;
use lastdb_node::saved_query::{
    authorize, destination_binding, saved_query_declare, DeclareSchema, Definition, RawSchema,
    Receipt, RecordKey, SavedQuery, MAX_STORED_TEXT, SAVED_QUERY_SCHEMA,
};
use serde_json::{json, Value};

use crate::{DatabaseTarget, ShareQueriesCommand};

mod apply;
mod http;
mod select;

use apply::{apply, read_live};
use http::{find_database, ListedDatabase, Store};
use select::select;

pub(super) fn run(
    data_dir: Option<std::path::PathBuf>,
    action: ShareQueriesCommand,
) -> Result<(), String> {
    let store = Store::new(data_dir);
    match action {
        ShareQueriesCommand::Save {
            id,
            app,
            source,
            destination,
            spec,
            spec_version,
            query,
        } => save(
            &store,
            &Save {
                id,
                app,
                source,
                destination,
                spec,
                spec_version,
                query,
            },
        ),
        ShareQueriesCommand::Reconcile { id, source } => reconcile(&store, &id, &source),
    }
}

struct Save {
    id: String,
    app: String,
    source: String,
    destination: String,
    spec: String,
    spec_version: u32,
    query: std::path::PathBuf,
}

fn save(store: &Store, request: &Save) -> Result<(), String> {
    let definition_json = read_query(&request.query)?;
    let rows = store.list()?;
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
        &definition_json,
    )
    .map_err(|error| error.as_str().to_owned())?;
    require_source_owner(&rows, &request.source)?;
    let source = store.database(&request.source)?;
    let binding = binding_for(&destination, &request.spec, request.spec_version)?;
    prepare_source(store, &source, &request.app, &binding, &query.definition)?;
    if let Some(existing) = load_saved(store, &source, &request.id)? {
        if !existing.same_target(&destination.id, &request.spec, request.spec_version) {
            return Err("saved query destination or spec cannot change".into());
        }
        query.ledger = existing.ledger;
        query.revision = existing.revision;
    }
    store_query(store, &source, &query)?;
    println!(
        "{}",
        json!({
            "id": request.id,
            "source": request.source,
            "destination": destination.id,
            "spec_name": request.spec,
            "spec_version": request.spec_version,
        })
    );
    Ok(())
}

fn reconcile(store: &Store, id: &str, source_name: &str) -> Result<(), String> {
    let rows = store.list()?;
    require_source_owner(&rows, source_name)?;
    let source = store.database(source_name)?;
    let mut saved =
        load_saved(store, &source, id)?.ok_or("saved query is not in the source database")?;
    let destination = publish_target(
        &rows,
        &saved.destination,
        &saved.spec_name,
        saved.spec_version,
    )?;
    refuse_same_database(&rows, source_name, &destination.id)?;
    let binding = binding_for(&destination, &saved.spec_name, saved.spec_version)?;
    let declared = prepare_source(store, &source, &saved.app_id, &binding, &saved.definition)?;
    declare_all(store, &store.database(&destination.id)?, &declared)?;
    let desired = select(store, &source, &saved.definition, &declared)?;
    let dest = store.database(&destination.id)?;
    let keys = live_keys(&desired, &saved);
    let live = read_live(store, &dest, &keys)?;
    let plan = lastdb_node::saved_query::plan(desired, &live, &saved.ledger)
        .map_err(|error| error.as_str().to_owned())?;
    for step in &plan.steps {
        apply(store, &source, &dest, step)?;
    }
    if plan.ledger_changed {
        saved.ledger = plan.ledger;
        store_query(store, &source, &saved)?;
    }
    print_receipt(&plan.receipt);
    Ok(())
}

fn prepare_source(
    store: &Store,
    source: &DatabaseTarget,
    app_id: &str,
    binding: &SpecBinding,
    definition: &Definition,
) -> Result<Vec<DeclareSchema>, String> {
    let installed = load_installed(store, source, definition)?;
    let declared = authorize(app_id, binding, &installed, definition)
        .map_err(|error| error.as_str().to_owned())?;
    declare_all(store, source, &[saved_query_declare()])?;
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

fn load_installed(
    store: &Store,
    source: &DatabaseTarget,
    definition: &Definition,
) -> Result<Vec<RawSchema>, String> {
    let mut installed = Vec::new();
    for (namespace, name) in definition.schema_refs() {
        let qualified = format!("{namespace}/{name}");
        let value = store.schema(source, &qualified)?;
        installed.push(
            RawSchema::from_installed(namespace, name, &value)
                .map_err(|error| error.as_str().to_owned())?,
        );
    }
    Ok(installed)
}

fn declare_all(
    store: &Store,
    database: &DatabaseTarget,
    schemas: &[DeclareSchema],
) -> Result<(), String> {
    for schema in schemas {
        let response = store.post(database, "/api/schemas/declare", &schema.body())?;
        if response.get("canonical").and_then(Value::as_str) != Some(schema.qualified().as_str()) {
            return Err("schema declaration resolved to an unexpected name".into());
        }
    }
    Ok(())
}

fn load_saved(
    store: &Store,
    source: &DatabaseTarget,
    id: &str,
) -> Result<Option<SavedQuery>, String> {
    let rows = store.query(source, SAVED_QUERY_SCHEMA, json!({"HashKey": id}))?;
    match rows.as_slice() {
        [] => Ok(None),
        [row] => SavedQuery::from_row(id, row)
            .map(Some)
            .map_err(|error| error.as_str().to_owned()),
        _ => Err("saved query id returned more than one record".into()),
    }
}

fn store_query(store: &Store, source: &DatabaseTarget, query: &SavedQuery) -> Result<(), String> {
    let (operation, expected) = match query.revision.as_deref() {
        None => ("create", json!({"type": "absent"})),
        Some(revision) => ("replace", json!({"type": "revision", "revision": revision})),
    };
    let body = json!({
        "type": "mutation",
        "schema": SAVED_QUERY_SCHEMA,
        "key_value": {"hash": query.id},
        "record": query.fields().map_err(|error| error.as_str().to_owned())?,
        "alternates": {},
        "mutation_type": operation,
        "expected": expected,
        "durability": "durable",
        "convergence": "sync",
    });
    store.post(source, "/api/mutation", &body)?;
    Ok(())
}

fn live_keys(
    desired: &[lastdb_node::saved_query::Projected],
    saved: &SavedQuery,
) -> Vec<RecordKey> {
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

fn print_receipt(receipt: &Receipt) {
    println!(
        "{}",
        json!({
            "added": receipt.added,
            "updated": receipt.updated,
            "removed": receipt.removed,
            "unchanged": receipt.unchanged,
        })
    );
}

fn read_query(path: &Path) -> Result<String, String> {
    let bytes = std::fs::read(path).map_err(|_| "saved query file is unreadable")?;
    if bytes.len() > MAX_STORED_TEXT {
        return Err("saved query definition is too large".into());
    }
    String::from_utf8(bytes).map_err(|_| "saved query file is not UTF-8".into())
}
