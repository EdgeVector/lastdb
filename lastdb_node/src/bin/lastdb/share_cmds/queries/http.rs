//! Owner-socket calls for one source database and one destination database.

use std::path::PathBuf;
use std::time::Duration;

use serde_json::{json, Value};

use crate::{fetch_schema, get_path, parse_json_response, post_json_admin, DatabaseTarget};
use lastdb_node::lastdb_uds::database_path::DATABASES_PATH;
use lastdb_node::saved_query::MAX_RECORDS;

pub(super) struct Store {
    data_dir: Option<PathBuf>,
}

impl Store {
    pub(super) fn new(data_dir: Option<PathBuf>) -> Self {
        Self { data_dir }
    }

    /// `personal` stays on the unscoped owner routes. Every other selector
    /// uses the scoped database routes.
    pub(super) fn database(&self, selector: &str) -> Result<DatabaseTarget, String> {
        let selector = (selector != "personal").then_some(selector);
        DatabaseTarget::open(self.data_dir.clone(), selector)
    }

    pub(super) fn list(&self) -> Result<Vec<Value>, String> {
        let target = DatabaseTarget::open(self.data_dir.clone(), None)?;
        let raw = get_path(&target.socket, DATABASES_PATH, Duration::from_secs(10))?;
        let response = parse_json_response(&raw, "database list")?;
        response
            .get("databases")
            .and_then(Value::as_array)
            .cloned()
            .ok_or("missing database list".to_owned())
    }

    pub(super) fn post(
        &self,
        database: &DatabaseTarget,
        path: &str,
        body: &Value,
    ) -> Result<Value, String> {
        let raw = post_json_admin(&database.socket, &database.path(path), body)?;
        parse_json_response(&raw, path)
    }

    pub(super) fn schema(&self, database: &DatabaseTarget, name: &str) -> Result<Value, String> {
        fetch_schema(database, name).map_err(|error| {
            if error.contains("404") {
                format!("source schema {name} is not installed")
            } else {
                error
            }
        })
    }

    pub(super) fn query(
        &self,
        database: &DatabaseTarget,
        schema: &str,
        filter: Value,
    ) -> Result<Vec<Value>, String> {
        let body = json!({
            "schema_name": schema,
            "filter": filter,
            "limit": MAX_RECORDS,
        });
        let response = self.post(database, "/api/query", &body)?;
        if response.get("has_more").and_then(Value::as_bool) == Some(true) {
            return Err("saved query read exceeds 256 records".into());
        }
        response
            .get("results")
            .and_then(Value::as_array)
            .cloned()
            .ok_or("query returned no results".to_owned())
    }
}

pub(super) struct ListedDatabase {
    pub(super) id: String,
    pub(super) name: Option<String>,
    pub(super) kind: Option<String>,
    pub(super) access: Option<String>,
    pub(super) state: Option<String>,
    pub(super) spec_name: Option<String>,
    pub(super) spec_version: Option<u32>,
}

pub(super) fn find_database(rows: &[Value], selector: &str) -> Result<ListedDatabase, String> {
    let row = rows
        .iter()
        .find(|row| {
            row.get("id").and_then(Value::as_str) == Some(selector)
                || row.get("name").and_then(Value::as_str) == Some(selector)
        })
        .ok_or("database is not registered on this node")?;
    listed(row)
}

fn listed(row: &Value) -> Result<ListedDatabase, String> {
    let id = row
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .ok_or("database has no ID")?
        .to_owned();
    let (spec_name, spec_version) = listed_spec(row.get("spec"))?;
    Ok(ListedDatabase {
        id,
        name: row.get("name").and_then(Value::as_str).map(str::to_owned),
        kind: row.get("kind").and_then(Value::as_str).map(str::to_owned),
        access: row.get("access").and_then(Value::as_str).map(str::to_owned),
        state: row.get("state").and_then(Value::as_str).map(str::to_owned),
        spec_name,
        spec_version,
    })
}

fn listed_spec(spec: Option<&Value>) -> Result<(Option<String>, Option<u32>), String> {
    match spec {
        None | Some(Value::Null) => Ok((None, None)),
        Some(spec) => {
            let name = spec
                .get("name")
                .and_then(Value::as_str)
                .ok_or("destination database has no spec binding")?;
            let version = spec
                .get("version")
                .and_then(Value::as_u64)
                .ok_or("destination database has no spec binding")?;
            let version =
                u32::try_from(version).map_err(|_| "unsupported database spec version")?;
            Ok((Some(name.to_owned()), Some(version)))
        }
    }
}
