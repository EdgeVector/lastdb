//! In-process store contract and listed-database metadata for saved-query execution.

use std::future::Future;
use std::pin::Pin;

use serde_json::Value;

pub(crate) type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, String>> + Send + 'a>>;

pub(crate) trait QueryStore: Sync {
    type Db: Send + Sync;

    fn list(&self) -> Result<Vec<Value>, String>;
    fn database(&self, selector: &str) -> Result<Self::Db, String>;
    fn schema<'a>(&'a self, db: &'a Self::Db, name: &'a str) -> StoreFuture<'a, Value>;
    fn declare<'a>(&'a self, db: &'a Self::Db, body: &'a Value) -> StoreFuture<'a, Value>;
    fn query<'a>(
        &'a self,
        db: &'a Self::Db,
        schema: &'a str,
        filter: &'a Value,
    ) -> StoreFuture<'a, Vec<Value>>;
    fn mutate<'a>(&'a self, db: &'a Self::Db, body: &'a Value) -> StoreFuture<'a, Value>;
    fn fetch_file<'a>(
        &'a self,
        db: &'a Self::Db,
        schema: &'a str,
        hash: &'a str,
        range: Option<&'a str>,
        placement: &'a str,
    ) -> StoreFuture<'a, (String, Vec<u8>)>;
    fn put_file<'a>(&'a self, db: &'a Self::Db, body: &'a Value) -> StoreFuture<'a, Value>;
    fn delete_file<'a>(&'a self, db: &'a Self::Db, body: &'a Value) -> StoreFuture<'a, Value>;
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
