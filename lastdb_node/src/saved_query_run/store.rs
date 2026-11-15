//! Concrete in-process store handle and listed-database metadata for saved-query execution.

use std::future::Future;
use std::sync::Arc;

use fold_db::fold_db_core::FoldDB;
use fold_db::schema::types::KeyValue;
use serde_json::{json, Value};

use crate::exec;
use crate::host::owned::Resolved;
use crate::host::Host;
use crate::lastdb_uds::uds_http::UdsRequest;

pub(super) struct TargetStore<'a> {
    host: &'a Host,
    db: Option<Arc<FoldDB>>,
}

pub(super) fn list_databases(host: &Host) -> Result<Vec<Value>, String> {
    exec::list_database_rows(host).map_err(|e| format_err(&e))
}

impl<'a> TargetStore<'a> {
    pub(super) fn open(host: &'a Host, selector: &str) -> Result<Self, String> {
        if !crate::lastdb_uds::valid_selector(selector) {
            return Err("invalid database ID or name".into());
        }
        let db = if selector == "personal" {
            None
        } else {
            match host.owned.resolve(selector) {
                Ok((Resolved::Owner, _)) => None,
                Ok((Resolved::Owned, Some(store))) => Some(store),
                Ok((Resolved::Owned, None) | (Resolved::Absent, Some(_))) => {
                    return Err("database registry is unavailable".into());
                }
                Ok((Resolved::Absent, None)) => {
                    return Err("database is not registered on this node".into());
                }
                Err(error) => return Err(error.status().1.to_owned()),
            }
        };
        Ok(Self { host, db })
    }
}

impl TargetStore<'_> {
    async fn run<T>(&self, fut: impl Future<Output = T>) -> T {
        match &self.db {
            None => fut.await,
            Some(store) => self.host.with_records(Arc::clone(store), fut).await,
        }
    }

    pub(super) async fn schema(&self, name: &str) -> Result<Value, String> {
        self.run(async {
            exec::get_schema_value(self.host, name).map_err(|err| {
                if err.status == 404 {
                    format!("source schema {name} is not installed")
                } else {
                    format_err(&err)
                }
            })
        })
        .await
    }

    pub(super) async fn declare(&self, body: &Value) -> Result<Value, String> {
        let bytes = serde_json::to_vec(body).map_err(|e| e.to_string())?;
        self.run(exec::declare_schema_slice(&bytes, self.host))
            .await
            .map_err(|e| format_err(&e))
    }

    pub(super) async fn query(&self, schema: &str, filter: &Value) -> Result<Vec<Value>, String> {
        let Value::Object(obj) = json!({
            "schema_name": schema,
            "filter": filter,
            "limit": crate::saved_query::MAX_RECORDS,
        }) else {
            return Err("invalid saved query request".into());
        };
        let req =
            exec::parse_query_body(obj).map_err(|_| "invalid saved query filter".to_owned())?;
        let response = self
            .run(exec::execute_query(self.host, req))
            .await
            .map_err(|e| format_err(&e))?;
        if response.get("has_more").and_then(Value::as_bool) == Some(true) {
            return Err("saved query read exceeds 256 records".into());
        }
        response
            .get("results")
            .and_then(Value::as_array)
            .cloned()
            .ok_or_else(|| "query returned no results".to_owned())
    }

    pub(super) async fn mutate(&self, body: &Value) -> Result<Value, String> {
        let bytes = serde_json::to_vec(body).map_err(|e| e.to_string())?;
        let parsed = exec::parse_mutation_body(&bytes)
            .map_err(|r| format!("invalid saved query mutation: {}", r.kind.as_str()))?;
        self.run(exec::commit_parsed_mutation(self.host, parsed))
            .await
            .map_err(|e| format_err(&e))
    }

    pub(super) async fn fetch_file(
        &self,
        schema: &str,
        hash: &str,
        range: Option<&str>,
        placement: &str,
    ) -> Result<(String, Vec<u8>), String> {
        self.run(async {
            let s = exec::resolve_schema_name(self.host, schema)
                .map_err(|e| format_err(&e))?
                .ok_or_else(|| format!("schema '{schema}' not found"))?;
            let key = KeyValue::new(Some(hash.to_owned()), range.map(str::to_owned));
            let (marker, bytes) = if placement == "inline" {
                self.host
                    .records()
                    .read_record_inline_file(&s, &key, None)
                    .await
                    .map_err(|e| format_err(&exec::HostError::from(e)))?
                    .ok_or_else(|| "the record has no inline file".to_owned())?
            } else {
                self.host
                    .records()
                    .read_record_file(&s, &key, None)
                    .await
                    .map_err(|e| format_err(&exec::HostError::from(e)))?
                    .ok_or_else(|| "the record has no file".to_owned())?
            };
            Ok((marker.sha256, bytes))
        })
        .await
    }

    pub(super) async fn put_file(&self, body: &Value) -> Result<Value, String> {
        let req = UdsRequest {
            method: "POST".into(),
            target: "/api/db/put-blob".into(),
            headers: vec![("content-type".into(), "application/json".into())],
            body: serde_json::to_vec(body).map_err(|e| e.to_string())?,
        };
        self.run(exec::put_blob_inner(&req, self.host))
            .await
            .map_err(|e| format_err(&e))
    }

    pub(super) async fn delete_file(&self, body: &Value) -> Result<Value, String> {
        let bytes = serde_json::to_vec(body).map_err(|e| e.to_string())?;
        self.run(exec::delete_blob_slice(&bytes, self.host))
            .await
            .map_err(|e| format_err(&e))
    }
}

fn format_err(err: &exec::HostError) -> String {
    format!("HTTP {}: {}", err.status, err.message)
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
