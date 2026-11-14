//! In-process saved-query store adapter and owner-only UDS routes.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use fold_db::access::AccessContext;
use fold_db::fold_db_core::FoldDB;
use fold_db::schema::types::KeyValue;
use serde::Deserialize;
use serde_json::{json, Value};

use super::reply::{render, HostError};
use super::{database_routes, file_blob, mutation, query, schema_declare, schema_routes, wire};
use crate::host::owned::Resolved;
use crate::host::Host;
use crate::lastdb_uds::uds_http::{UdsRequest, UdsResponse};
use crate::saved_query::MAX_STORED_TEXT;
use crate::saved_query_run::{self, QueryStore, SaveRequest, StoreFuture};

struct ExecQueryStore<'a> {
    host: &'a Host,
}

impl QueryStore for ExecQueryStore<'_> {
    type Db = Option<Arc<FoldDB>>;

    fn list(&self) -> Result<Vec<Value>, String> {
        database_routes::list_database_rows(self.host).map_err(|e| format_err(&e))
    }

    fn database(&self, selector: &str) -> Result<Self::Db, String> {
        if !crate::lastdb_uds::valid_selector(selector) {
            return Err("invalid database ID or name".into());
        }
        if selector == "personal" {
            return Ok(None);
        }
        match self.host.owned.resolve(selector) {
            Ok((Resolved::Owner, _)) => Ok(None),
            Ok((Resolved::Owned, Some(store))) => Ok(Some(store)),
            Ok((Resolved::Owned, None) | (Resolved::Absent, Some(_))) => {
                Err("database registry is unavailable".into())
            }
            Ok((Resolved::Absent, None)) => Err("database is not registered on this node".into()),
            Err(error) => Err(error.status().1.to_owned()),
        }
    }

    fn schema<'a>(&'a self, db: &'a Self::Db, name: &'a str) -> StoreFuture<'a, Value> {
        Box::pin(async move {
            with_store(self.host, db, async {
                schema_routes::get_schema_value(self.host, name).map_err(|err| {
                    if err.status == 404 {
                        format!("source schema {name} is not installed")
                    } else {
                        format_err(&err)
                    }
                })
            })
            .await
        })
    }

    fn declare<'a>(&'a self, db: &'a Self::Db, body: &'a Value) -> StoreFuture<'a, Value> {
        Box::pin(async move {
            let bytes = serde_json::to_vec(body).map_err(|e| e.to_string())?;
            with_store(
                self.host,
                db,
                schema_declare::declare_schema_slice(&bytes, self.host),
            )
            .await
            .map_err(|e| format_err(&e))
        })
    }

    fn query<'a>(
        &'a self,
        db: &'a Self::Db,
        schema: &'a str,
        filter: &'a Value,
    ) -> StoreFuture<'a, Vec<Value>> {
        Box::pin(async move {
            let Value::Object(obj) = json!({
                "schema_name": schema,
                "filter": filter,
                "limit": crate::saved_query::MAX_RECORDS,
            }) else {
                return Err("invalid saved query request".into());
            };
            let req =
                wire::parse_query_body(obj).map_err(|_| "invalid saved query filter".to_owned())?;
            let response = with_store(self.host, db, query::execute_query(self.host, req))
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
        })
    }

    fn mutate<'a>(&'a self, db: &'a Self::Db, body: &'a Value) -> StoreFuture<'a, Value> {
        Box::pin(async move {
            let bytes = serde_json::to_vec(body).map_err(|e| e.to_string())?;
            let parsed = wire::parse_mutation_body(&bytes)
                .map_err(|r| format!("invalid saved query mutation: {}", r.kind.as_str()))?;
            with_store(
                self.host,
                db,
                mutation::commit_parsed_mutation(self.host, parsed),
            )
            .await
            .map_err(|e| format_err(&e))
        })
    }

    fn fetch_file<'a>(
        &'a self,
        db: &'a Self::Db,
        schema: &'a str,
        hash: &'a str,
        range: Option<&'a str>,
        placement: &'a str,
    ) -> StoreFuture<'a, (String, Vec<u8>)> {
        Box::pin(async move {
            with_store(self.host, db, async {
                let s = schema_routes::resolve_schema_name(self.host, schema)
                    .map_err(|e| format_err(&e))?
                    .ok_or_else(|| format!("schema '{schema}' not found"))?;
                let key = KeyValue::new(Some(hash.to_owned()), range.map(str::to_owned));
                let (marker, bytes) = if placement == "inline" {
                    self.host
                        .records()
                        .read_record_inline_file(&s, &key, None)
                        .await
                        .map_err(|e| format_err(&HostError::from(e)))?
                        .ok_or_else(|| "the record has no inline file".to_owned())?
                } else {
                    self.host
                        .records()
                        .read_record_file(&s, &key, None)
                        .await
                        .map_err(|e| format_err(&HostError::from(e)))?
                        .ok_or_else(|| "the record has no file".to_owned())?
                };
                Ok((marker.sha256, bytes))
            })
            .await
        })
    }

    fn put_file<'a>(&'a self, db: &'a Self::Db, body: &'a Value) -> StoreFuture<'a, Value> {
        Box::pin(async move {
            let req = UdsRequest {
                method: "POST".into(),
                target: "/api/db/put-blob".into(),
                headers: vec![("content-type".into(), "application/json".into())],
                body: serde_json::to_vec(body).map_err(|e| e.to_string())?,
            };
            with_store(self.host, db, file_blob::put_blob_inner(&req, self.host))
                .await
                .map_err(|e| format_err(&e))
        })
    }

    fn delete_file<'a>(&'a self, db: &'a Self::Db, body: &'a Value) -> StoreFuture<'a, Value> {
        Box::pin(async move {
            let bytes = serde_json::to_vec(body).map_err(|e| e.to_string())?;
            with_store(
                self.host,
                db,
                file_blob::delete_blob_slice(&bytes, self.host),
            )
            .await
            .map_err(|e| format_err(&e))
        })
    }
}

async fn with_store<T>(host: &Host, db: &Option<Arc<FoldDB>>, fut: impl Future<Output = T>) -> T {
    match db {
        None => fut.await,
        Some(store) => host.with_records(Arc::clone(store), fut).await,
    }
}

fn format_err(err: &HostError) -> String {
    format!("HTTP {}: {}", err.status, err.message)
}

fn route_error(err: String) -> HostError {
    if let Some(rest) = err.strip_prefix("HTTP ") {
        if let Some((code, msg)) = rest.split_once(": ") {
            if let Ok(status) = code.parse::<u16>() {
                return HostError::new(status, msg.to_owned());
            }
        }
    }
    let status = if err == "database is not registered on this node" {
        404
    } else if err == "database registry is unavailable" {
        503
    } else {
        400
    };
    HostError::new(status, err)
}

pub fn reconcile_saved_query_catalog(
    host: Arc<Host>,
    source: String,
) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> {
    Box::pin(async move {
        let store = ExecQueryStore { host: &host };
        saved_query_run::reconcile_catalog(&store, &source).await
    })
}

pub(super) async fn save_route(req: &UdsRequest, ctx: &AccessContext, host: &Host) -> UdsResponse {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Body {
        id: String,
        app: String,
        source: String,
        destination: String,
        spec: String,
        spec_version: u32,
        definition_json: String,
    }
    let res = async {
        if !ctx.is_owner {
            return Err(HostError::new(403, "saved queries are owner-only"));
        }
        let b: Body = serde_json::from_slice(&req.body).map_err(|e| {
            HostError::new(400, format!("invalid /api/saved-queries/save body: {e}"))
        })?;
        if b.definition_json.len() > MAX_STORED_TEXT {
            return Err(HostError::new(400, "saved query definition is too large"));
        }
        let store = ExecQueryStore { host };
        let saved = saved_query_run::save(
            &store,
            &SaveRequest {
                id: b.id,
                app: b.app,
                source: b.source,
                destination: b.destination,
                spec: b.spec,
                spec_version: b.spec_version,
                definition_json: b.definition_json,
            },
        )
        .await
        .map_err(route_error)?;
        Ok(json!({
            "saved": {
                "id": saved.id,
                "source": saved.source,
                "destination": saved.destination,
                "spec_name": saved.spec_name,
                "spec_version": saved.spec_version,
            }
        }))
    }
    .await;
    render(res, ctx)
}

pub(super) async fn reconcile_route(
    req: &UdsRequest,
    ctx: &AccessContext,
    host: &Host,
) -> UdsResponse {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Body {
        id: String,
        source: String,
    }
    let res = async {
        if !ctx.is_owner {
            return Err(HostError::new(403, "saved queries are owner-only"));
        }
        let b: Body = serde_json::from_slice(&req.body).map_err(|e| {
            HostError::new(
                400,
                format!("invalid /api/saved-queries/reconcile body: {e}"),
            )
        })?;
        let store = ExecQueryStore { host };
        let r = saved_query_run::reconcile(&store, &b.id, &b.source)
            .await
            .map_err(route_error)?;
        Ok(json!({
            "receipt": {
                "added": r.added,
                "updated": r.updated,
                "removed": r.removed,
                "unchanged": r.unchanged,
            }
        }))
    }
    .await;
    render(res, ctx)
}
