//! Ordinary read routes for a registered Share. Every route holds one verified
//! cache slot and never runs against the personal home or a staged writer.

use super::*;
use crate::registered_share::RegisteredShare;
use crate::share_cache::{ShareCacheError, ShareReader};
use fold_db::db_operations::{SchemaRecordKey, SchemaRecordKeysReport};
use fold_db::record_value::FileMarker;
use fold_db::schema::types::field::HashRangeFilter;
use fold_db::schema::{SchemaListEntry, SchemaState};
use fold_db::storage::disk_pack::run_restore::SnapshotSchema;
use fold_db::storage::StorageError;
use serde_json::json;

mod alternate;
mod query;

pub(super) fn read(
    route: DataRoute,
    req: &UdsRequest,
    ctx: &AccessContext,
    share: &RegisteredShare,
) -> UdsResponse {
    // The inactive native reader is synchronous. Keep those bounded disk
    // reads out of the runtime's async worker set.
    tokio::task::block_in_place(|| {
        let reader = match share.reader() {
            Ok(reader) => reader,
            Err(ShareCacheError::Busy | ShareCacheError::Unavailable) => {
                return error_response(503, "Share has no available verified copy", ctx);
            }
            Err(error) => return render(Err(HostError::new(500, error.to_string())), ctx),
        };
        let catalog = match reader.schemas() {
            Ok(schemas) => schemas,
            Err(error) => return render(Err(storage_error(error)), ctx),
        };
        let database = DatabaseRead {
            reader: &reader,
            catalog: &catalog,
            id: share.id.as_str(),
        };
        match route {
            DataRoute::Query => {
                let parsed = body_object(req).and_then(parse_query_body);
                database.render_query(parsed, ctx)
            }
            DataRoute::QueryBatch => database.batch(req, ctx),
            DataRoute::ListRecordKeys => render(database.list_keys(&req.target), ctx),
            DataRoute::GetSchema => render(database.schema_detail(&req.target), ctx),
            DataRoute::ListSchemas => render(database.schema_list(&req.target), ctx),
            DataRoute::DbFetchBlob => {
                super::file_blob::render_file(req, ctx, database.file(&req.body))
            }
            _ => error_response(403, "operation is unavailable for a Share database", ctx),
        }
    })
}

struct DatabaseRead<'a> {
    reader: &'a ShareReader,
    catalog: &'a [SnapshotSchema],
    id: &'a str,
}

impl DatabaseRead<'_> {
    fn find_schema(&self, name: &str) -> Result<&SnapshotSchema, HostError> {
        let canonical = |name: &str| self.catalog.iter().find(|item| item.entry.name() == name);
        if let Some(item) = canonical(name) {
            return Ok(item);
        }
        let candidates = self
            .catalog
            .iter()
            .map(|item| item.entry.clone())
            .collect::<Vec<_>>();
        schema_resolve::resolve_captured_schema_alias(name, &candidates)?
            .as_deref()
            .and_then(canonical)
            .or_else(|| canonical(name.trim()))
            .ok_or_else(|| HostError::new(404, "schema not found in this Share database"))
    }

    fn queryable_schema(&self, name: &str) -> Result<&Schema, HostError> {
        let item = self.find_schema(name)?;
        if item.entry.state == SchemaState::Blocked {
            return Err(HostError::new(400, "schema is blocked and cannot be read"));
        }
        let schema = &item.entry.schema;
        fold_db::schema::require_logical_layout(&schema.schema_type)?;
        Ok(schema)
    }

    fn schema_detail(&self, target: &str) -> Result<Value, HostError> {
        let name = schema_name_from_target(target)
            .ok_or_else(|| HostError::new(400, "schema name is required"))?;
        Ok(json!({ "schema": self.find_schema(&name)?.entry }))
    }

    fn schema_list(&self, target: &str) -> Result<Value, HostError> {
        if query_flag(target, "include_counts") {
            return Err(HostError::new(
                400,
                "Share schema listing does not count records",
            ));
        }
        let include_full = query_flag(target, "include_full");
        let mut newer = Vec::new();
        let mut schemas = Vec::new();
        for item in self.catalog {
            let entry = &item.entry;
            let schema = &entry.schema;
            if schema.identity_is_newer_than_binary() {
                newer.push(entry.name());
            }
            if entry.state == SchemaState::Blocked {
                continue;
            }
            let mut row = if include_full {
                serde_json::to_value(entry)
            } else {
                serde_json::to_value(SchemaListEntry::from_schema(schema, entry.state))
            }
            .map_err(|_| HostError::new(500, "could not serialize verified schema"))?;
            row["record_count"] = Value::Null;
            row["has_data"] = Value::Null;
            schemas.push(row);
        }
        Ok(json!({
            "count": schemas.len(), "schemas": schemas,
            "ready": true, "full_included": include_full, "counts_included": false,
            "identity_algo_version": schema_types::IDENTITY_HASH_ALGO_VERSION,
            "newer_identity_algo_count": newer.len(), "newer_identity_algo_schemas": newer,
        }))
    }

    fn list_keys(&self, target: &str) -> Result<Value, HostError> {
        let request = super::list_keys_route::ListRecordKeysRequest::parse(target)
            .map_err(|message| HostError::new(400, message))?;
        let schema = self.queryable_schema(&request.requested)?;
        if request.key_name.is_some() {
            return self.alternate_list(schema, &request);
        }
        let key_definition = schema.require_record_keys()?.primary.clone();
        let filter = request
            .hash_filter
            .as_ref()
            .map(|hash| HashRangeFilter::HashKey(hash.clone()));
        let page = self
            .reader
            .record_page(
                schema,
                filter.as_ref(),
                request.cursor.as_deref(),
                request.limit,
            )
            .map_err(storage_error)?;
        let has_more = page.next_cursor.is_some();
        let keys = page
            .records
            .into_iter()
            .map(|record| SchemaRecordKey {
                hash: record.key.hash.unwrap_or_default(),
                range: record.key.range.unwrap_or_default(),
            })
            .collect();
        Ok(json!({"list": SchemaRecordKeysReport {
            schema: request.requested, schema_id: schema.name.clone(),
            key_definition, hash_filter: request.hash_filter,
            field_key: page.record_space,
            keys, next_cursor: page.next_cursor, has_more, truncated: has_more,
        }}))
    }

    fn file(&self, body: &[u8]) -> Result<(FileMarker, Vec<u8>), HostError> {
        let target = super::file_blob::FileTarget::parse(body, "/api/db/fetch-blob")?;
        let schema = self.queryable_schema(&target.schema)?;
        schema
            .require_record_keys()?
            .primary
            .validate_coordinates(target.key.hash.as_deref(), target.key.range.as_deref())
            .map_err(|_| HostError::new(400, "record key differs from its schema"))?;
        if target.placement == super::file_blob::Placement::Blob {
            return Err(HostError::new(
                404,
                "Share publications contain inline files only",
            ));
        }
        self.reader
            .inline_file(schema, &target.key, target.expected_revision.as_deref())
            .map_err(HostError::from)?
            .ok_or_else(|| HostError::new(404, "the record has no inline file"))
    }
}

fn storage_error(error: StorageError) -> HostError {
    match error {
        StorageError::InvalidOperation(message) => HostError::new(400, message),
        other => HostError::from(other),
    }
}
