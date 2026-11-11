//! Named alternate reads retain the record's primary mutation address.

use super::*;
use crate::exec::app_routes::{alternate_list, list_rules::ListRecordKeysRequest};
use fold_db::alternate_keys::AlternateEntry;
use fold_db::fold_db_core::query::{query_executor::AlternatePage, Record};
use fold_db::schema::types::key_comparison::KeyComparisonContext;
use fold_db::schema::types::{Query, QueryCursor};
use handlers::QueryResponseFormat;

impl DatabaseRead<'_> {
    pub(super) fn alternate_list(
        &self,
        schema: &Schema,
        request: &ListRecordKeysRequest,
    ) -> Result<Value, HostError> {
        let hash = request
            .hash_filter
            .as_ref()
            .ok_or_else(|| HostError::new(400, "alternate key lists require hash="))?;
        let mut query = Query::new_with_filter(
            schema.name.clone(),
            Some(HashRangeFilter::HashKey(hash.clone())),
        );
        query.key_name = request.key_name.clone();
        let page = self.alternate_query(
            schema,
            QueryRequest {
                query,
                limit: Some(request.limit),
                offset: None,
                cursor: alternate_list::decode_cursor(request.cursor.as_deref())?,
                response_format: QueryResponseFormat::Values,
            },
        )?;
        Ok(json!({"list": alternate_list::report(request, schema, &page)?}))
    }

    pub(super) fn alternate_query(
        &self,
        schema: &Schema,
        request: QueryRequest,
    ) -> Result<Value, HostError> {
        let QueryRequest {
            query,
            limit,
            offset,
            cursor,
            response_format,
        } = request;
        let name = query
            .key_name
            .as_deref()
            .ok_or_else(|| HostError::new(400, "alternate key name is required"))?;
        let filter = query
            .filter
            .as_ref()
            .ok_or_else(|| HostError::new(400, "alternate filter is required"))?;
        let count_only = response_format == QueryResponseFormat::Count;
        if offset.unwrap_or_default() != 0
            || count_only && cursor.is_some()
            || query.expected_total_count.is_some()
        {
            return Err(HostError::new(
                400,
                "alternate Share pages use cursors without offsets or count assertions",
            ));
        }
        let comparison =
            KeyComparisonContext::for_schema(schema)?.cursor_binding(&(self.id, name, filter))?;
        let after = cursor
            .map(|cursor| alternate_after(cursor, &comparison))
            .transpose()?;
        let limit = crate::lastdb_host::pagination::clamp_request_limit(limit);
        let read_limit = if count_only { 1000 } else { limit };
        let page = self
            .reader
            .alternate_page(schema, name, filter, after.as_ref(), read_limit)
            .map_err(storage_error)?;
        if count_only {
            if page.has_more {
                return Err(storage_error(StorageError::ReadResourceLimit {
                    detail: "Share alternate count exceeds the bounded read allowance".into(),
                }));
            }
            return Ok(json!({"total_count": page.rows.len(), "exact": true}));
        }
        let mut entries = Vec::with_capacity(page.rows.len());
        let mut values = HashMap::with_capacity(page.rows.len());
        for (entry, record) in page.rows {
            values.insert(entry.primary.clone(), Record::from_stored(&record));
            entries.push(entry);
        }
        handlers::format_alternate_page(
            AlternatePage {
                values,
                entries,
                has_more: page.has_more,
                comparison,
            },
            response_format == QueryResponseFormat::Values,
            limit,
        )
    }
}

fn alternate_after(cursor: QueryCursor, binding: &str) -> Result<AlternateEntry, HostError> {
    if cursor.comparison.as_deref() != Some(binding) {
        return Err(HostError::new(
            409,
            "alternate cursor context changed; restart from the first page",
        ));
    }
    if cursor.record.is_some() {
        return Err(HostError::new(
            400,
            "alternate cursor cannot use a record position",
        ));
    }
    let primary = cursor
        .alternate_primary
        .clone()
        .ok_or_else(|| HostError::new(400, "alternate cursor requires its primary tie-breaker"))?;
    Ok(AlternateEntry {
        primary,
        matched: cursor.into_key(),
    })
}
