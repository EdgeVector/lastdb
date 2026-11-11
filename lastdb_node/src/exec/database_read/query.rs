//! Key selection and ordinary query pagination for a verified Share.

use super::*;
use fold_db::fold_db_core::query::Record;
use fold_db::record_value::StoredRecord;
use fold_db::schema::types::key_comparison::KeyComparisonContext;
use fold_db::schema::types::operations::SortOrder;
use fold_db::schema::types::{KeyValue, QueryCursor};
use handlers::QueryResponseFormat;
use schema_types::record_keys::RecordKeyKind;

impl DatabaseRead<'_> {
    pub(super) fn render_query(
        &self,
        parsed: Result<QueryRequest, UdsResponse>,
        ctx: &AccessContext,
    ) -> UdsResponse {
        match parsed {
            Ok(request) => render(self.query(request), ctx),
            Err(response) => response,
        }
    }

    pub(super) fn batch(&self, req: &UdsRequest, ctx: &AccessContext) -> UdsResponse {
        let queries = match super::super::query_batch::parse_query_batch(req, ctx) {
            Ok(queries) => queries,
            Err(response) => return response,
        };
        let mut bytes = 0usize;
        let mut finished = Vec::with_capacity(queries.len());
        for (index, body) in queries.into_iter().enumerate() {
            if bytes >= 64 * 1024 * 1024 {
                return error_response(
                    413,
                    "Share query batch exceeds its response allowance",
                    ctx,
                );
            }
            let parsed = match body {
                Value::Object(body) => parse_query_body(body),
                _ => Err(Reject::malformed_body().response()),
            };
            let response = self.render_query(parsed, ctx);
            bytes += response.body.len();
            finished.push((index, response));
        }
        if bytes > 64 * 1024 * 1024 {
            return error_response(413, "Share query batch exceeds its response allowance", ctx);
        }
        let results = super::super::query_batch::batch_results(finished);
        render(Ok(json!({"count": results.len(), "results": results})), ctx)
    }

    fn query(&self, request: QueryRequest) -> Result<Value, HostError> {
        let schema = self.queryable_schema(&request.query.schema_name)?;
        let query = &request.query;
        if query.include_tombstones || matches!(query.sort_order, Some(SortOrder::Desc)) {
            return Err(HostError::new(
                400,
                "Share queries support ascending keyed reads",
            ));
        }
        if query.key_name.is_some() {
            return self.alternate_query(schema, request);
        }
        let QueryRequest {
            mut query,
            limit,
            offset,
            cursor,
            response_format,
        } = request;
        let comparison = KeyComparisonContext::for_schema(schema)?;
        query.filter = comparison.canonical_filter(query.filter)?;
        let filter = query
            .filter
            .as_ref()
            .ok_or_else(|| HostError::new(400, "Share queries require a key selector"))?;
        let binding = comparison.cursor_binding(&(self.id, filter))?;
        let limit = crate::lastdb_host::pagination::clamp_request_limit(limit);
        let offset = offset.unwrap_or_default();
        let count_only = response_format == QueryResponseFormat::Count;
        if cursor.is_some() && (offset != 0 || count_only || query.expected_total_count.is_some())
            || count_only && offset != 0
        {
            return Err(HostError::new(
                400,
                "invalid Share query continuation or count controls",
            ));
        }
        if cursor
            .as_ref()
            .is_some_and(|cursor| cursor.comparison.as_deref() != Some(binding.as_str()))
        {
            return Err(HostError::new(
                409,
                "cursor comparison context changed; restart from the first page",
            ));
        }
        let page_request = PageRequest {
            binding: &binding,
            cursor: cursor.as_ref(),
            limit,
            offset,
            needs_count: count_only || query.expected_total_count.is_some(),
        };
        let ranged = schema.require_record_keys()?.primary.kind == RecordKeyKind::HashRange;
        let page = if let Some(keys) = exact_keys(filter, ranged) {
            self.exact_page(schema, &comparison, &keys, &page_request)?
        } else {
            self.range_page(schema, filter, &page_request)?
        };
        if query
            .expected_total_count
            .is_some_and(|expected| page.total != Some(expected))
        {
            return Err(HostError::new(
                409,
                "expected_total_count differs from the exact keyed count",
            ));
        }
        if count_only {
            return Ok(json!({"total_count": page.total, "exact": true}));
        }
        let rows = page
            .records
            .into_iter()
            .map(|record| {
                Ok((
                    comparison.canonical_key(record.key.clone())?,
                    Record::from_stored(&record),
                ))
            })
            .collect::<Result<Vec<_>, HostError>>()?;
        Ok(handlers::verified_query_page(
            rows,
            response_format == QueryResponseFormat::Values,
            page.total,
            limit,
            offset,
            page.has_more,
            page.next,
        ))
    }

    fn exact_page(
        &self,
        schema: &Schema,
        comparison: &KeyComparisonContext,
        keys: &[KeyValue],
        request: &PageRequest<'_>,
    ) -> Result<QueryPage, HostError> {
        let mut records = self
            .reader
            .exact_records(schema, keys)
            .map_err(storage_error)?;
        let order = |left: &KeyValue, right: &KeyValue| {
            left.cmp_page_order_with(right, |left, right| {
                comparison
                    .range_comparison()
                    .compare(left, right)
                    .unwrap_or_else(|_| left.cmp(right))
            })
        };
        records.sort_by(|left, right| order(&left.key, &right.key));
        let total = records.len();
        let start = if let Some(cursor) = request.cursor {
            if cursor.record.is_some() || cursor.alternate_primary.is_some() {
                return Err(HostError::new(400, "invalid exact Share query cursor"));
            }
            let key = comparison.canonical_key(cursor.clone().into_key())?;
            schema
                .require_record_keys()?
                .primary
                .validate_coordinates(key.hash.as_deref(), key.range.as_deref())
                .map_err(|_| HostError::new(400, "invalid exact Share query cursor key"))?;
            records.partition_point(|record| !order(&record.key, &key).is_gt())
        } else {
            request.offset.min(total)
        };
        let end = start.saturating_add(request.limit).min(total);
        let has_more = end < total;
        let records = records.drain(start..end).collect::<Vec<_>>();
        let next = records
            .last()
            .filter(|_| has_more)
            .map(|record| QueryCursor::new(record.key.clone(), Some(request.binding.to_owned())));
        Ok(QueryPage {
            records,
            total: Some(total),
            has_more,
            next,
        })
    }

    fn range_page(
        &self,
        schema: &Schema,
        filter: &HashRangeFilter,
        request: &PageRequest<'_>,
    ) -> Result<QueryPage, HostError> {
        if !matches!(
            filter,
            HashRangeFilter::HashKey(_)
                | HashRangeFilter::HashRangePrefix { .. }
                | HashRangeFilter::HashRangeRange { .. }
        ) {
            return Err(HostError::new(
                400,
                "Share queries require exact keys or a range under one hash",
            ));
        }
        let after = request.cursor.map(range_position).transpose()?;
        let window = request.limit.saturating_add(request.offset);
        if window > 2000 {
            return Err(HostError::new(
                400,
                "Share query offset is too large; continue with next_cursor",
            ));
        }
        let total = if request.needs_count {
            let count = self
                .reader
                .record_page(schema, Some(filter), None, 2000)
                .map_err(storage_error)?;
            if count.next_cursor.is_some() {
                return Err(read_limit("Share count exceeds the bounded read allowance"));
            }
            Some(count.records.len())
        } else {
            None
        };
        let page = self
            .reader
            .record_page(schema, Some(filter), after.as_deref(), window.max(1))
            .map_err(storage_error)?;
        let has_more = page.next_cursor.is_some() || page.records.len() > window;
        if request.offset != 0 && page.records.len() < window && has_more {
            return Err(read_limit("Share offset cannot continue; use next_cursor"));
        }
        let next = page
            .next_cursor
            .as_deref()
            .filter(|_| request.limit != 0)
            .map(|position| QueryCursor::at_record(position, Some(request.binding.to_owned())));
        let records = page
            .records
            .into_iter()
            .skip(request.offset)
            .take(request.limit)
            .collect();
        Ok(QueryPage {
            records,
            total,
            has_more,
            next,
        })
    }
}

fn exact_keys(filter: &HashRangeFilter, ranged: bool) -> Option<Vec<KeyValue>> {
    use HashRangeFilter as Filter;
    let key = |hash: &str, range: &str| {
        KeyValue::new(Some(hash.to_owned()), ranged.then(|| range.to_owned()))
    };
    match filter {
        Filter::HashRangeKey { hash, range } => Some(vec![key(hash, range)]),
        Filter::HashRangeKeys(keys) => {
            Some(keys.iter().map(|(hash, range)| key(hash, range)).collect())
        }
        Filter::HashKey(hash)
        | Filter::HashRangePrefix { hash, .. }
        | Filter::HashRangeRange { hash, .. }
            if !ranged =>
        {
            Some(vec![key(hash, "")])
        }
        _ => None,
    }
}

struct PageRequest<'a> {
    binding: &'a str,
    cursor: Option<&'a QueryCursor>,
    limit: usize,
    offset: usize,
    needs_count: bool,
}

struct QueryPage {
    records: Vec<StoredRecord>,
    total: Option<usize>,
    has_more: bool,
    next: Option<QueryCursor>,
}

fn range_position(cursor: &QueryCursor) -> Result<String, HostError> {
    if cursor.hash.is_some() || cursor.range.is_some() || cursor.alternate_primary.is_some() {
        return Err(HostError::new(400, "invalid Share range cursor"));
    }
    cursor
        .record_position()
        .map_err(|error| HostError::new(400, error))?
        .ok_or_else(|| HostError::new(400, "Share cursor does not contain its position"))
}

fn read_limit(detail: &str) -> HostError {
    storage_error(StorageError::ReadResourceLimit {
        detail: detail.to_owned(),
    })
}
