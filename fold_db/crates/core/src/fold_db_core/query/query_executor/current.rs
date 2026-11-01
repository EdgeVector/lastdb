//! Resolve the caller's namespace, prepare its schema, then read selected fields.

use crate::access::AccessContext;
use crate::schema::types::field::KeyWindow;
use crate::schema::types::Query;
use crate::schema::SchemaError;

use super::{rules, FieldQueryOptions, QueryExecutor, QueryRecords};

impl QueryExecutor {
    pub(super) async fn query_current(
        &self,
        mut query: Query,
        window: Option<KeyWindow>,
        access_context: Option<&AccessContext>,
    ) -> Result<QueryRecords, SchemaError> {
        if query.key_name.is_some() {
            return Err(SchemaError::InvalidData(
                "use the named alternate page reader".into(),
            ));
        }
        // Approved local callers read all local fields. Context is identity +
        // optional multi-DB storage_prefix (X-LastDB-Db → org cohabitation).
        let storage_prefix = match access_context {
            Some(context) => {
                self.db_ops
                    .db_catalog()
                    .resolve_storage_prefix(
                        context.db_locator.as_deref(),
                        &query.schema_name,
                        context.storage_prefix.as_deref(),
                    )
                    .await?
            }
            None => None,
        };

        let Some(mut schema) = self.prepare_query_schema(&query).await? else {
            return Err(SchemaError::InvalidData(format!(
                "Schema '{}' not found",
                query.schema_name
            )));
        };
        // Scope record reads to the request DB handle (personal /
        // org). Clone-only schema — never written back to the registry.
        rules::scope_fields(&mut schema, storage_prefix.as_deref());

        self.hash_range_processor
            .query_with_field_predicates(
                &schema,
                &query.fields,
                query.filter.take(),
                window,
                FieldQueryOptions {
                    predicates: query.field_predicates.as_deref(),
                    value_filters: query.value_filters.as_deref(),
                    order_by: query.order_by.as_ref(),
                    predicate_limit: query.predicate_limit,
                },
            )
            .await
    }
}
