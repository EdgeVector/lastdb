//! Resolve the caller's namespace, prepare its schema, then read selected fields.

use std::collections::HashMap;
use std::sync::Arc;

use crate::access::AccessContext;
use crate::schema::types::field::{FieldValue, KeyWindow};
use crate::schema::types::{KeyValue, Query};
use crate::schema::SchemaError;

use super::{rules, FieldQueryOptions, QueryExecutor};

impl QueryExecutor {
    pub(super) async fn query_current(
        &self,
        mut query: Query,
        window: Option<KeyWindow>,
        access_context: Option<&AccessContext>,
    ) -> Result<HashMap<String, HashMap<KeyValue, FieldValue>>, SchemaError> {
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
        // Scope molecule reads to the request DB handle (personal /
        // org). Clone-only schema — never written back to the registry.
        rules::scope_fields(&mut schema, storage_prefix.as_deref());

        // A format 7 home has no field molecules to preload.
        if let Some(runtime) = self
            .db_ops
            .namespaced_store()
            .disk_pack_runtime()
            .filter(|runtime| !runtime.record_home())
        {
            runtime
                .preload_schema_keys(Arc::clone(&self.db_ops), &schema)
                .await?;
        }

        self.hash_range_processor
            .query_with_field_predicates(
                &mut schema,
                &query.fields,
                query.filter.take(),
                window,
                FieldQueryOptions {
                    predicates: query.field_predicates.as_deref(),
                    value_filters: query.value_filters.as_deref(),
                    order_by: query.order_by.as_ref(),
                    predicate_limit: query.predicate_limit,
                    include_tombstones: query.include_tombstones,
                    secondary_concurrency: query.secondary_concurrency,
                },
            )
            .await
    }
}
