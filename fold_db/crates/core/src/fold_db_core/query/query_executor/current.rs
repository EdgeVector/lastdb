use super::*;

impl QueryExecutor {
    pub(super) async fn query_internal_scoped(
        &self,
        query: Query,
        window: Option<KeyWindow>,
        access_context: Option<&AccessContext>,
    ) -> Result<HashMap<String, HashMap<KeyValue, FieldValue>>, SchemaError> {
        crate::storage::disk_pack::logical_paths::field::query_scope(self.query_current(
            query,
            window,
            access_context,
        ))
        .await
    }

    async fn query_current(
        &self,
        query: Query,
        window: Option<KeyWindow>,
        access_context: Option<&AccessContext>,
    ) -> Result<HashMap<String, HashMap<KeyValue, FieldValue>>, SchemaError> {
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

        match self
            .schema_manager
            // Hot keyed-read path: take the molecule-stripped clone. The
            // executor mutates `schema.runtime_fields` while `resolve_value`
            // re-hydrates each field from storage, so the registry's hydrated
            // molecule would only be overwritten before use — cloning it per
            // query was the O(field cardinality) residual this avoids (#905
            // follow-up).
            .get_schema_following_supersession_for_read(&query.schema_name)
            .await?
        {
            Some(schema) => {
                let owner = schema.owner_app_id.clone();
                crate::warm_admit::with_schema_owner(
                    owner.as_deref(),
                    self.run_query_on_schema(schema, query, window, storage_prefix),
                )
                .await
            }
            None => Err(SchemaError::InvalidData(format!(
                "Schema '{}' not found",
                query.schema_name
            ))),
        }
    }
}
