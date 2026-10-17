use super::*;

impl QueryExecutor {
    pub(super) async fn query_internal_scoped(
        &self,
        query: Query,
        window: Option<KeyWindow>,
        access_context: Option<&AccessContext>,
    ) -> Result<HashMap<String, HashMap<KeyValue, FieldValue>>, SchemaError> {
        crate::storage::disk_pack::logical_paths::field::query_scope(
            query.as_of.is_none(),
            self.query_current(query, window, access_context),
        )
        .await
    }

    async fn query_current(
        &self,
        query: Query,
        window: Option<KeyWindow>,
        access_context: Option<&AccessContext>,
    ) -> Result<HashMap<String, HashMap<KeyValue, FieldValue>>, SchemaError> {
        let can_use_exact =
            crate::storage::disk_pack::logical_paths::warm::covers_window(&query, window.as_ref());
        let runtime = self.db_ops.namespaced_store().disk_pack_runtime();
        if can_use_exact {
            if let Some(warm) = crate::storage::disk_pack::logical_paths::warm::lookup(
                runtime.clone(),
                &query,
                access_context,
            )?
            .filter(crate::storage::disk_pack::WarmQueryFields::complete_wire_metadata)
            {
                let (api, opaque) = match warm.key_form() {
                    Some("api") => (1, 0),
                    Some("opaque") => (0, 1),
                    Some("mixed") => (1, 1),
                    _ => (0, 0),
                };
                crate::db_operations::note_page_key_forms(api, opaque);
                return Ok(warm.fields);
            }
        }
        let remembered = query.clone();
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

        let result = match self
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
        };
        if let Ok(fields) = &result {
            crate::storage::disk_pack::logical_paths::warm::remember(
                runtime,
                &remembered,
                access_context,
                fields,
            )?;
        }
        result
    }
}
