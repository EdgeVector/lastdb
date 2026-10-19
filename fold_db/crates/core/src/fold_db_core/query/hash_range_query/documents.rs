//! Resolve document authority for the selected keys and their actual scopes.

use super::*;

impl HashRangeQueryProcessor {
    pub(super) async fn resolve_query_documents(
        &self,
        schema: &Schema,
        rows: &mut CokeyRows,
        include_tombstones: bool,
    ) -> Result<(), SchemaError> {
        self.resolve_document_projection(
            schema,
            &rows.page_keys,
            &rows.key_sources,
            &mut rows.fields,
            include_tombstones,
        )
        .await
    }

    pub(super) async fn resolve_document_projection(
        &self,
        schema: &Schema,
        keys: &[KeyValue],
        sources: &HashMap<KeyValue, KeySource>,
        fields: &mut HashMap<String, HashMap<KeyValue, FieldValue>>,
        include_tombstones: bool,
    ) -> Result<(), SchemaError> {
        let mut by_source: HashMap<KeySource, Vec<KeyValue>> = HashMap::new();
        for key in keys {
            let source = sources
                .get(key)
                .cloned()
                .unwrap_or_else(|| crate::record_molecule::record_scope(schema).map(str::to_owned));
            by_source.entry(source).or_default().push(key.clone());
        }
        for (source, keys) in by_source {
            crate::record_molecule::overlay_document_fields(
                &self.db_ops,
                schema,
                &keys,
                source.as_deref(),
                fields,
                include_tombstones,
            )
            .await?;
        }
        Ok(())
    }
}
