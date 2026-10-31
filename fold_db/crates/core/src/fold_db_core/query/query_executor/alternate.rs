//! Alternate reads select primary keys, then use the ordinary field reader.

use super::*;
use crate::alternate_keys::{self, AlternateEntry};
use crate::schema::types::QueryCursor;
use sha2::{Digest, Sha256};

pub struct AlternatePage {
    pub values: HashMap<String, HashMap<KeyValue, FieldValue>>,
    pub entries: Vec<AlternateEntry>,
    pub has_more: bool,
    pub comparison: String,
}

impl QueryExecutor {
    /// Count one explicitly keyed alternate selection in one captured root.
    /// Pages retain bounded memory; native read admission refuses excess work.
    pub async fn count_alternate_rows(
        &self,
        query: &Query,
        context: &AccessContext,
    ) -> Result<usize, SchemaError> {
        let scope = self.db_ops.read_scope().await?;
        scope
            .run_result(Box::pin(
                crate::storage::disk_pack::logical_paths::field::query_scope(
                    self.count_alternate_scoped(query, context),
                ),
            ))
            .await
    }

    async fn count_alternate_scoped(
        &self,
        query: &Query,
        context: &AccessContext,
    ) -> Result<usize, SchemaError> {
        let (schema, scope) = self.scoped_alternate_schema(query, context).await?;
        let name = query
            .key_name
            .as_deref()
            .ok_or_else(|| alternate_keys::invalid("alternate name is absent"))?;
        let definition = schema
            .alternate_keys
            .get(name)
            .ok_or_else(|| alternate_keys::invalid("alternate key is not declared"))?;
        let names = definition.fields().collect();
        let filter = query
            .filter
            .as_ref()
            .ok_or_else(|| alternate_keys::invalid("alternate count requires a key"))?;
        let mut after = None;
        let mut count = 0usize;
        loop {
            let entries = self
                .db_ops
                .atoms()
                .select_alternate_entries(
                    &schema,
                    name,
                    filter,
                    after.as_ref(),
                    128,
                    scope.as_deref(),
                )
                .await?;
            let keys: Vec<_> = entries.iter().map(|entry| entry.primary.clone()).collect();
            let fields = self
                .db_ops
                .atoms()
                .schema_fields_at_keys(&schema, &keys, scope.as_deref(), &names)
                .await?;
            for (entry, fields) in entries.iter().zip(fields) {
                if alternate_keys::derive_key(&schema, definition, &fields)?.as_ref()
                    != Some(&entry.matched)
                {
                    return Err(alternate_keys::invalid(
                        "alternate count found an entry without its current primary record",
                    ));
                }
            }
            count = count
                .checked_add(entries.len())
                .ok_or_else(|| alternate_keys::invalid("alternate count overflow"))?;
            if entries.len() < 128 {
                return Ok(count);
            }
            after = entries.last().map(|entry| entry.matched.clone());
        }
    }

    pub async fn query_alternate_page(
        &self,
        query: Query,
        limit: usize,
        cursor: Option<QueryCursor>,
        context: &AccessContext,
    ) -> Result<AlternatePage, SchemaError> {
        let scope = self.db_ops.read_scope().await?;
        scope
            .run_result(Box::pin(
                crate::storage::disk_pack::logical_paths::field::query_scope(
                    self.read_alternate_page(query, limit, cursor, context),
                ),
            ))
            .await
    }

    async fn read_alternate_page(
        &self,
        mut query: Query,
        limit: usize,
        cursor: Option<QueryCursor>,
        context: &AccessContext,
    ) -> Result<AlternatePage, SchemaError> {
        // The primary read below resolves the same scope from `context`.
        let (schema, scope) = self.scoped_alternate_schema(&query, context).await?;
        let name = query
            .key_name
            .take()
            .ok_or_else(|| alternate_keys::invalid("alternate name is absent"))?;
        if !schema.alternate_keys.contains_key(&name) {
            return Err(alternate_keys::invalid("alternate key is not declared"));
        }
        let filter = query
            .filter
            .take()
            .ok_or_else(|| alternate_keys::invalid("alternate filter is absent"))?;
        let comparison = comparison_binding(&schema, &name, &filter)?;
        if cursor
            .as_ref()
            .is_some_and(|cursor| cursor.comparison.as_deref() != Some(&comparison))
        {
            return Err(alternate_keys::invalid(
                "alternate cursor does not match this query",
            ));
        }
        let after = cursor.map(QueryCursor::into_key);
        let mut entries = self
            .db_ops
            .atoms()
            .select_alternate_entries(
                &schema,
                &name,
                &filter,
                after.as_ref(),
                limit.saturating_add(1),
                scope.as_deref(),
            )
            .await?;
        let has_more = entries.len() > limit;
        entries.truncate(limit);
        let keys: Vec<_> = entries.iter().map(|entry| entry.primary.clone()).collect();
        let projection = query.fields.clone();
        if !projection.is_empty() {
            for field in schema.alternate_keys[&name].fields() {
                if !query.fields.iter().any(|name| name == field) {
                    query.fields.push(field.to_owned());
                }
            }
        }
        query.filter = Some(crate::schema::types::field::HashRangeFilter::HashRangeKeys(
            keys.iter()
                .map(|key| (key.hash.clone().unwrap_or_default(), String::new()))
                .collect(),
        ));
        let mut values = if keys.is_empty() {
            HashMap::new()
        } else {
            self.query_current(query, None, Some(context)).await?
        };
        validate_entries(&schema, &name, &entries, &values)?;
        if !projection.is_empty() {
            values.retain(|field, _| projection.contains(field));
        }
        Ok(AlternatePage {
            values,
            entries,
            has_more,
            comparison,
        })
    }

    /// The schema and the storage scope of the caller's database: `None` for
    /// the personal database, the instance prefix for an org database.
    async fn scoped_alternate_schema(
        &self,
        query: &Query,
        context: &AccessContext,
    ) -> Result<(crate::schema::Schema, Option<String>), SchemaError> {
        let schema = self
            .prepare_query_schema(query)
            .await?
            .ok_or_else(|| alternate_keys::invalid("alternate schema is absent"))?;
        let scope = self
            .db_ops
            .db_catalog()
            .resolve_storage_prefix(
                context.db_locator.as_deref(),
                &schema.name,
                context.storage_prefix.as_deref(),
            )
            .await?;
        validate_controls(query)?;
        self.hash_range_processor.refuse_received_data().await?;
        Ok((schema, scope))
    }
}

fn validate_controls(query: &Query) -> Result<(), SchemaError> {
    if query.requires_post_key_work()
        || query.as_of.is_some()
        || query.rehydrate_depth.is_some()
        || query.include_tombstones
        || query
            .sort_order
            .as_ref()
            .is_some_and(|order| *order != crate::schema::types::operations::SortOrder::Asc)
        || query.expected_total_count.is_some()
    {
        return Err(alternate_keys::invalid(
            "unsupported alternate query controls",
        ));
    }
    Ok(())
}

fn comparison_binding(
    schema: &crate::schema::Schema,
    name: &str,
    filter: &crate::schema::types::field::HashRangeFilter,
) -> Result<String, SchemaError> {
    Ok(format!(
        "ak1:{:x}",
        Sha256::digest(
            serde_json::to_vec(&(
                &schema.name,
                schema.alternate_key_revision(),
                &name,
                &filter
            ))
            .map_err(|_| alternate_keys::invalid("invalid alternate comparison"))?,
        )
    ))
}

fn validate_entries(
    schema: &crate::schema::Schema,
    name: &str,
    entries: &[AlternateEntry],
    values: &HashMap<String, HashMap<KeyValue, FieldValue>>,
) -> Result<(), SchemaError> {
    let definition = &schema.alternate_keys[name];
    for entry in entries {
        let fields = definition
            .fields()
            .filter_map(|field| {
                values
                    .get(field)?
                    .get(&entry.primary)
                    .map(|value| (field.to_owned(), value.value.clone()))
            })
            .collect();
        if alternate_keys::derive_key(schema, definition, &fields)?.as_ref() != Some(&entry.matched)
        {
            return Err(alternate_keys::invalid(
                "alternate entry does not match its primary record",
            ));
        }
    }
    Ok(())
}
