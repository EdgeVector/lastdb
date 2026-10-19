//! Captured primary selection, scope precedence, membership, then its window.

use super::primary_selection::{PrimaryRequest, PrimarySelection};
use super::*;
use crate::schema::types::FieldVariant;

impl HashRangeQueryProcessor {
    /// A primary reports its actual tip presence separately from returned rows.
    pub(super) async fn resolve_primary(
        &self,
        schema: &mut Schema,
        primary_name: &str,
        filter: Option<HashRangeFilter>,
        window: Option<KeyWindow>,
        include_tombstones: bool,
        received_from_namespaces: &[String],
    ) -> Result<PrimarySelection, SchemaError> {
        let named_hash =
            Self::named_layout_hash(schema, Some(primary_name), filter.as_ref()).map(str::to_owned);
        let binding =
            crate::storage::disk_pack::logical_paths::field::FieldScope::new(schema, primary_name)?;
        if let Some(runtime) = self.db_ops.namespaced_store().disk_pack_runtime() {
            runtime
                .preload_bound_field(Arc::clone(&self.db_ops), schema, primary_name)
                .await?;
        }
        let field = schema.runtime_fields.get_mut(primary_name).ok_or_else(|| {
            SchemaError::InvalidField(format!(
                "co-key primary field '{primary_name}' missing from schema runtime"
            ))
        })?;
        let request = PrimaryRequest {
            name: primary_name,
            filter,
            window,
            include_tombstones,
            received: received_from_namespaces,
            own_prefix: field.common().storage_prefix().map(str::to_owned),
            named_hash,
        };
        binding
            .run(async {
                let mut selected = if matches!(request.filter, Some(HashRangeFilter::Page { .. }))
                    && !request.received.is_empty()
                {
                    self.resolve_primary_merged_page(field, &request).await?
                } else {
                    self.resolve_primary_windowed(field, &request).await?
                };
                if !selected.had_spine {
                    selected.had_spine = self.primary_has_tips(field, &request).await?;
                }
                Ok(selected)
            })
            .await
    }

    /// Explicit physical Page plus shares retains its existing merge order.
    async fn resolve_primary_merged_page(
        &self,
        field: &mut FieldVariant,
        request: &PrimaryRequest<'_>,
    ) -> Result<PrimarySelection, SchemaError> {
        let full = Some(full_span_page());
        let values = field
            .resolve_value(&self.db_ops, full.clone(), request.include_tombstones)
            .await?;
        let mut selected = PrimarySelection::new(values, &request.own_prefix);
        self.merge_primary_namespaces(field, request, full, &mut selected)
            .await?;
        let Some(HashRangeFilter::Page { offset, limit }) = request.filter.as_ref() else {
            unreachable!("only explicit Page uses the merged page route")
        };
        let mut keys: Vec<_> = selected.values.keys().cloned().collect();
        Self::sort_keys_for_field(field, &mut keys);
        selected.retain_keys(&keys.into_iter().skip(*offset).take(*limit).collect());
        Ok(selected)
    }

    async fn resolve_primary_windowed(
        &self,
        field: &mut FieldVariant,
        request: &PrimaryRequest<'_>,
    ) -> Result<PrimarySelection, SchemaError> {
        if request.received.is_empty() {
            if let (Some(wanted), Some(filter), Some(window)) = (
                request.named_hash.as_deref(),
                request.filter.as_ref(),
                request.window.clone(),
            ) {
                let (values, had_spine) = field
                    .resolve_named_window(
                        &self.db_ops,
                        filter,
                        window,
                        wanted,
                        request.include_tombstones,
                    )
                    .await?;
                let mut selected = PrimarySelection::new(values, &request.own_prefix);
                selected.had_spine = had_spine;
                return Ok(selected);
            }
        }
        // Shared scopes need one merged set before membership and the window.
        let own_window = request
            .window
            .clone()
            .filter(|_| request.received.is_empty());
        let values = field
            .resolve_value_windowed(
                &self.db_ops,
                request.filter.clone(),
                own_window,
                request.include_tombstones,
            )
            .await?;
        let mut selected = PrimarySelection::new(values, &request.own_prefix);
        self.merge_primary_namespaces(field, request, request.filter.clone(), &mut selected)
            .await?;
        selected.retain_named(request.named_hash.as_deref());
        if !request.received.is_empty() {
            if let Some(window) = request.window.clone() {
                selected.bound(window);
            }
        }
        Ok(selected)
    }

    /// Personal data wins before membership checks; filtering cannot reveal a peer.
    async fn merge_primary_namespaces(
        &self,
        field: &FieldVariant,
        request: &PrimaryRequest<'_>,
        filter: Option<HashRangeFilter>,
        selected: &mut PrimarySelection,
    ) -> Result<(), SchemaError> {
        for namespace in request.received {
            let mut shared = self
                .resolve_field_from_namespace(
                    field,
                    namespace,
                    filter.clone(),
                    request.include_tombstones,
                )
                .await
                .map_err(|error| match error {
                    SchemaError::ReadResourceLimit { .. }
                    | SchemaError::WriteResourceLimit { .. } => error,
                    error => SchemaError::InvalidData(format!(
                        "Failed to resolve primary '{}' from namespace '{namespace}': {error}",
                        request.name
                    )),
                })?;
            selected.had_spine |= !shared.is_empty();
            Self::stamp_shared_writer(namespace, &mut shared);
            for (key, value) in shared {
                if let std::collections::hash_map::Entry::Vacant(entry) =
                    selected.values.entry(key.clone())
                {
                    entry.insert(value);
                    selected.sources.insert(key, Some(namespace.clone()));
                }
            }
        }
        Ok(())
    }

    /// Empty hydration or a window tail cannot authorize a projection fallback.
    async fn primary_has_tips(
        &self,
        field: &FieldVariant,
        request: &PrimaryRequest<'_>,
    ) -> Result<bool, SchemaError> {
        if field
            .has_primary_tips(
                &self.db_ops,
                request.filter.as_ref(),
                request.include_tombstones,
            )
            .await?
        {
            return Ok(true);
        }
        for namespace in request.received {
            let mut shared = field.cloned_without_molecule();
            shared
                .common_mut()
                .set_storage_prefix(Some(namespace.clone()));
            if shared
                .has_primary_tips(
                    &self.db_ops,
                    request.filter.as_ref(),
                    request.include_tombstones,
                )
                .await?
            {
                return Ok(true);
            }
        }
        Ok(false)
    }
}
