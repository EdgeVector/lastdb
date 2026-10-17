//! Primary and secondary key resolution (current head and `as_of`), plus pending-atom lookup.

use super::*;
use crate::schema::types::FieldVariant;

impl HashRangeQueryProcessor {
    /// Resolve the primary field (with share-namespace merge) and record
    /// which storage prefix owns each key (personal wins on collision).
    #[allow(
        clippy::too_many_arguments,
        reason = "the window rides alongside the filter it bounds; bundling them into a struct would only move the argument list"
    )]
    pub(super) async fn resolve_primary(
        &self,
        schema: &mut Schema,
        primary_name: &str,
        filter: Option<HashRangeFilter>,
        window: Option<KeyWindow>,
        as_of: Option<DateTime<Utc>>,
        include_tombstones: bool,
        received_from_namespaces: &[String],
    ) -> Result<(HashMap<KeyValue, FieldValue>, HashMap<KeyValue, KeySource>), SchemaError> {
        // Shares turn a read into a cross-namespace merge, and a window has to
        // be taken on the merged set or it pages one namespace and silently
        // truncates the rest. The merge itself needs every namespace resolved,
        // so windowing before it is not available here. A home with share
        // namespaces keeps the pre-existing merge-then-slice path: correctness
        // on a rare shape beats read amplification on it. `requested_window`
        // survives the nulling below so the catch-all arm can re-apply it to
        // the merged set — mirroring the explicit skip/take the `Page` /
        // `PageAfter` arm already does for its shape.
        let requested_window = window.clone();
        let window = window.filter(|_| received_from_namespaces.is_empty());
        let binding = crate::storage::disk_pack::logical_paths::field::FieldScope::for_runtime(
            self.db_ops.namespaced_store().disk_pack_runtime().is_some(),
            schema,
            primary_name,
        )?;
        let field = schema.runtime_fields.get_mut(primary_name).ok_or_else(|| {
            SchemaError::InvalidField(format!(
                "co-key primary field '{primary_name}' missing from schema runtime"
            ))
        })?;
        let own_prefix = field.common().storage_prefix().map(str::to_owned);

        let mut sources: HashMap<KeyValue, KeySource> = HashMap::new();

        // Page / PageAfter + shares: full-span merge then slice. Unfiltered
        // cursor continuation (`PageAfter`, no KeyWindow) used to resolve each
        // namespace's cursor independently and skip the post-merge slice, so
        // later pages were a per-namespace union (duplicates, holes, extra
        // rows). Offset `Page` and key-restricted windows stay on their
        // existing arms.
        let primary_vals = binding
            .run(async {
                match filter.as_ref() {
                    Some(HashRangeFilter::Page { .. } | HashRangeFilter::PageAfter { .. })
                        if !received_from_namespaces.is_empty() =>
                    {
                        self.resolve_primary_merged_page(
                            field,
                            primary_name,
                            filter.as_ref(),
                            received_from_namespaces,
                            as_of,
                            include_tombstones,
                            own_prefix,
                            &mut sources,
                        )
                        .await
                    }
                    _ => {
                        self.resolve_primary_windowed(
                            field,
                            primary_name,
                            filter,
                            window,
                            requested_window,
                            received_from_namespaces,
                            as_of,
                            include_tombstones,
                            own_prefix,
                            &mut sources,
                        )
                        .await
                    }
                }
            })
            .await?;

        Ok((primary_vals, sources))
    }

    /// Page / PageAfter with share namespaces: full-span merge, then slice.
    #[allow(clippy::too_many_arguments)]
    async fn resolve_primary_merged_page(
        &self,
        field: &mut FieldVariant,
        primary_name: &str,
        filter: Option<&HashRangeFilter>,
        received_from_namespaces: &[String],
        as_of: Option<DateTime<Utc>>,
        include_tombstones: bool,
        own_prefix: KeySource,
        sources: &mut HashMap<KeyValue, KeySource>,
    ) -> Result<HashMap<KeyValue, FieldValue>, SchemaError> {
        let mut merged = field
            .resolve_value(
                &self.db_ops,
                Some(full_span_page()),
                as_of,
                include_tombstones,
            )
            .await?;
        for k in merged.keys() {
            sources.insert(k.clone(), own_prefix.clone());
        }
        for namespace in received_from_namespaces {
            let mut shared = self
                .resolve_field_from_namespace(
                    field,
                    namespace,
                    Some(full_span_page()),
                    as_of,
                    include_tombstones,
                )
                .await
                .map_err(|e| {
                    SchemaError::InvalidData(format!(
                        "Failed to resolve primary '{primary_name}' from namespace '{namespace}': {e}"
                    ))
                })?;
            Self::stamp_shared_writer(namespace, &mut shared);
            for (key, value) in shared {
                if let std::collections::hash_map::Entry::Vacant(e) = merged.entry(key.clone()) {
                    e.insert(value);
                    sources.insert(key, Some(namespace.clone()));
                }
            }
        }
        let mut keys: Vec<KeyValue> = merged.keys().cloned().collect();
        let page = match filter {
            Some(HashRangeFilter::Page { offset, limit }) => {
                Self::sort_keys_for_field(field, &mut keys);
                let page: HashMap<KeyValue, FieldValue> = keys
                    .into_iter()
                    .skip(*offset)
                    .take(*limit)
                    .filter_map(|key| merged.remove(&key).map(|value| (key, value)))
                    .collect();
                sources.retain(|k, _| page.contains_key(k));
                page
            }
            Some(HashRangeFilter::PageAfter { after, limit }) => {
                keys.sort_by(KeyValue::cmp_page_order);
                let page: HashMap<KeyValue, FieldValue> = keys
                    .into_iter()
                    .filter(|key| key.cmp_page_order(after).is_gt())
                    .take(*limit)
                    .filter_map(|key| merged.remove(&key).map(|value| (key, value)))
                    .collect();
                sources.retain(|k, _| page.contains_key(k));
                page
            }
            _ => unreachable!("outer match only admits Page / PageAfter"),
        };
        Ok(page)
    }

    /// Every other shape: resolve own and shared sets, then bound the merge.
    #[allow(clippy::too_many_arguments)]
    async fn resolve_primary_windowed(
        &self,
        field: &mut FieldVariant,
        primary_name: &str,
        filter: Option<HashRangeFilter>,
        window: Option<KeyWindow>,
        requested_window: Option<KeyWindow>,
        received_from_namespaces: &[String],
        as_of: Option<DateTime<Utc>>,
        include_tombstones: bool,
        own_prefix: KeySource,
        sources: &mut HashMap<KeyValue, KeySource>,
    ) -> Result<HashMap<KeyValue, FieldValue>, SchemaError> {
        let mut own = field
            .resolve_value_windowed(
                &self.db_ops,
                filter.clone(),
                window,
                as_of,
                include_tombstones,
            )
            .await?;
        for k in own.keys() {
            sources.insert(k.clone(), own_prefix.clone());
        }
        for namespace in received_from_namespaces {
            let mut shared = self
                .resolve_field_from_namespace(
                    field,
                    namespace,
                    filter.clone(),
                    as_of,
                    include_tombstones,
                )
                .await
                .map_err(|e| {
                    SchemaError::InvalidData(format!(
                        "Failed to resolve primary '{primary_name}' from namespace '{namespace}': {e}"
                    ))
                })?;
            Self::stamp_shared_writer(namespace, &mut shared);
            for (key, value) in shared {
                if let std::collections::hash_map::Entry::Vacant(e) = own.entry(key.clone()) {
                    e.insert(value);
                    sources.insert(key, Some(namespace.clone()));
                }
            }
        }
        if !received_from_namespaces.is_empty() {
            if let Some(requested_window) = requested_window {
                Self::bound_merged_to_window(&mut own, sources, requested_window);
            }
        }
        Ok(own)
    }

    /// Shares nulled the window so the per-source resolves returned their full
    /// matching sets; bound the merged result here or every non-`Page`
    /// windowed/keyset shape with an active share namespace returns the whole
    /// merged superset on every call instead of one page.
    fn bound_merged_to_window(
        own: &mut HashMap<KeyValue, FieldValue>,
        sources: &mut HashMap<KeyValue, KeySource>,
        requested_window: KeyWindow,
    ) {
        let mut keys: Vec<KeyValue> = own.keys().cloned().collect();
        keys.sort_by(KeyValue::cmp_page_order);
        let bounded: HashMap<KeyValue, ()> = match requested_window {
            KeyWindow::Offset { offset, limit } => keys
                .into_iter()
                .skip(offset)
                .take(limit)
                .map(|k| (k, ()))
                .collect(),
            KeyWindow::After { after, limit } => keys
                .into_iter()
                .filter(|k| k.cmp_page_order(&after).is_gt())
                .take(limit)
                .map(|k| (k, ()))
                .collect(),
        };
        own.retain(|k, _| bounded.contains_key(k));
        sources.retain(|k, _| bounded.contains_key(k));
    }
}
