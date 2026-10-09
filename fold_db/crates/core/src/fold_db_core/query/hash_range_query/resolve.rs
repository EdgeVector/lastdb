//! Primary and secondary key resolution (current head and `as_of`), plus pending-atom lookup.

use super::*;

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
        // lint:fn-size-ok verbatim move from hash_range_query.rs; splitting this function is separate work
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
        let primary_vals = match filter.as_ref() {
            Some(HashRangeFilter::Page { .. } | HashRangeFilter::PageAfter { .. })
                if !received_from_namespaces.is_empty() =>
            {
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
                        if let std::collections::hash_map::Entry::Vacant(e) =
                            merged.entry(key.clone())
                        {
                            e.insert(value);
                            sources.insert(key, Some(namespace.clone()));
                        }
                    }
                }
                let mut keys: Vec<KeyValue> = merged.keys().cloned().collect();
                match filter.as_ref() {
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
                }
            }
            _ => {
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
                        if let std::collections::hash_map::Entry::Vacant(e) = own.entry(key.clone())
                        {
                            e.insert(value);
                            sources.insert(key, Some(namespace.clone()));
                        }
                    }
                }
                // Shares nulled the window above so the per-source resolves
                // above returned their full matching sets; bound the merged
                // result here or every non-`Page` windowed/keyset shape with
                // an active share namespace returns the whole merged superset
                // on every call instead of one page.
                if !received_from_namespaces.is_empty() {
                    if let Some(requested_window) = requested_window {
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
                own
            }
        };

        Ok((primary_vals, sources))
    }

    /// Batch-resolve pending secondary bodies via
    /// [`crate::db_operations::atom_store::AtomStore::get_atoms_located`],
    /// grouped by storage prefix (share namespaces). Preserves pending order.
    pub(super) async fn resolve_pending_atoms(
        &self,
        pending: &[SecondaryPending],
    ) -> Result<Vec<Option<crate::atom::Atom>>, SchemaError> {
        // Group indexes that share a storage prefix so each get_atoms_located
        // call gets one prefix for the whole batch.
        let mut by_prefix: HashMap<Option<String>, Vec<usize>> = HashMap::new();
        for (i, item) in pending.iter().enumerate() {
            by_prefix.entry(item.5.clone()).or_default().push(i);
        }
        let mut out: Vec<Option<crate::atom::Atom>> = (0..pending.len()).map(|_| None).collect();
        for (prefix, idxs) in by_prefix {
            let slots: Vec<(&str, Option<crate::atom::AtomPartition>)> = idxs
                .iter()
                .map(|&i| (pending[i].2.as_str(), pending[i].6.clone()))
                .collect();
            let atoms = crate::db_operations::resident_read::get_atoms_located_resident_first(
                &self.db_ops,
                &slots,
                prefix.as_deref(),
            )
            .await?;
            for (i, atom) in idxs.into_iter().zip(atoms) {
                out[i] = atom;
            }
        }
        Ok(out)
    }
}
