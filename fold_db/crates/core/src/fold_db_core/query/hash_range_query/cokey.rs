//! The single co-key query plan: primary key set, then secondary field resolution.

use super::*;

pub(super) struct CokeySelection<'a> {
    pub fields: &'a [String],
    /// Key discovery uses the caller's projection, not condition-only fields.
    /// None uses `fields`, as in a normal projection read.
    pub key_fields: Option<&'a [String]>,
    pub filter: Option<HashRangeFilter>,
    pub window: Option<KeyWindow>,
    pub include_tombstones: bool,
    pub received_from_namespaces: &'a [String],
    pub request_concurrency: Option<usize>,
}

impl HashRangeQueryProcessor {
    pub(super) async fn query_cokey_with_sources(
        &self,
        schema: &mut Schema,
        selection: CokeySelection<'_>,
    ) -> Result<CokeyRows, SchemaError> {
        let CokeySelection {
            fields,
            key_fields,
            filter,
            window,
            include_tombstones,
            received_from_namespaces,
            request_concurrency,
        } = selection;
        let selected = field_rules::selected_fields(schema, fields);
        let key_selected = key_fields.map(|fields| field_rules::selected_fields(schema, fields));
        let key_selected = key_selected.as_deref().unwrap_or(&selected);
        if selected.is_empty() || key_selected.is_empty() {
            return Ok(CokeyRows {
                fields: HashMap::new(),
                page_keys: Vec::new(),
                key_sources: HashMap::new(),
                primary_field: String::new(),
            });
        }

        let (primary_name, primary_vals, page_keys, key_sources, layout_values) = self
            .resolve_cokey_primary_page(
                schema,
                key_selected,
                filter,
                window,
                include_tombstones,
                received_from_namespaces,
            )
            .await?;
        let mut rows = CokeyRows {
            fields: HashMap::from([(primary_name.clone(), primary_vals)]),
            page_keys,
            key_sources,
            primary_field: primary_name,
        };
        // Membership already resolved the range field through the selected
        // source. Move it only when requested and complete for this final page.
        let reused_field = schema
            .key
            .as_ref()
            .and_then(|key| key.range_field.as_ref())
            .filter(|range_field| {
                *range_field != &rows.primary_field
                    && selected.contains(range_field)
                    && !rows.page_keys.is_empty()
                    && rows
                        .page_keys
                        .iter()
                        .all(|key| layout_values.contains_key(key))
            });
        if let Some(range_field) = reused_field {
            rows.fields.insert(range_field.clone(), layout_values);
        }
        self.fill_cokey_projection(
            schema,
            &selected,
            &mut rows,
            include_tombstones,
            request_concurrency,
        )
        .await?;
        tracing::debug!(
            fields = rows.fields.len(),
            page_keys = rows.page_keys.len(),
            "HashRangeQueryProcessor::query_cokey: done"
        );
        Ok(rows)
    }

    /// Projected fields use only the keys and source namespaces already chosen.
    async fn fill_cokey_projection(
        &self,
        schema: &mut Schema,
        selected: &[String],
        rows: &mut CokeyRows,
        include_tombstones: bool,
        request_concurrency: Option<usize>,
    ) -> Result<(), SchemaError> {
        if selected.len() == 1 && selected[0] == rows.primary_field {
            return Ok(());
        }
        let secondary: Vec<String> = selected
            .iter()
            .filter(|fname| {
                fname.as_str() != rows.primary_field && !rows.fields.contains_key(*fname)
            })
            .cloned()
            .collect();
        for fname in &secondary {
            rows.fields.entry(fname.clone()).or_default();
        }
        if rows.page_keys.is_empty() {
            return Ok(());
        }

        // --- 2. Secondary fields for page_keys only ---
        // The page arrived already renamed, so the fan-out keys are just the page
        // keys: a secondary lands on the very `KeyValue` the primary is filed
        // under. That identity is what keeps a row whole — land a secondary on a
        // different key and the row splits in two (one fully populated, one blank
        // shell, the shape the board scan showed).
        let pending = self
            .collect_cokey_secondary_pending(
                schema,
                secondary,
                &rows.page_keys,
                &rows.key_sources,
                include_tombstones,
                request_concurrency,
            )
            .await?;

        // --- 3. Atom batches for secondary bodies (grouped by storage prefix) ---
        if !pending.is_empty() {
            self.fill_cokey_secondaries(schema, pending, include_tombstones, &mut rows.fields)
                .await?;
        }

        Ok(())
    }
}
