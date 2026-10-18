//! The single co-key query plan: primary key set, then secondary field resolution.

use super::*;

impl HashRangeQueryProcessor {
    /// Co-key plan: primary defines keys; secondaries fill those keys only.
    #[allow(
        clippy::too_many_arguments,
        reason = "the window rides alongside the filter it bounds; bundling them into a struct would only move the argument list"
    )]
    pub(super) async fn query_cokey(
        &self,
        schema: &mut Schema,
        fields: &[String],
        filter: Option<HashRangeFilter>,
        window: Option<KeyWindow>,
        include_tombstones: bool,
        received_from_namespaces: &[String],
        request_concurrency: Option<usize>,
    ) -> Result<HashMap<String, HashMap<KeyValue, FieldValue>>, SchemaError> {
        self.query_cokey_with_sources(
            schema,
            fields,
            filter,
            window,
            include_tombstones,
            received_from_namespaces,
            request_concurrency,
        )
        .await
        .map(|rows| rows.fields)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the window rides alongside the filter it bounds; bundling them into a struct would only move the argument list"
    )]
    pub(super) async fn query_cokey_with_sources(
        &self,
        schema: &mut Schema,
        fields: &[String],
        filter: Option<HashRangeFilter>,
        window: Option<KeyWindow>,
        include_tombstones: bool,
        received_from_namespaces: &[String],
        request_concurrency: Option<usize>,
    ) -> Result<CokeyRows, SchemaError> {
        let selected = Self::cokey_selected_fields(schema, fields);
        if selected.is_empty() {
            return Ok(CokeyRows {
                fields: HashMap::new(),
                page_keys: Vec::new(),
                key_sources: HashMap::new(),
            });
        }

        let (primary_name, primary_vals, page_keys, key_sources) = self
            .resolve_cokey_primary_page(
                schema,
                &selected,
                filter,
                window,
                include_tombstones,
                received_from_namespaces,
            )
            .await?;
        let mut result: HashMap<String, HashMap<KeyValue, FieldValue>> = HashMap::new();
        result.insert(primary_name.clone(), primary_vals);

        // F=1: done. Empty secondaries still get empty maps when multi-field
        // and page is empty so clients see a complete field set.
        if selected.len() == 1 && selected[0] == primary_name {
            tracing::debug!(
                primary = %primary_name,
                keys = page_keys.len(),
                "HashRangeQueryProcessor::query_cokey: single-field done"
            );
            return Ok(CokeyRows {
                fields: result,
                page_keys,
                key_sources,
            });
        }

        let secondary: Vec<String> = selected
            .iter()
            .filter(|fname| *fname != &primary_name)
            .cloned()
            .collect();
        for fname in &secondary {
            result.entry(fname.clone()).or_default();
        }
        if page_keys.is_empty() {
            return Ok(CokeyRows {
                fields: result,
                page_keys,
                key_sources,
            });
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
                &page_keys,
                &key_sources,
                include_tombstones,
                request_concurrency,
            )
            .await?;

        // --- 3. Atom batches for secondary bodies (grouped by storage prefix) ---
        if !pending.is_empty() {
            self.fill_cokey_secondaries(schema, pending, include_tombstones, &mut result)
                .await?;
        }

        tracing::debug!(
            fields = result.len(),
            page_keys = page_keys.len(),
            "HashRangeQueryProcessor::query_cokey: done"
        );
        Ok(CokeyRows {
            fields: result,
            page_keys,
            key_sources,
        })
    }
}
