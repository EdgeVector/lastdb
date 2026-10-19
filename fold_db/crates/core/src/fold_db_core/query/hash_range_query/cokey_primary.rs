//! Primary-key step of the co-key plan: resolve the page of keys.

use super::*;

/// The primary field's name, its values, the API-form page keys and key sources.
pub(super) type CokeyPrimaryPage = (
    String,
    HashMap<KeyValue, FieldValue>,
    Vec<KeyValue>,
    HashMap<KeyValue, KeySource>,
);

impl HashRangeQueryProcessor {
    /// Resolve the primary field (falling back to the first selected field when
    /// the key spine is empty) and rename the page to API-form keys.
    pub(super) async fn resolve_cokey_primary_page(
        &self,
        schema: &mut Schema,
        selected: &[String],
        filter: Option<HashRangeFilter>,
        window: Option<KeyWindow>,
        include_tombstones: bool,
        received_from_namespaces: &[String],
    ) -> Result<CokeyPrimaryPage, SchemaError> {
        let mut primary_name = Self::cokey_primary_field(schema, selected);
        let key_form = Self::primary_key_form(filter.as_ref());
        if matches!(
            window,
            Some(KeyWindow::Offset { limit: 0, .. } | KeyWindow::After { limit: 0, .. })
        ) {
            return Ok((primary_name, HashMap::new(), Vec::new(), HashMap::new()));
        }

        // --- 1. Primary: current resolve under filter / namespaces ---
        // The primary selects its layout before the window. Secondaries fill
        // only those keys, and actual tip presence controls the fallback.
        let primary = self
            .resolve_primary(
                schema,
                &primary_name,
                filter.clone(),
                window.clone(),
                include_tombstones,
                received_from_namespaces,
            )
            .await?;
        let had_named_spine_keys = primary.had_spine;
        // Keyed reads already echo caller hashes. Only page/sample walks return
        // physical hashes that require value-based recovery at this boundary.
        let (primary_vals, page_keys, key_sources) = self.rename_page_to_api_keys(
            schema,
            &primary_name,
            key_form,
            primary.values,
            primary.sources,
        );

        // A schema key field is the dense row spine for rows written through
        // that layout. Multi-key expand is the exception: a sibling layout can
        // share projected field hashes without ever having atoms under its own
        // key field. Prefer the key spine, then fall back to the projection only
        // when that spine is empty so sibling-key reads remain addressable.
        // Do not fall back after dropping sibling-layout tips: that re-opens
        // the union. Projecting the empty key field does not block fallback.
        let fallback = Self::cokey_fallback_field(selected, &primary_name)
            .filter(|_| page_keys.is_empty() && !had_named_spine_keys);
        let (primary_vals, page_keys, key_sources) = if let Some(fallback) = fallback {
            let fallback_page = self
                .resolve_primary(
                    schema,
                    fallback,
                    filter,
                    window,
                    include_tombstones,
                    received_from_namespaces,
                )
                .await?;
            primary_name = fallback.to_string();
            self.rename_page_to_api_keys(
                schema,
                &primary_name,
                key_form,
                fallback_page.values,
                fallback_page.sources,
            )
        } else {
            (primary_vals, page_keys, key_sources)
        };
        Ok((primary_name, primary_vals, page_keys, key_sources))
    }
}
