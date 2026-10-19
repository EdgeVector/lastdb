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

        // --- 1. Primary: current resolve under filter / namespaces ---
        // The primary establishes the page of keys; secondaries only fill those
        // keys. So windowing the primary windows the whole row set, and the
        // secondary fan-out below shrinks with it for free.
        let (primary_vals, key_sources) = self
            .resolve_primary(
                schema,
                &primary_name,
                filter.clone(),
                window.clone(),
                include_tombstones,
                received_from_namespaces,
            )
            .await?;
        // Keyed reads already echo caller hashes. Only page/sample walks return
        // physical hashes that require value-based recovery at this boundary.
        let (mut primary_vals, mut page_keys, mut key_sources) = self.rename_page_to_api_keys(
            schema,
            &primary_name,
            key_form,
            primary_vals,
            key_sources,
        );

        // Shared-molecule multi-key (graph-edge planes): one molecule holds
        // tips for both key layouts. HashKey(slug) therefore lists every tip
        // whose storage hash is slug, inbound and outbound. Keep only tips
        // whose *named* hash-field value equals that slug so schema S returns
        // S's layout. Stage 0 dump 2026-09-02: all GRAPH_EDGE_FIELDS share
        // molecule_uuid across BySource/ByDestination; HashKey and empty
        // Prefix both returned the union (12 = 5 out + 7 in).
        let had_named_spine_keys = !page_keys.is_empty();
        Self::retain_named_layout_primary(
            schema,
            &primary_name,
            &filter,
            &mut primary_vals,
            &mut page_keys,
            &mut key_sources,
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
            let (values, sources) = self
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
            self.rename_page_to_api_keys(schema, &primary_name, key_form, values, sources)
        } else {
            (primary_vals, page_keys, key_sources)
        };
        Ok((primary_name, primary_vals, page_keys, key_sources))
    }
}
