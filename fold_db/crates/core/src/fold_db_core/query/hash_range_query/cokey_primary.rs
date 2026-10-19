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
        // The walk hands back storage-form keys. Rename the page here, at the one
        // boundary that holds the mapping, so the rest of the query — and the
        // caller — only ever sees API form. Two reasons it has to happen here and
        // not per-consumer: secondaries blind again, so they must be fanned out on
        // API keys or every one of them misses; and the primary's key becomes the
        // response's `key.hash`, so a storage-form key that escapes is a key the
        // caller cannot point-read back.
        let (mut primary_vals, mut page_keys, mut key_sources) =
            self.rename_page_to_api_keys(schema, &primary_name, primary_vals, key_sources);

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
        // the union when the caller omitted the hash field from `selected`.
        let (primary_vals, page_keys, key_sources) = if page_keys.is_empty()
            && !had_named_spine_keys
            && !selected.iter().any(|field| field == &primary_name)
        {
            let fallback = selected[0].clone();
            let (values, sources) = self
                .resolve_primary(
                    schema,
                    &fallback,
                    filter,
                    window,
                    include_tombstones,
                    received_from_namespaces,
                )
                .await?;
            primary_name = fallback;
            self.rename_page_to_api_keys(schema, &primary_name, values, sources)
        } else {
            (primary_vals, page_keys, key_sources)
        };
        Ok((primary_name, primary_vals, page_keys, key_sources))
    }
}
