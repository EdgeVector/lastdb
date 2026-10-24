//! Primary-key step of the co-key plan: resolve the page of keys.

use super::*;

/// The primary name, values, API page keys, sources and verified range values.
pub(super) type CokeyPrimaryPage = (
    String,
    HashMap<KeyValue, FieldValue>,
    Vec<KeyValue>,
    HashMap<KeyValue, KeySource>,
    HashMap<KeyValue, FieldValue>,
);

impl HashRangeQueryProcessor {
    /// The primary establishes API keys before secondary reads. Administrative
    /// inspection proves its complete bounded set before taking a page.
    pub(super) async fn resolve_cokey_primary_page(
        &self,
        schema: &mut Schema,
        selected: &[String],
        filter: Option<HashRangeFilter>,
        window: Option<KeyWindow>,
        include_tombstones: bool,
        received_from_namespaces: &[String],
    ) -> Result<CokeyPrimaryPage, SchemaError> {
        let key_form = Self::primary_key_form(filter.as_ref());
        if key_form == paging::PrimaryKeyForm::Storage {
            let page = self
                .resolve_inspection_primary_page(
                    schema,
                    selected,
                    filter.as_ref(),
                    include_tombstones,
                    received_from_namespaces,
                )
                .await?;
            crate::db_operations::note_page_key_forms(Self::countable_keys(&page.2), 0);
            return Ok(page);
        }
        let mut primary_name = Self::cokey_primary_field(schema, selected);
        if matches!(
            window,
            Some(KeyWindow::Offset { limit: 0, .. } | KeyWindow::After { limit: 0, .. })
        ) {
            return Ok((
                primary_name,
                HashMap::new(),
                Vec::new(),
                HashMap::new(),
                HashMap::new(),
            ));
        }

        // --- 1. Primary: current resolve under filter / namespaces ---
        // The primary selects its layout before the window. Secondaries fill
        // only those keys, and actual tip presence controls the fallback.
        let mut primary = self
            .resolve_primary(
                schema,
                &primary_name,
                filter.clone(),
                window.clone(),
                include_tombstones,
                received_from_namespaces,
            )
            .await?;

        // A schema key field is the dense row spine for rows written through
        // that layout. Multi-key expand is the exception: a sibling layout can
        // share projected field hashes without ever having atoms under its own
        // key field. Prefer the key spine, then fall back to the projection only
        // when that spine is empty so sibling-key reads remain addressable.
        // Do not fall back after dropping sibling-layout tips: that re-opens
        // the union. Projecting the empty key field does not block fallback.
        let fallback = Self::cokey_fallback_field(selected, &primary_name)
            .filter(|_| primary.values.is_empty() && !primary.had_spine);
        if let Some(fallback) = fallback {
            primary = self
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
        }
        let page_keys: Vec<_> = primary.values.keys().cloned().collect();
        crate::db_operations::note_page_key_forms(Self::countable_keys(&page_keys), 0);
        Ok((
            primary_name,
            primary.values,
            page_keys,
            primary.sources,
            primary.layout_values,
        ))
    }
}
