//! Primary-key step of the co-key plan: resolve the page of keys.

use std::collections::HashMap;

use crate::schema::types::field::{FieldValue, HashRangeFilter, KeyWindow};
use crate::schema::types::key_value::KeyValue;
use crate::schema::{Schema, SchemaError};

use super::{field_rules, paging, HashRangeQueryProcessor, KeySource};

/// The primary name, values, API page keys, sources and verified range values.
pub(super) struct CokeyPrimaryPage {
    pub(super) primary_field: String,
    pub(super) values: HashMap<KeyValue, FieldValue>,
    pub(super) page_keys: Vec<KeyValue>,
    pub(super) key_sources: HashMap<KeyValue, KeySource>,
    pub(super) layout_values: HashMap<KeyValue, FieldValue>,
}

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
            crate::db_operations::note_page_key_forms(Self::countable_keys(&page.page_keys), 0);
            return Ok(page);
        }
        let mut primary_name = field_rules::primary_field(schema, selected);
        if matches!(
            window,
            Some(KeyWindow::Offset { limit: 0, .. } | KeyWindow::After { limit: 0, .. })
        ) {
            return Ok(CokeyPrimaryPage {
                primary_field: primary_name,
                values: HashMap::new(),
                page_keys: Vec::new(),
                key_sources: HashMap::new(),
                layout_values: HashMap::new(),
            });
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
        let fallback = field_rules::fallback_field(selected, &primary_name)
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
        Ok(CokeyPrimaryPage {
            primary_field: primary_name,
            values: primary.values,
            page_keys,
            key_sources: primary.sources,
            layout_values: primary.layout_values,
        })
    }
}
