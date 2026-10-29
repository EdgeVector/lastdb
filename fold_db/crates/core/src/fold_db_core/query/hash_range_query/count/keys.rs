//! Count one field in the personal namespace, then the received namespaces.

use super::super::{full_span_page, HashRangeQueryProcessor};
use crate::schema::types::field::HashRangeFilter;
use crate::schema::types::key_value::KeyValue;
use crate::schema::{Schema, SchemaError};
use std::collections::HashSet;

impl HashRangeQueryProcessor {
    /// The same scoped metadata read serves the primary and its empty fallback.
    /// Tombstone flags exclude deleted keys without loading atom bodies.
    pub(super) async fn count_field_keys(
        &self,
        schema: &Schema,
        name: &str,
        filter: Option<&HashRangeFilter>,
        received: &[String],
    ) -> Result<HashSet<KeyValue>, SchemaError> {
        let Some(field) = schema.runtime_fields.get(name) else {
            return Ok(HashSet::new());
        };
        let binding =
            crate::storage::disk_pack::logical_paths::field::FieldScope::new(schema, name)?;
        binding
            .run(async {
                let mut keys = HashSet::new();
                let namespaces = std::iter::once(None)
                    .chain(received.iter().map(|namespace| Some(namespace.as_str())));
                for namespace in namespaces {
                    let mut working = field.cloned_without_molecule();
                    if let Some(namespace) = namespace {
                        working
                            .common_mut()
                            .set_storage_prefix(Some(namespace.to_string()));
                    }
                    let matches = working
                        .collect_matches(
                            &self.db_ops,
                            Some(filter.cloned().unwrap_or_else(full_span_page)),
                            false,
                        )
                        .await?;
                    keys.extend(matches.into_iter().map(|(key, _, _, _)| key));
                }
                Ok(keys)
            })
            .await
    }
}
