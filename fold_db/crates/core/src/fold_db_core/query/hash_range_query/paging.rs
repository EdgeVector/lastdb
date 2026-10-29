//! API page-key recovery and named-layout key handling.

use super::*;

/// Keyed reads echo caller hashes. Only physical page/sample walks need recovery.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum PrimaryKeyForm {
    Api,
    Storage,
}

impl HashRangeQueryProcessor {
    pub(super) fn primary_key_form(filter: Option<&HashRangeFilter>) -> PrimaryKeyForm {
        match filter {
            Some(
                HashRangeFilter::HashKey(_)
                | HashRangeFilter::HashRangeKey { .. }
                | HashRangeFilter::HashRangeKeys(_)
                | HashRangeFilter::HashRangePrefix { .. }
                | HashRangeFilter::HashRangeRange { .. },
            ) => PrimaryKeyForm::Api,
            // Unsupported selectors are refused before this boundary. The
            // remaining producers are None, Page and SampleN.
            _ => PrimaryKeyForm::Storage,
        }
    }

    /// Only keys with a hash contribute to the API-coordinate response stamp.
    pub(super) fn countable_keys(keys: &[KeyValue]) -> u64 {
        keys.iter().filter(|k| k.hash.is_some()).count() as u64
    }

    /// Keyed reads of the declared HashRange hash field need layout membership.
    /// Each selected API key supplies its own hash, including exact batches.
    pub(super) fn needs_named_layout_membership(
        schema: &Schema,
        primary_name: Option<&str>,
        filter: Option<&HashRangeFilter>,
    ) -> bool {
        // A shared HashRange molecule can contain more than one key layout.
        // Its declared hash and range values identify the requested layout.
        if !matches!(
            schema.schema_type,
            crate::schema::types::schema::DeclarativeSchemaType::HashRange
        ) {
            return false;
        }
        let hash_field = schema
            .key
            .as_ref()
            .and_then(|key| key.hash_field.as_deref());
        if hash_field.is_none() || primary_name != hash_field {
            return false;
        }
        Self::primary_key_form(filter) == PrimaryKeyForm::Api
    }
}
