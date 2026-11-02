//! One API coordinate identity precedes warm-set lookups and page selection.

use super::{KeyComparisonContext, KeyValue, SchemaError};
use crate::schema::types::field::{HashRangeFilter, KeyWindow};
use schema_types::field_range_profile::FieldRangeProfile;

impl KeyComparisonContext {
    pub fn canonical_range(&self, value: &str) -> Result<String, SchemaError> {
        canonical_range(self.profile, value)
    }

    pub fn canonical_key(&self, mut key: KeyValue) -> Result<KeyValue, SchemaError> {
        canonical_key(self.profile, &mut key)?;
        Ok(key)
    }

    pub fn canonical_filter(
        &self,
        filter: Option<HashRangeFilter>,
    ) -> Result<Option<HashRangeFilter>, SchemaError> {
        canonical_filter(self.profile, filter)
    }

    pub fn canonical_window(
        &self,
        window: Option<KeyWindow>,
    ) -> Result<Option<KeyWindow>, SchemaError> {
        canonical_window(self.profile, window)
    }
}

pub(crate) fn canonical_window(
    profile: FieldRangeProfile,
    window: Option<KeyWindow>,
) -> Result<Option<KeyWindow>, SchemaError> {
    match window {
        Some(KeyWindow::After { mut after, limit }) => {
            canonical_key(profile, &mut after)?;
            Ok(Some(KeyWindow::after(after, limit)))
        }
        other => Ok(other),
    }
}

pub(crate) fn canonical_range(
    profile: FieldRangeProfile,
    value: &str,
) -> Result<String, SchemaError> {
    profile
        .encode_value(value)
        .and_then(|stored| profile.decode_value(&stored))
        .map_err(SchemaError::InvalidData)
}

pub(crate) fn canonical_key(
    profile: FieldRangeProfile,
    key: &mut KeyValue,
) -> Result<(), SchemaError> {
    if let Some(range) = key.range.as_mut() {
        *range = canonical_range(profile, range)?;
    } else if matches!(profile, FieldRangeProfile::TypedV1 { .. }) {
        return Err(SchemaError::InvalidData(
            "a typed range key requires a range coordinate".into(),
        ));
    }
    Ok(())
}

pub(crate) fn canonical_filter(
    profile: FieldRangeProfile,
    mut filter: Option<HashRangeFilter>,
) -> Result<Option<HashRangeFilter>, SchemaError> {
    use HashRangeFilter::*;
    match filter.as_mut() {
        Some(HashRangeKey { range, .. } | RangeKey(range)) => {
            *range = canonical_range(profile, range)?;
        }
        Some(HashRangeKeys(keys)) => {
            for (_, range) in keys {
                *range = canonical_range(profile, range)?;
            }
        }
        Some(HashRangeRange { start, end, .. } | RangeRange { start, end }) => {
            *start = canonical_range(profile, start)?;
            *end = canonical_range(profile, end)?;
        }
        Some(
            HashRangePrefix { .. } | RangePrefix(_) | HashRangePattern { .. } | RangePattern(_),
        ) => {
            profile
                .require_text_prefix()
                .map_err(SchemaError::InvalidData)?;
        }
        Some(PageAfter { after, .. }) => canonical_key(profile, after)?,
        _ => {}
    }
    Ok(filter)
}
