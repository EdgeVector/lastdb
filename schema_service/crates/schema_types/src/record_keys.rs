//! Explicit record addresses are independent of payload field definitions.
use crate::alternate_keys::MAX_ALTERNATE_KEYS;
use crate::field_range_profile::FieldRangeProfile;
use crate::{DeclarativeSchemaDefinition, FieldValueType};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RecordKeyKind {
    Hash,
    HashRange,
}

/// A key declares its shape and scalar range order, never a payload field.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RecordKeyDefinition {
    pub kind: RecordKeyKind,
    /// An alternate name must still be supplied; null explicitly selects no address.
    #[serde(default, skip_serializing_if = "is_false")]
    pub allow_absent: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range_type: Option<FieldValueType>,
}

impl RecordKeyDefinition {
    #[must_use]
    pub const fn hash() -> Self {
        Self {
            kind: RecordKeyKind::Hash,
            allow_absent: false,
            range_type: None,
        }
    }

    #[must_use]
    pub fn hash_range(range_type: FieldValueType) -> Self {
        Self {
            kind: RecordKeyKind::HashRange,
            allow_absent: false,
            range_type: Some(range_type),
        }
    }

    pub fn range_profile(&self) -> Result<FieldRangeProfile, String> {
        match (self.kind, &self.range_type) {
            (RecordKeyKind::Hash, None) | (RecordKeyKind::HashRange, Some(_)) => {
                FieldRangeProfile::for_field_type(self.range_type.as_ref())
            }
            _ => Err("hash keys have no range type; hash_range keys require one".into()),
        }
    }

    pub fn validate_coordinates(
        &self,
        hash: Option<&str>,
        range: Option<&str>,
    ) -> Result<(), String> {
        if hash.is_none_or(str::is_empty) {
            return Err("a record address requires a nonempty hash".into());
        }
        match (self.kind, range) {
            (RecordKeyKind::Hash, None) => (),
            (RecordKeyKind::HashRange, Some(value)) if !value.is_empty() => (),
            _ => return Err("record address differs from the declared key shape".into()),
        }
        self.range_profile()?
            .encode_value(range.unwrap_or_default())?;
        Ok(())
    }
}

/// Every replacement supplies exactly these named alternate addresses.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RecordKeys {
    pub primary: RecordKeyDefinition,
    pub alternates: BTreeMap<String, RecordKeyDefinition>,
}

impl RecordKeys {
    pub fn validate(&self) -> Result<(), String> {
        if self.primary.allow_absent {
            return Err("a primary key cannot allow an absent address".into());
        }
        self.primary.range_profile()?;
        if self.alternates.len() > MAX_ALTERNATE_KEYS {
            return Err(format!(
                "a schema permits at most {MAX_ALTERNATE_KEYS} alternate keys"
            ));
        }
        for (name, key) in &self.alternates {
            DeclarativeSchemaDefinition::validate_field_name(name)?;
            if name == "primary" {
                return Err("primary is reserved as an alternate key name".into());
            }
            key.range_profile()?;
        }
        Ok(())
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}
