//! Report the declared range domain once per schema, without a type guess.

use crate::atom::molecule_key_codec::storage_profile::RangeStorageProfile;
use crate::schema::{types::FieldValueType, Schema};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct RangeFieldInventory {
    pub schema: String,
    pub range_field: String,
    pub field_type: FieldValueType,
    pub proposed_profile: Option<RangeStorageProfile>,
}

impl RangeFieldInventory {
    pub(super) fn from_schema(schema: &Schema) -> Option<Self> {
        let range = schema.key.as_ref()?.range_field.as_ref()?;
        let field_type = schema.key_field_type(range);
        Some(Self {
            schema: schema.name.clone(),
            range_field: range.clone(),
            proposed_profile: RangeStorageProfile::typed(&field_type).ok(),
            field_type,
        })
    }
}
