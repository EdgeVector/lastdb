//! A named layout proves both declared coordinates before its result window.

use super::*;
use crate::schema::types::key_value::scalar_key_coordinate;

impl HashRangeQueryProcessor {
    pub(super) async fn retain_named_layout(
        &self,
        schema: &Schema,
        primary_name: &str,
        values: &mut HashMap<KeyValue, FieldValue>,
        sources: &HashMap<KeyValue, KeySource>,
        include_tombstones: bool,
    ) -> Result<HashMap<KeyValue, FieldValue>, SchemaError> {
        values.retain(|key, value| key.hash_matches_scalar(&value.value));
        let Some(range_field) = schema
            .key
            .as_ref()
            .and_then(|key| key.range_field.as_deref())
        else {
            return Ok(HashMap::new());
        };
        if range_field == primary_name {
            values.retain(|key, value| {
                scalar_key_coordinate(&value.value).as_deref() == key.range.as_deref()
            });
            return Ok(HashMap::new());
        }
        let keys: Vec<_> = values.keys().cloned().collect();
        let mut pending = Vec::new();
        self.secondary_matches_current(
            schema,
            range_field,
            &keys,
            sources,
            include_tombstones,
            &mut pending,
        )
        .await?;
        let mut fields = HashMap::new();
        self.fill_cokey_secondaries(schema, pending, include_tombstones, &mut fields)
            .await?;
        let mut range_values = fields.remove(range_field).unwrap_or_default();
        values.retain(|key, _| {
            range_values.get(key).is_some_and(|value| {
                scalar_key_coordinate(&value.value).as_deref() == key.range.as_deref()
            })
        });
        range_values.retain(|key, _| values.contains_key(key));
        Ok(range_values)
    }
}
