//! Borrowed JSON values with deterministic object order and no body clone.

use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};
use serde_json::Value;
use std::collections::BTreeMap;

/// Sort every nested object independently of serde_json's map backend.
/// Arrays retain their declared order; scalar serialization stays unchanged.
pub(crate) struct CanonicalValue<'a>(pub(crate) &'a Value);

impl Serialize for CanonicalValue<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            Value::Object(values) => {
                let sorted: BTreeMap<_, _> = values.iter().collect();
                let mut map = serializer.serialize_map(Some(sorted.len()))?;
                for (name, value) in sorted {
                    map.serialize_entry(name, &CanonicalValue(value))?;
                }
                map.end()
            }
            Value::Array(values) => serializer.collect_seq(values.iter().map(CanonicalValue)),
            value => value.serialize(serializer),
        }
    }
}
