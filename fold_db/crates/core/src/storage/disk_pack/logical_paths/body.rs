use super::*;
use crate::schema::types::field::FieldValue;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ReadVersion {
    pub generation: u64,
    pub accepted: usize,
}

/// Only one field's content and provenance belong to this logical path.
#[derive(Serialize, Deserialize)]
pub(super) struct PathBody {
    pub content: serde_json::Value,
    pub provenance: Option<(ReadVersion, FieldValue)>,
    #[serde(default)]
    pub wire: Option<super::warm::WireMetadata>,
}

impl PathBody {
    pub(super) fn content(value: &serde_json::Value) -> Self {
        Self {
            content: value.clone(),
            provenance: None,
            wire: None,
        }
    }

    pub(super) fn from_field(version: ReadVersion, value: &FieldValue) -> Self {
        let mut provenance = value.clone();
        provenance.value = serde_json::Value::Null;
        Self {
            content: value.value.clone(),
            provenance: Some((version, provenance)),
            wire: None,
        }
    }

    pub(super) fn field(self, version: ReadVersion) -> Option<FieldValue> {
        let (cached_version, mut value) = self.provenance?;
        if version != cached_version {
            return None;
        }
        value.value = self.content;
        Some(value)
    }
}
