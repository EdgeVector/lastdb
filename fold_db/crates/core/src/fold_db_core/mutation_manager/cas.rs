//! Integrity-aware result of an exact fixed-field record read.
use std::collections::BTreeMap;

#[derive(Debug)]
pub(super) enum CurrentRowFields {
    Absent,
    Present(BTreeMap<String, serde_json::Value>),
    Corrupt { field: String, atom_uuid: String },
}

impl CurrentRowFields {
    pub(super) fn has_live_field(&self) -> bool {
        matches!(self, Self::Present(_) | Self::Corrupt { .. })
    }
}
