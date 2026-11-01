//! Integrity-aware result of an exact fixed-field record read.

/// Presence of the selected fields: all tombstones are absent, and a mix of
/// live and tombstone values is corrupt.
#[derive(Debug)]
pub(super) enum CurrentRowFields {
    Absent,
    Present,
    Corrupt,
}

impl CurrentRowFields {
    pub(super) fn has_live_field(&self) -> bool {
        matches!(self, Self::Present | Self::Corrupt)
    }
}
