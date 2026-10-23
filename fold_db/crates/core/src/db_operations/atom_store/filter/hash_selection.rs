//! One API selector and cursor normalized for ready keys and physical tip reads.

use crate::atom::MoleculeKeyCodec;
use crate::schema::types::field::{HashRangeFilter, KeyWindow};
use crate::schema::SchemaError;

pub(crate) struct HashTipSelection<'a> {
    pub(crate) api_hash: &'a str,
    pub(crate) storage_hash: String,
    pub(crate) range_start: String,
    pub(crate) range_end: Option<String>,
    pub(crate) range_prefix: Option<String>,
    pub(crate) skip: usize,
    pub(crate) limit: usize,
    /// API range excluded by the cursor; an equal range with a later hash is included.
    pub(crate) after: Option<String>,
    pub(crate) empty: bool,
}

impl<'a> HashTipSelection<'a> {
    pub(crate) fn new(
        codec: &MoleculeKeyCodec,
        molecule: &str,
        filter: &'a HashRangeFilter,
        window: &KeyWindow,
    ) -> Result<Option<Self>, SchemaError> {
        let (api_hash, start, end, prefix) = match filter {
            HashRangeFilter::HashKey(hash) => (hash.as_str(), "", None, None),
            HashRangeFilter::HashRangePrefix { hash, prefix } => (
                hash.as_str(),
                prefix.as_str(),
                None,
                (!prefix.is_empty()).then_some(prefix.as_str()),
            ),
            HashRangeFilter::HashRangeRange { hash, start, end } => {
                (hash.as_str(), start.as_str(), Some(end.as_str()), None)
            }
            _ => return Ok(None),
        };
        let (skip, limit, cursor) = match window {
            KeyWindow::Offset { offset, limit } => (*offset, *limit, None),
            KeyWindow::After { after, limit } => (0, *limit, Some(after)),
        };
        let after = cursor
            .filter(|key| api_hash <= key.hash.as_deref().unwrap_or(""))
            .map(|key| key.range.clone().unwrap_or_default());
        let mut selection = Self {
            api_hash,
            storage_hash: String::new(),
            range_start: String::new(),
            range_end: None,
            range_prefix: None,
            skip,
            limit,
            after,
            empty: limit == 0 || end.is_some_and(|end| start >= end),
        };
        // Empty intervals need no encryption keys or physical key construction.
        if selection.empty {
            return Ok(Some(selection));
        }
        let encode = |range| codec.storage_range(molecule, range).map_err(codec_error);
        selection.storage_hash = codec
            .storage_hash(molecule, api_hash)
            .map_err(codec_error)?;
        selection.range_start = encode(start)?;
        selection.range_end = end.map(encode).transpose()?;
        selection.range_prefix = prefix.map(encode).transpose()?;
        if let Some(cursor) = cursor {
            let cursor = encode(cursor.range.as_deref().unwrap_or(""))?;
            selection.range_start = selection.range_start.max(cursor);
        }
        selection.empty = selection
            .range_end
            .as_ref()
            .is_some_and(|end| selection.range_start >= *end)
            || selection
                .range_prefix
                .as_ref()
                .is_some_and(|prefix| !selection.range_start.starts_with(prefix.as_str()));
        Ok(Some(selection))
    }

    /// Empty OPE ranges use their own physical key outside the nonempty lane.
    pub(crate) fn accepts_empty(&self) -> bool {
        !self.empty && self.range_start.is_empty() && self.after.is_none()
    }
}

fn codec_error(error: impl std::fmt::Display) -> SchemaError {
    SchemaError::InvalidData(format!("hash range selection codec: {error}"))
}
