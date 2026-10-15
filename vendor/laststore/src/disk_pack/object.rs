use super::page::Page;
use super::{corrupt, Location, ReadCall, BLOCK_BYTES, FEATURE_OBJECT_METADATA, MAP_ENTRIES};
use crate::Result;

/// Complete bounds repeated in each immutable block-map page header.
pub(super) struct ObjectMetadata {
    pub(super) logical_bytes: u64,
    pub(super) blocks: u64,
    pub(super) data_parts: u64,
    pub(super) generation: u64,
}

impl ObjectMetadata {
    fn decode(page: &Page) -> Result<Self> {
        let (logical_bytes, data_parts) = page
            .object_bounds()
            .ok_or_else(|| corrupt("required complete object bounds are missing"))?;
        let blocks = logical_bytes.div_ceil(BLOCK_BYTES as u64);
        let maximum_parts = u64::from(u32::MAX) + 1;
        let map_pages = blocks.div_ceil(MAP_ENTRIES);
        if logical_bytes == 0
            || data_parts == 0
            || data_parts > blocks
            || data_parts > maximum_parts
            || map_pages > maximum_parts * 512
        {
            return Err(corrupt("complete object bounds exceed format limits"));
        }
        Ok(Self {
            logical_bytes,
            blocks,
            data_parts,
            generation: page.generation(),
        })
    }

    pub(super) fn check_block(
        &self,
        ordinal: u64,
        part: u32,
        decoded: usize,
        count: usize,
    ) -> Result<()> {
        let first = (ordinal / MAP_ENTRIES) * MAP_ENTRIES;
        let expected = (self.blocks - first).min(MAP_ENTRIES);
        let block_bytes = if ordinal + 1 == self.blocks {
            (self.logical_bytes - 1) % BLOCK_BYTES as u64 + 1
        } else {
            BLOCK_BYTES as u64
        };
        if u64::from(part) >= self.data_parts
            || count as u64 != expected
            || decoded as u64 != block_bytes
        {
            return Err(corrupt("block map exceeds complete object bounds"));
        }
        Ok(())
    }
}

impl ReadCall {
    pub(super) fn journal_object_metadata(
        &mut self,
        stream: uuid::Uuid,
        batch: u64,
    ) -> Result<ObjectMetadata> {
        super::bytes::nonnil(stream)?;
        if batch == 0
            || batch > self.current.journal_frontier
            || self.current.required_features & FEATURE_OBJECT_METADATA == 0
        {
            return Err(corrupt("journal object outside held publication boundary"));
        }
        let path = self
            .root
            .join("journal")
            .join(stream.to_string())
            .join(format!("batch-{batch:016x}"));
        let page = self.map_page(&path, stream, 0)?;
        ObjectMetadata::decode(&page)
    }

    pub(super) fn object_metadata(
        &mut self,
        location: &Location,
    ) -> Result<Option<ObjectMetadata>> {
        if self.current.required_features & FEATURE_OBJECT_METADATA == 0 {
            return Ok(None);
        }
        let base = self.data_path(location);
        let page = self.map_page(&base, location.object, location.first_block / MAP_ENTRIES)?;
        ObjectMetadata::decode(&page).map(Some)
    }
}
