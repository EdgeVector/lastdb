//! Caller-specific body admission before any payload block or record allocation.
use super::{Location, ReadCall};
use crate::Result;

impl ReadCall {
    /// Locate one exact key and reject its body length before payload allocation.
    pub fn get_bounded(
        &mut self,
        collection: &str,
        group: u16,
        key: &str,
        maximum: u64,
    ) -> Result<Option<Vec<u8>>> {
        let Some(location) = self.locate(collection, group, key)? else {
            return Ok(None);
        };
        Self::check_body_bound(&location, key, maximum)?;
        self.read_record(collection, key, &location).map(Some)
    }

    pub(super) fn check_body_bound(location: &Location, key: &str, maximum: u64) -> Result<()> {
        let body = location
            .record_bytes
            .checked_sub(96 + key.len() as u64)
            .ok_or_else(|| super::corrupt("record shorter than complete key"))?;
        if body > maximum {
            return Err(super::resource());
        }
        Ok(())
    }
}
