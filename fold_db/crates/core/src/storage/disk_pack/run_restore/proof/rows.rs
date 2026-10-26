use super::{budget, invalid, map, read_budget, StorageResult};
use laststore::disk_run::home::RunRead;

/// One bounded row at a time, including progress across tombstone-only pages.
pub(super) struct Rows<'a> {
    source: &'a RunRead,
    collection: &'static str,
    after: Option<String>,
    exhausted: bool,
}

impl<'a> Rows<'a> {
    pub(super) fn new(source: &'a RunRead, collection: &'static str) -> Self {
        Self {
            source,
            collection,
            after: None,
            exhausted: false,
        }
    }

    pub(super) fn next(&mut self) -> StorageResult<Option<(String, Vec<u8>)>> {
        while !self.exhausted {
            let mut page = {
                let mut read = self
                    .source
                    .fork_bounded(read_budget(budget::ATOM_INVENTORY_READ_BYTES, 10000))
                    .map_err(map)?;
                let mut cursor = match &self.after {
                    Some(after) => {
                        read.administrative_range_after(self.collection, after.as_bytes(), &[0xff])
                    }
                    None => read.administrative_range(self.collection, &[], &[0xff]),
                }
                .map_err(map)?;
                cursor
                    .next_page_yielding_bounded(1, 0, 0, budget::ATOM_BODY_MAX_BYTES)
                    .map_err(map)?
            };
            if page.records.len() > 1 {
                return Err(invalid("run proof page shape differs"));
            }
            let next = page
                .resume
                .clone()
                .or_else(|| page.records.last().map(|(key, _)| key.clone()));
            if let Some((key, _)) = page.records.first() {
                if self.after.as_ref().is_some_and(|after| key <= after) {
                    return Err(invalid("run proof record order differs"));
                }
            }
            if !page.exhausted
                && next
                    .as_ref()
                    .is_none_or(|next| self.after.as_ref().is_some_and(|after| next <= after))
            {
                return Err(invalid("run proof continuation differs"));
            }
            self.after = next;
            self.exhausted = page.exhausted;
            if let Some(row) = page.records.pop() {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }
}
