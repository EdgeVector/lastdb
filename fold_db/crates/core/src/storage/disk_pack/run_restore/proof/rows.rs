use super::{budget, invalid, map, read_budget, StorageResult};
use laststore::disk_run::home::{RunHashRange, RunRead};

pub(super) fn read(source: &RunRead, key_heads: bool) -> StorageResult<Option<RunRead>> {
    key_heads
        .then(|| source.fork_bounded(read_budget(budget::ATOM_INVENTORY_READ_BYTES, 10000)))
        .transpose()
        .map_err(map)
}

enum Mode<'a> {
    Persistent(RunHashRange<'a>),
    Stateless(&'a RunRead, &'static str),
}

/// One bounded row at a time, including progress across tombstone-only pages.
pub(super) struct Rows<'a> {
    mode: Mode<'a>,
    after: Option<String>,
    exhausted: bool,
}

impl<'a> Rows<'a> {
    pub(super) fn new(
        source: &'a RunRead,
        read: Option<&'a mut RunRead>,
        collection: &'static str,
    ) -> StorageResult<Self> {
        let mode = match read {
            Some(read) => Mode::Persistent(
                read.administrative_range(collection, &[], &[0xff])
                    .map_err(map)?,
            ),
            None => Mode::Stateless(source, collection),
        };
        Ok(Self {
            mode,
            after: None,
            exhausted: false,
        })
    }

    pub(super) fn next(&mut self) -> StorageResult<Option<(String, Vec<u8>)>> {
        while !self.exhausted {
            let mut page = match &mut self.mode {
                Mode::Persistent(cursor) => cursor
                    .next_backup_page(1, budget::ATOM_BODY_MAX_BYTES)
                    .map_err(map)?,
                Mode::Stateless(source, collection) => {
                    stateless_page(source, collection, self.after.as_deref())?
                }
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

fn stateless_page(
    source: &RunRead,
    collection: &str,
    after: Option<&str>,
) -> StorageResult<laststore::disk_run::home::RunRangePage> {
    let mut read = source
        .fork_bounded(read_budget(budget::ATOM_INVENTORY_READ_BYTES, 10000))
        .map_err(map)?;
    let mut cursor = match after {
        Some(after) => read.administrative_range_after(collection, after.as_bytes(), &[0xff]),
        None => read.administrative_range(collection, &[], &[0xff]),
    }
    .map_err(map)?;
    cursor
        .next_page_yielding_bounded(1, 0, 0, budget::ATOM_BODY_MAX_BYTES)
        .map_err(map)
}
