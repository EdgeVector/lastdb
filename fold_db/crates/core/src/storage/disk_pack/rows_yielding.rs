//! Range reads that continue across a budget stop in a run of deleted keys.

use super::rows::{cursor, decode_native_rows, require_page_limit, NativeRows};
use super::*;

impl PackKvStore {
    /// Read up to `limit` rows of `start..end`, and name where to continue.
    ///
    /// Same rows as [`Self::range_rows`], but a call that runs short of key
    /// budget returns the rows it has and `resume`, the last key it examined,
    /// instead of an error. It stops while `keys_per_row` keys for each row are
    /// left, for the reads that the caller makes for those rows. `None` means the range ended. A
    /// write scope, a range over more than one collection and a range that is
    /// not UTF-8 read as `range_rows` does, and `resume` is the last row of a
    /// full page.
    pub(super) async fn range_rows_yielding(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
        keys_per_row: usize,
    ) -> StorageResult<(Vec<(Vec<u8>, Vec<u8>)>, Option<Vec<u8>>)> {
        let collections = self.owned_range_collections(start, end);
        let simple = limit > 0
            && start < end
            && collections.len() == 1
            && std::str::from_utf8(start).is_ok_and(|text| !text.starts_with("b64:"))
            && std::str::from_utf8(end).is_ok()
            && transaction::PendingBatch::current(&self.runtime).is_none();
        if !simple {
            let rows = self.range_rows(start, end, limit, false).await?;
            let resume = (limit > 0 && rows.len() >= limit)
                .then(|| rows.last().map(|(key, _)| key.clone()))
                .flatten();
            return Ok((rows, resume));
        }
        let (start, end) = (start.to_vec(), end.to_vec());
        let runtime = Arc::clone(&self.runtime);
        let context = read::ReadContext::current(&runtime).await?;
        let observed = Arc::clone(&context);
        let collection = collections.into_iter().next().expect("one collection");
        let result = run_blocking(move || {
            let (rows, resume) = runtime.raw_rows_yielding(
                &context,
                &collection,
                &start,
                &end,
                limit,
                keys_per_row,
            )?;
            let rows = decode_native_rows(rows)?;
            let resume = resume.map(|key| native_routing::decode(&key)).transpose()?;
            Ok((rows, resume))
        })
        .await;
        observed.remembered(result)
    }
}

impl PackRuntime {
    /// One page of rows in `start..end` with the continuation key.
    ///
    /// `Some(key)` means continue after `key`: the page is full, or the call ran
    /// short of key budget (the page is then short or empty, and `key` is the
    /// last key examined, deleted or live). `None` means the range ended.
    pub(super) fn raw_rows_yielding(
        &self,
        context: &Arc<read::ReadContext>,
        collection: &str,
        start: &[u8],
        end: &[u8],
        limit: usize,
        keys_per_row: usize,
    ) -> StorageResult<(NativeRows, Option<String>)> {
        let result =
            self.raw_rows_yielding_inner(context, collection, start, end, limit, keys_per_row);
        context.remembered(result)
    }

    fn raw_rows_yielding_inner(
        &self,
        context: &Arc<read::ReadContext>,
        collection: &str,
        start: &[u8],
        end: &[u8],
        limit: usize,
        keys_per_row: usize,
    ) -> StorageResult<(NativeRows, Option<String>)> {
        context.validate_owner(None)?;
        require_page_limit(limit)?;
        let mut call = crate::read_receipts::lock(&context.call);
        let mut cursor = cursor(&mut call, collection, start, end)?;
        let mut out: NativeRows = Vec::new();
        loop {
            if out.len() >= limit {
                let last = out.last().map(|(key, _)| key.clone());
                return Ok((out, last));
            }
            let page = match cursor.next_page_yielding(
                (limit - out.len()).min(128),
                out.len(),
                keys_per_row,
            ) {
                Ok(page) => page,
                Err(laststore::Error::ReadBudgetExceeded) if !out.is_empty() => {
                    let last = out.last().map(|(key, _)| key.clone());
                    return Ok((out, last));
                }
                Err(error) => return Err(map(error)),
            };
            out.extend(
                page.records
                    .into_iter()
                    .map(|record| (record.key, record.body)),
            );
            if let Some(resume) = page.resume {
                let resume = String::from_utf8(resume).map_err(|_| {
                    StorageError::InvalidOperation("range storage key is not UTF-8".into())
                })?;
                return Ok((out, Some(resume)));
            }
            if page.exhausted {
                return Ok((out, None));
            }
        }
    }
}
