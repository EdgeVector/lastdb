//! Range reads that continue across a budget stop in a run of deleted keys.

use super::super::laststore::{native_routing, LastStoreKvStore};
use super::rows::{cursor, NativeRows};
use super::*;

impl PackKvStore {
    /// Read up to `limit` rows of `start..end`, and name where to continue.
    ///
    /// Same rows as [`Self::range_rows`], but a call that runs out of key budget
    /// in a run of deleted keys returns the rows it has and `resume`, the last
    /// key it examined, instead of an error. `None` means the range ended. A
    /// write scope, a range over more than one collection and a range that is
    /// not UTF-8 read as `range_rows` does, and `resume` is the last row of a
    /// full page.
    pub(super) async fn range_rows_yielding(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> StorageResult<(Vec<(Vec<u8>, Vec<u8>)>, Option<Vec<u8>>)> {
        let collections: Vec<String> = if self.namespace == "main" {
            native_routing::range_collections(start, end)
                .into_iter()
                .map(str::to_string)
                .collect()
        } else {
            vec![self.namespace.clone()]
        };
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
        let result = LastStoreKvStore::run_blocking(move || {
            let (rows, resume) =
                runtime.raw_rows_yielding(&context, &collection, &start, &end, limit)?;
            let rows = rows
                .into_iter()
                .map(|(key, body)| Ok((native_routing::decode(&key)?, body)))
                .collect::<StorageResult<Vec<_>>>()?;
            let resume = resume.map(|key| native_routing::decode(&key)).transpose()?;
            Ok((rows, resume))
        })
        .await;
        observed.remember(&result);
        result
    }
}

impl PackRuntime {
    /// One page of rows in `start..end` with the continuation key.
    ///
    /// `Some(key)` means continue after `key`: the page is full, or the call ran
    /// out of key budget in a run of deleted keys (the page is then short or
    /// empty). `None` means the range ended. A budget failure after the call
    /// selected a live row returns the rows read so far and the last of them.
    pub(super) fn raw_rows_yielding(
        &self,
        context: &Arc<read::ReadContext>,
        collection: &str,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> StorageResult<(NativeRows, Option<String>)> {
        let result = self.raw_rows_yielding_inner(context, collection, start, end, limit);
        context.remember(&result);
        result
    }

    fn raw_rows_yielding_inner(
        &self,
        context: &Arc<read::ReadContext>,
        collection: &str,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> StorageResult<(NativeRows, Option<String>)> {
        context.validate_owner(None)?;
        if limit > 10000 {
            return Err(StorageError::ReadResourceLimit {
                detail: "native page exceeds 10000 records".into(),
            });
        }
        let mut call = crate::read_receipts::lock(&context.call);
        let mut cursor = cursor(&mut call, collection, start, end)?;
        let mut out: NativeRows = Vec::new();
        loop {
            if out.len() >= limit {
                let last = out.last().map(|(key, _)| key.clone());
                return Ok((out, last));
            }
            let page = match cursor.next_page_yielding((limit - out.len()).min(128)) {
                Ok(page) => page,
                Err(laststore::Error::ReadBudgetExceeded) if !out.is_empty() => {
                    let last = out.last().map(|(key, _)| key.clone());
                    return Ok((out, last));
                }
                Err(error) => return Err(map(error)),
            };
            if let Some(resume) = page.resume {
                let resume = String::from_utf8(resume).map_err(|_| {
                    StorageError::InvalidOperation("range storage key is not UTF-8".into())
                })?;
                return Ok((out, Some(resume)));
            }
            out.extend(
                page.records
                    .into_iter()
                    .map(|record| (record.key, record.body)),
            );
            if page.exhausted {
                return Ok((out, None));
            }
        }
    }
}
