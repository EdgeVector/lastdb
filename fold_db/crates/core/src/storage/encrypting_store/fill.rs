//! Paged scans that stay full across rows with no at-rest envelope.
//!
//! A paged scan returns `limit` rows unless the range ends, and every loop
//! that reads pages relies on it: a page shorter than the limit is the end of
//! the range. This store drops a row that has no at-rest envelope after the
//! inner store applied the limit, so a page of `limit` raw rows can open to
//! fewer rows. Without a refill, one such row cuts a scan short with no error,
//! and the caller takes the cut for the end of the range.
//!
//! The refill reads on from the key after the last raw row, so the key of a
//! dropped row never has to be known. A page is short only when the inner
//! store returned fewer raw rows than it was asked for. The first read is the
//! read the caller made; a range with no dropped row costs no other read.

use super::*;

/// The fewest raw rows a refill read asks for.
const REFILL_MIN_ROWS: usize = 16;
/// The most raw rows a refill read asks for. A long run of dropped rows widens
/// the read to this size, so the number of reads follows the run, not the page.
const REFILL_MAX_ROWS: usize = 4096;

/// Where a paged scan starts.
pub(super) enum PagedSource<'a> {
    /// Every key under the prefix.
    Prefix(&'a [u8]),
    /// The keys in `start..end`.
    Range(&'a [u8], &'a [u8]),
}

impl EncryptingKvStore {
    /// Read up to `limit` opened rows of `source`, in key order. The page is
    /// short only when the range holds no more rows.
    pub(super) async fn scan_paged_filled(
        &self,
        source: PagedSource<'_>,
        limit: usize,
    ) -> StorageResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut raw = match source {
            PagedSource::Prefix(prefix) => self.inner.scan_prefix_paged(prefix, limit).await?,
            PagedSource::Range(start, end) => {
                self.inner.scan_range_paged(start, end, limit).await?
            }
        };
        let mut asked = limit;
        let mut out = Vec::with_capacity(raw.len());
        let mut floor = REFILL_MIN_ROWS;
        loop {
            let exhausted = raw.len() < asked;
            let last = raw.last().map(|(key, _)| key.clone());
            for (key, stored) in raw {
                if out.len() >= limit {
                    break;
                }
                if let Some(plaintext) = self.open_or_discard(&key, stored).await? {
                    out.push((key, plaintext));
                }
            }
            if exhausted || out.len() >= limit {
                return Ok(out);
            }
            let Some(last) = last else {
                return Ok(out);
            };
            let Some(end) = source_end(&source) else {
                return Err(StorageError::InvalidOperation(
                    "a paged scan met a row with no at-rest envelope and the prefix has no upper bound"
                        .into(),
                ));
            };
            let next = key_after(&last);
            if next.as_slice() >= end.as_slice() {
                return Ok(out);
            }
            asked = (limit - out.len()).max(floor);
            floor = (floor * 4).min(REFILL_MAX_ROWS);
            raw = self.inner.scan_range_paged(&next, &end, asked).await?;
        }
    }
}

/// The exclusive end of the range a source reads.
fn source_end(source: &PagedSource<'_>) -> Option<Vec<u8>> {
    match source {
        PagedSource::Range(_, end) => Some(end.to_vec()),
        PagedSource::Prefix(prefix) => prefix_end(prefix),
    }
}

/// The least key above `key`.
fn key_after(key: &[u8]) -> Vec<u8> {
    let mut next = Vec::with_capacity(key.len() + 1);
    next.extend_from_slice(key);
    next.push(0);
    next
}

/// The least key that is above every key under `prefix`. `None` when every
/// byte of the prefix is `0xFF`.
fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let at = prefix.iter().rposition(|byte| *byte != 0xFF)?;
    let mut end = prefix[..=at].to_vec();
    end[at] += 1;
    Some(end)
}
