//! One bounded discovery page at a time, plus each schema's discovery cursor
//! and temporary page of expired keys.

use fold_db::db_operations::{with_query_page_tally, PageResume};
use fold_db::fold_db_core::purge::budget::EraseShape;
use fold_db::schema::types::field::HashRangeFilter;
use fold_db::schema::types::operations::Query;
use fold_db::schema::types::schema::DeclarativeSchemaType;
use fold_db::schema::types::KeyValue;
use fold_db::schema::types::KeyValuePageOrder;

use super::{key_is_expired, partition_then_age, AGE_KEY_DIGITS};
use crate::host::Host;

/// Keys that one pass erases from one schema, in the order it erases them.
pub(super) struct Backlog {
    pub(super) schema_name: String,
    pub(super) schema_type: DeclarativeSchemaType,
    /// Fields and alternate keys of the schema. A batch of keys must fit in
    /// one native batch, and its size follows from this shape.
    pub(super) shape: EraseShape,
    /// A cap on the batch size after a batch hit a size limit. `usize::MAX`
    /// means no cap.
    batch_cap: usize,
    /// The cutoff of the listing: a key older than this was expired then.
    pub(super) cutoff: u64,
    pub(super) source: Source,
    keys: Vec<KeyValue>,
    next: usize,
    failed: bool,
    abandoned: usize,
}

impl Backlog {
    pub(super) fn new(
        schema_name: String,
        schema_type: DeclarativeSchemaType,
        shape: EraseShape,
        cutoff: u64,
        source: Source,
    ) -> Self {
        Self {
            schema_name,
            schema_type,
            shape,
            batch_cap: usize::MAX,
            cutoff,
            source,
            keys: Vec::new(),
            next: 0,
            failed: false,
            abandoned: 0,
        }
    }

    /// True when the pass has no batch left for this schema: it reached the
    /// last key, or a batch failed and the pass skips the schema.
    pub(super) fn is_done(&self) -> bool {
        self.failed || (self.source.is_done() && self.left() == 0)
    }

    /// Keys that the pass listed and has not reached.
    pub(super) fn left(&self) -> usize {
        self.abandoned + self.page_left()
    }

    /// Skip this schema for the rest of the pass. Its keys stay counted as left.
    pub(super) fn abandon(&mut self) {
        self.failed = true;
        self.abandoned += self.page_left();
        self.release_page();
    }

    pub(super) fn page_left(&self) -> usize {
        self.keys.len().saturating_sub(self.next)
    }

    /// Only the schema currently served by the worker owns a page. Other
    /// schemas keep cursors, never buffered expired-key pages.
    pub(super) fn set_page(&mut self, keys: Vec<KeyValue>) {
        debug_assert!(self.keys.is_empty());
        self.keys = keys;
        self.next = 0;
    }

    pub(super) fn release_page(&mut self) {
        self.keys = Vec::new();
        self.next = 0;
    }

    /// The next `n` keys. The backlog moves past them.
    pub(super) fn take(&mut self, n: usize) -> Vec<KeyValue> {
        let end = self.keys.len().min(self.next.saturating_add(n));
        let chunk = self.keys[self.next..end].to_vec();
        self.next = end;
        chunk
    }

    /// The batch size cap that earlier size limit refusals set.
    pub(super) fn batch_cap(&self) -> usize {
        self.batch_cap
    }

    /// A batch of `taken` keys hit a size limit. Put its keys back and cap the
    /// next batch at half of it, at least one key.
    pub(super) fn shrink_after_limit(&mut self, taken: usize) {
        self.next = self.next.saturating_sub(taken);
        self.batch_cap = (taken / 2).max(1);
    }

    /// Give up the keys that are left, for example when the policy is gone.
    pub(super) fn clear(&mut self) {
        self.next = self.keys.len();
        self.source = Source::Done;
    }
}

pub(super) enum Source {
    Ages {
        after: Option<String>,
    },
    Range {
        hashes: Vec<String>,
        partition: usize,
        after: Option<KeyValue>,
        /// Every key that this pass read in the current hash was a deleted
        /// record, so a page that ends early can move the floor of the hash.
        leading: bool,
    },
    Done,
}

impl Source {
    pub(super) fn is_done(&self) -> bool {
        matches!(self, Self::Done)
    }
}

/// Fetch one physical page, even if it contains no eligible keys. The
/// scheduler yields to the next schema after this page, so undated or stale
/// entries cannot monopolize discovery. Never infer exhaustion from a short
/// nonempty page or a page whose entries fail the expiry recheck.
pub(super) async fn fill_page(
    host: &Host,
    backlog: &mut Backlog,
    limit: usize,
) -> Result<(), String> {
    let keys = match &mut backlog.source {
        Source::Done => return Ok(()),
        Source::Ages { after } => {
            let (keys, next) = host
                .db
                .db_ops()
                .schemas()
                .expired_schema_retention_page(
                    &backlog.schema_name,
                    backlog.cutoff,
                    after.as_deref(),
                    limit,
                )
                .await
                .map_err(|error| error.to_string())?;
            match next {
                Some(next) => *after = Some(next),
                None => backlog.source = Source::Done,
            }
            keys
        }
        Source::Range {
            hashes,
            partition,
            after,
            leading,
        } => {
            if *partition >= hashes.len() {
                backlog.source = Source::Done;
                return Ok(());
            }
            let hash = &hashes[*partition];
            let floors = host.db.db_ops().schemas().retention_floors();
            if after.is_none() {
                *after = floors.start(&backlog.schema_name, hash);
                *leading = true;
            }
            let filter = range_filter(hash, backlog.cutoff);
            let page = range_page(host, &backlog.schema_name, filter, after.clone(), limit).await?;
            if !page.keys.is_empty() {
                *leading = false;
            }
            // A page that ended early examined keys after its last row, so
            // the next page starts after the key where it ended.
            let next = page
                .keys
                .iter()
                .chain(page.resume.as_ref())
                .max_by(|a, b| a.cmp_page_order(b))
                .cloned();
            if let Some(next) = next {
                if after
                    .as_ref()
                    .is_some_and(|previous| !next.cmp_page_order(previous).is_gt())
                {
                    return Err("TTL discovery cursor did not advance".into());
                }
                if *leading && page.resume.is_some() {
                    floors.keep(&backlog.schema_name, hash, &next);
                }
                *after = Some(next);
            } else {
                floors.end(&backlog.schema_name, hash);
                *partition += 1;
                *after = None;
            }
            page.keys
                .into_iter()
                .filter(|key| key_is_expired(key, &backlog.schema_type, backlog.cutoff))
                .collect()
        }
    };
    let mut keys = keys;
    keys.sort_by(partition_then_age);
    keys.dedup();
    backlog.set_page(keys);
    Ok(())
}

fn range_filter(hash: &str, cutoff: u64) -> HashRangeFilter {
    let start = "0".repeat(AGE_KEY_DIGITS);
    let end = format!("{cutoff:0AGE_KEY_DIGITS$}");
    HashRangeFilter::HashRangeRange {
        hash: hash.to_string(),
        start,
        end,
    }
}

/// One page of a range read: the external keys of the live records, and
/// the last key that the read examined when a run of deleted records ended
/// the page early.
struct RangePage {
    keys: Vec<KeyValue>,
    resume: Option<KeyValue>,
}

async fn range_page(
    host: &Host,
    schema: &str,
    filter: HashRangeFilter,
    after: Option<KeyValue>,
    limit: usize,
) -> Result<RangePage, String> {
    let query = Query::new_with_filter(schema.to_string(), Some(filter));
    let executor = host.db.query_executor();
    // The page tally lets the read end early in a run of deleted records and
    // name where it ended, where a read without it fails at the read limit.
    let (rows, end) = with_query_page_tally(async {
        match after {
            Some(cursor) => executor.query_keyset_windowed(query, cursor, limit).await,
            None => executor.query_windowed(query, (0, limit)).await,
        }
    })
    .await;
    let rows = rows.map_err(|error| error.to_string())?;
    let resume = end.resume.map(|PageResume::Key(key)| key);
    Ok(RangePage {
        keys: rows.into_keys().collect(),
        resume,
    })
}
