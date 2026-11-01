//! One bounded discovery page at a time. Cursors survive deletion of the
//! preceding page; a new pass starts from the oldest remaining key, or after
//! the floor of a hash (below).
//!
//! One request examines at most 8,192 record keys, and a deleted record is a
//! key. A Range or HashRange page that passes that many deleted records before
//! its window is full ends early and names the last key it examined (the page
//! tally of the query, `with_query_page_tally`). The next page starts after
//! that key. So a run of deleted records of any length costs pages; it does not
//! fail the pass.
//!
//! The erase of the sweep leaves deleted records at the oldest keys of each
//! hash, and each pass would read them again from the start of the hash. So
//! the sweep keeps a floor for each hash, in memory: the key where a page of
//! the leading run of deleted records ended. The next pass of the hash starts
//! after its floor. A write below the floor (a peer import of old records, or
//! a key with an old entry time) is not seen while the floor stays. Once in
//! [`floors::FULL_PASS_EVERY`] passes of a hash, and after a restart of the
//! node, the pass reads the hash from its start again.

use fold_db::db_operations::{with_query_page_tally, PageResume};
use fold_db::schema::types::field::HashRangeFilter;
use fold_db::schema::types::operations::Query;
use fold_db::schema::types::schema::DeclarativeSchemaType;
use fold_db::schema::types::KeyValue;
use fold_db::schema::types::KeyValuePageOrder;

use super::backlog::Backlog;
use super::{key_is_expired, owner_context, partition_then_age, AGE_KEY_DIGITS};
use crate::host::Host;

pub(super) enum Source {
    Ages {
        after: Option<String>,
    },
    Range {
        field: String,
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
            field,
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
            if after.is_none() {
                *after = floors::start(&backlog.schema_name, hash);
                *leading = true;
            }
            let filter = range_filter(&backlog.schema_type, hash, backlog.cutoff);
            let page = range_page(
                host,
                &backlog.schema_name,
                field,
                filter,
                after.clone(),
                limit,
            )
            .await?;
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
                    floors::keep(&backlog.schema_name, hash, &next);
                }
                *after = Some(next);
            } else {
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

fn range_filter(kind: &DeclarativeSchemaType, hash: &str, cutoff: u64) -> HashRangeFilter {
    let start = "0".repeat(AGE_KEY_DIGITS);
    let end = format!("{cutoff:0AGE_KEY_DIGITS$}");
    if matches!(kind, DeclarativeSchemaType::HashRange) {
        HashRangeFilter::HashRangeRange {
            hash: hash.to_string(),
            start,
            end,
        }
    } else {
        HashRangeFilter::RangeRange { start, end }
    }
}

/// One page of a range read: the keys of the live rows with the field, and
/// the last key that the read examined when a run of deleted records ended
/// the page early.
struct RangePage {
    keys: Vec<KeyValue>,
    resume: Option<KeyValue>,
}

async fn range_page(
    host: &Host,
    schema: &str,
    field: &str,
    filter: HashRangeFilter,
    after: Option<KeyValue>,
    limit: usize,
) -> Result<RangePage, String> {
    let query = Query::new_with_filter(schema.to_string(), vec![field.to_string()], Some(filter));
    let access = owner_context(host);
    let executor = host.db.query_executor();
    // The page tally lets the read end early in a run of deleted records and
    // name where it ended, where a read without it fails at the read limit.
    let (rows, _, end) = with_query_page_tally(async {
        match after {
            Some(cursor) => {
                executor
                    .query_with_access_keyset_windowed(query, cursor, limit, &access)
                    .await
            }
            None => {
                executor
                    .query_with_access_windowed(query, (0, limit), &access)
                    .await
            }
        }
    })
    .await;
    let rows = rows.map_err(|error| error.to_string())?;
    let resume = match end.resume {
        Some(PageResume::Key(key)) => Some(key),
        Some(PageResume::Record(_)) => {
            return Err("TTL discovery read ended at a record walk position".into())
        }
        None => None,
    };
    Ok(RangePage {
        keys: rows
            .into_iter()
            .filter(|(_, record)| record.fields.contains_key(field))
            .map(|(key, _)| key)
            .collect(),
        resume,
    })
}

/// The floor of each hash of each schema: where the leading run of deleted
/// records ended in an earlier pass. Process memory only.
pub(super) mod floors {
    use fold_db::schema::types::KeyValue;
    use std::collections::HashMap;
    use std::sync::{LazyLock, Mutex};

    /// A pass of a hash starts from the start of the hash once in this many
    /// passes, so a write below the floor is erased in time.
    pub(super) const FULL_PASS_EVERY: u32 = 64;

    struct Floor {
        after: KeyValue,
        /// Passes of the hash that started after this floor.
        passes: u32,
    }

    type Floors = HashMap<(String, String), Floor>;

    static FLOORS: LazyLock<Mutex<Floors>> = LazyLock::new(Mutex::default);

    /// Where a pass of `hash` starts: after its floor, or at its start when it
    /// has none or a full pass is due.
    pub(super) fn start(schema: &str, hash: &str) -> Option<KeyValue> {
        let mut floors = FLOORS.lock().ok()?;
        let key = (schema.to_owned(), hash.to_owned());
        let passes = {
            let floor = floors.get_mut(&key)?;
            floor.passes += 1;
            floor.passes
        };
        if passes >= FULL_PASS_EVERY {
            floors.remove(&key);
            return None;
        }
        floors.get(&key).map(|floor| floor.after.clone())
    }

    /// Move the floor of `hash` to `after`: every key up to it was a deleted
    /// record when this pass read it.
    pub(super) fn keep(schema: &str, hash: &str, after: &KeyValue) {
        let Ok(mut floors) = FLOORS.lock() else {
            return;
        };
        let key = (schema.to_owned(), hash.to_owned());
        let passes = floors.get(&key).map_or(0, |floor| floor.passes);
        floors.insert(
            key,
            Floor {
                after: after.clone(),
                passes,
            },
        );
    }
}
