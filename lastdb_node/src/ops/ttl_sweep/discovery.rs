//! One bounded discovery page at a time. Cursors survive deletion of the
//! preceding page; a new pass starts from the oldest remaining key.

use fold_db::schema::types::field::HashRangeFilter;
use fold_db::schema::types::operations::Query;
use fold_db::schema::types::schema::DeclarativeSchemaType;
use fold_db::schema::types::KeyValue;

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
        } => {
            if *partition >= hashes.len() {
                backlog.source = Source::Done;
                return Ok(());
            }
            let filter = range_filter(&backlog.schema_type, &hashes[*partition], backlog.cutoff);
            let rows = range_page(
                host,
                &backlog.schema_name,
                field,
                filter,
                after.clone(),
                limit,
            )
            .await?;
            let next = rows.iter().max_by(|a, b| a.cmp_page_order(b)).cloned();
            if let Some(next) = next {
                if after
                    .as_ref()
                    .is_some_and(|previous| !next.cmp_page_order(previous).is_gt())
                {
                    return Err("TTL discovery cursor did not advance".into());
                }
                *after = Some(next);
            } else {
                *partition += 1;
                *after = None;
            }
            rows.into_iter()
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

async fn range_page(
    host: &Host,
    schema: &str,
    field: &str,
    filter: HashRangeFilter,
    after: Option<KeyValue>,
    limit: usize,
) -> Result<Vec<KeyValue>, String> {
    let query = Query::new_with_filter(schema.to_string(), vec![field.to_string()], Some(filter));
    let access = owner_context(host);
    let executor = host.db.query_executor();
    let mut rows = match after {
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
    .map_err(|error| error.to_string())?;
    Ok(rows
        .remove(field)
        .into_iter()
        .flat_map(std::collections::HashMap::into_keys)
        .collect())
}
