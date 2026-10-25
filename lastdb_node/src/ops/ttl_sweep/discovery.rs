//! List dated keys through bounded reads, without exhausting one request's
//! read-work budget on a large expired hash.

use fold_db::schema::types::field::HashRangeFilter;
use fold_db::schema::types::operations::Query;
use fold_db::schema::types::schema::DeclarativeSchemaType;
use fold_db::schema::types::KeyValue;
use std::sync::atomic::{AtomicBool, Ordering};

use super::{key_is_expired, owner_context};
use crate::host::Host;

const PAGE_KEYS: usize = 256;

pub(super) async fn expired_range_keys(
    host: &Host,
    schema_name: &str,
    range_field: &str,
    filter: &HashRangeFilter,
    schema_type: &DeclarativeSchemaType,
    cutoff: u64,
    shutdown: &AtomicBool,
) -> Result<Vec<KeyValue>, String> {
    let mut after: Option<KeyValue> = None;
    let mut keys = Vec::new();
    let access = owner_context(host);
    loop {
        if shutdown.load(Ordering::Acquire) {
            return Ok(keys);
        }
        let query = Query::new_with_filter(
            schema_name.to_string(),
            vec![range_field.to_string()],
            Some(filter.clone()),
        );
        let executor = host.db.query_executor();
        let rows = match after.clone() {
            Some(cursor) => {
                executor
                    .query_with_access_keyset_windowed(query, cursor, PAGE_KEYS, &access)
                    .await
            }
            None => {
                executor
                    .query_with_access_windowed(query, (0, PAGE_KEYS), &access)
                    .await
            }
        }
        .map_err(|error| error.to_string())?;
        let field = rows.get(range_field);
        let Some(next) = field
            .and_then(|values| values.keys().max_by(|a, b| a.cmp_page_order(b)))
            .cloned()
        else {
            return Ok(keys);
        };
        if after
            .as_ref()
            .is_some_and(|previous| !next.cmp_page_order(previous).is_gt())
        {
            return Err("TTL discovery cursor did not advance".to_string());
        }
        // Advance over undated keys too. Stopping on a short nonempty page or
        // starting again at zero could strand later expired keys.
        keys.extend(
            field
                .into_iter()
                .flat_map(|values| values.keys())
                .filter(|key| key_is_expired(key, schema_type, cutoff))
                .cloned(),
        );
        after = Some(next);
    }
}
