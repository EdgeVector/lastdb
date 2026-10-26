//! Bounded age-index reads for the node-local TTL worker.

use super::catalog_keys::{
    retention_age_latest_key, retention_age_prefix, SchemaRetentionAgeEntry,
};
use super::SchemaStore;
use crate::schema::types::KeyValue;
use crate::schema::SchemaError;

impl SchemaStore {
    /// Read at most 256 age entries older than `cutoff`. The continuation is
    /// the last physical age key, even when every entry was superseded. Only
    /// an empty physical page ends the interval. Deleting the preceding page
    /// does not invalidate this exclusive continuation.
    pub async fn expired_schema_retention_page(
        &self,
        schema_name: &str,
        cutoff: u64,
        after: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<KeyValue>, Option<String>), SchemaError> {
        let anchor = self.retention_anchor(schema_name).await?;
        let prefix = retention_age_prefix(&anchor);
        let first = format!("{prefix}{:020}", 0);
        let end = format!("{prefix}{cutoff:020}");
        let start = match after {
            Some(cursor) if cursor >= first.as_str() && cursor < end.as_str() => {
                format!("{cursor}\0")
            }
            Some(_) => return Err(SchemaError::InvalidData("invalid TTL age cursor".into())),
            None => first,
        };
        let rows = self
            .schema_states_store
            .scan_items_in_range_paged::<SchemaRetentionAgeEntry>(&start, &end, limit.clamp(1, 256))
            .await?;
        let next = rows.last().map(|(key, _)| key.clone());
        let latest_keys: Vec<_> = rows
            .iter()
            .map(|(_, entry)| retention_age_latest_key(&anchor, &entry.key))
            .collect();
        let latest = self
            .schema_states_store
            .get_items::<SchemaRetentionAgeEntry>(&latest_keys)
            .await?;
        let keys = rows
            .into_iter()
            .zip(latest)
            .filter_map(|((_, entry), current)| {
                (current.as_ref() == Some(&entry)).then_some(entry.key)
            })
            .collect();
        Ok((keys, next))
    }
}
