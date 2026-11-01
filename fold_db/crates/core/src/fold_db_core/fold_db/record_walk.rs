//! The record key order of the rows of an unfiltered page of a format 7 home.
//!
//! An unfiltered read walks the records of a schema in record key order
//! (`atom_store/record_walk.rs`), but its rows reach the query reply in a map.
//! The reply builder asks for the order of its rows to drop the probe row and
//! to take the cursor from the last row that the caller receives.

use crate::schema::types::KeyValue;

use super::FoldDB;

impl FoldDB {
    /// The positions of `keys` in record key order: the order in which an
    /// unfiltered read walks the records of `schema_name`. The schema
    /// resolves as the read resolves it. `None` when the schema is absent or
    /// a key does not resolve.
    pub async fn record_walk_order(
        &self,
        schema_name: &str,
        keys: &[KeyValue],
    ) -> Option<Vec<usize>> {
        let schema = self
            .schema_manager
            .get_schema_following_supersession_for_read(schema_name)
            .await
            .ok()??;
        let molecule_keys = self.db_ops.molecule_keys();
        molecule_keys.load_schema(&schema).await.ok()?;
        let atoms = self.db_ops.atoms();
        // Every scope stores one API key at the same suffix, so the personal
        // scope gives the order of the merged walk too.
        let records = keys
            .iter()
            .map(|key| {
                let key = molecule_keys.canonical_key(&schema, key.clone()).ok()?;
                atoms.record_key(&schema, &key, None).ok()
            })
            .collect::<Option<Vec<_>>>()?;
        let mut order: Vec<(&str, usize)> = records
            .iter()
            .enumerate()
            .map(|(at, record)| (record.as_str(), at))
            .collect();
        order.sort_unstable();
        Some(order.into_iter().map(|(_, at)| at).collect())
    }
}
