//! Reads of conflict rows and markers by key.

use crate::db_operations::atom_store::AtomStore;
use crate::schema::SchemaError;

const READ_CHUNK: usize = 10_000;

impl AtomStore {
    pub(super) async fn read_conflict_keys(
        &self,
        keys: &[&String],
    ) -> Result<Vec<Option<Vec<u8>>>, SchemaError> {
        let mut out = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(READ_CHUNK) {
            let rows = self
                .main_store
                .inner()
                .get_many(chunk.iter().map(|key| key.as_bytes().to_vec()).collect())
                .await
                .map_err(|error| invalid(&format!("read conflict rows: {error}")))?;
            if rows.len() != chunk.len() {
                return Err(invalid("the conflict read batch is incomplete"));
            }
            out.extend(rows);
        }
        Ok(out)
    }
}

pub(super) fn invalid(reason: &str) -> SchemaError {
    SchemaError::InvalidData(format!("record conflict: {reason}"))
}
