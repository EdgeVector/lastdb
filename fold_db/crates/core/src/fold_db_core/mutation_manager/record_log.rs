//! Host-supplied record of a committed mutation. The engine stores records only.

use super::ChangedRecord;
use crate::db_operations::record_store::RecordChange;
use crate::schema::SchemaError;

/// One record write inside a native commit.
pub struct RecordWriteNote {
    pub address: String,
    pub schema_name: String,
    pub before_revision: Option<String>,
    pub after_revision: Option<String>,
    pub mutation_id: String,
}

/// Durable notes applied after the native commit succeeds.
pub trait RecordWriteLog: Send + Sync {
    /// Plan the notes. The returned work runs only after the commit is durable.
    fn prepare(
        &self,
        notes: &[RecordWriteNote],
    ) -> Result<Box<dyn FnOnce() -> Result<(), String> + Send>, SchemaError>;
}

pub(crate) fn record_notes(
    schema: &str,
    records: &[RecordChange],
    changes: &[ChangedRecord<'_>],
) -> Vec<RecordWriteNote> {
    records
        .iter()
        .zip(changes)
        .map(|(row, change)| {
            let mutation = change.mutation;
            RecordWriteNote {
                address: row.key.as_str().to_owned(),
                schema_name: schema.to_owned(),
                before_revision: row
                    .before
                    .as_ref()
                    .filter(|record| record.is_live())
                    .map(|record| record.stamp.revision()),
                after_revision: row.after.is_live().then(|| row.after.stamp.revision()),
                mutation_id: mutation.uuid.clone(),
            }
        })
        .collect()
}
