//! Bind a store-local revision to the mutation identity shared by both stores.

use super::{ChangedRecord, MutationManager};
use crate::db_operations::record_store::RecordChange;
use crate::schema::types::{KeyValue, MutationType};
use crate::schema::SchemaError;
use crate::storage::format::metadata::SharedCommentWrite;
use schema_types::STANDARD_COMMENT_SCHEMA;

/// The same record version expressed in local and publisher terms.
pub struct SharedRecordRevision {
    /// Conditional-write token in this store.
    pub local_revision: String,
    /// Mutation ID carried in the publisher's accepted record.
    pub mutation_id: String,
    /// Verified original receiver that created a standard shared comment.
    pub comment_creator: Option<String>,
}

impl MutationManager {
    /// Read the native record and its publisher mutation ID from one committed view.
    pub async fn shared_record_revision(
        &self,
        schema: &str,
        key: &KeyValue,
    ) -> Result<Option<SharedRecordRevision>, SchemaError> {
        let address = self.replay_record_address(schema, key)?;
        self.db_ops
            .read_scope()
            .await?
            .run_result(async {
                let preimage = self.replay_preimage(address.clone()).await?;
                let Some(record) = preimage.filter(|row| row.is_live()) else {
                    return Ok(None);
                };
                let local_revision = record.stamp.revision();
                let meta = self.db_ops.metadata();
                let Some(mutation_id) = meta
                    .shared_record_mutation(&address, &local_revision)
                    .await?
                else {
                    return Ok(None);
                };
                let comment_creator = if schema == STANDARD_COMMENT_SCHEMA {
                    meta.shared_comment_creator(&address, &local_revision)
                        .await?
                } else {
                    None
                };
                Ok(Some(SharedRecordRevision {
                    local_revision,
                    mutation_id,
                    comment_creator,
                }))
            })
            .await
    }

    pub(super) async fn stage_shared_revisions(
        &self,
        schema: &str,
        records: &[RecordChange],
        changes: &[ChangedRecord<'_>],
    ) -> Result<(), SchemaError> {
        let meta = self.db_ops.metadata();
        meta.stage_shared_record_mutations(records.iter().zip(changes).map(|(row, change)| {
            (
                row.key.as_str(),
                row.after
                    .is_live()
                    .then_some((&row.after.stamp, change.mutation.uuid.as_str())),
            )
        }))
        .await?;
        if schema == STANDARD_COMMENT_SCHEMA {
            meta.stage_shared_comment_creators(records.iter().zip(changes).map(|(row, change)| {
                let mutation = &change.mutation;
                let creator = mutation.contribution.as_ref().filter(|admission| {
                    mutation.mutation_type == MutationType::Create && admission.schema_scope
                });
                SharedCommentWrite {
                    address: row.key.as_str(),
                    before: row
                        .before
                        .as_ref()
                        .filter(|r| r.is_live())
                        .map(|r| &r.stamp),
                    after: row.after.is_live().then_some(&row.after.stamp),
                    creator: creator.map(|admission| admission.writer.as_str()),
                }
            }))
            .await?;
        }
        Ok(())
    }
}
