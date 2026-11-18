//! Per-record replay winner, deferred candidate, baseline, and whole-record materialization.

use super::{has_attachment, MutationManager};
use crate::schema::types::{cas::CasExpectation, KeyValue, Mutation, MutationType};
use crate::schema::SchemaError;
use crate::storage::format::metadata::{ReplayStamp, ReplayTarget};
use lastdb_model::record_replacement::RecordTarget;
use lastdb_model::record_value::StoredRecord;
use std::collections::{BTreeMap, HashMap};

impl MutationManager {
    /// The latest local mutation to this record that has not been read back from the cloud.
    pub async fn pending_replay_record(
        &self,
        schema: &str,
        key: &KeyValue,
    ) -> Result<Option<String>, SchemaError> {
        let address = self.replay_record_address(schema, key)?;
        self.db_ops
            .metadata()
            .pending_replay(ReplayTarget::Record(&address))
            .await
    }

    /// An own pending edit changes an attachment in this record's local preimage.
    pub async fn pending_replay_attachment(
        &self,
        schema: &str,
        key: &KeyValue,
    ) -> Result<bool, SchemaError> {
        let address = self.replay_record_address(schema, key)?;
        self.db_ops
            .metadata()
            .pending_replay_attachment(&address)
            .await
    }

    /// The native replay preimage contains an external or inline attachment.
    pub async fn replay_record_has_attachment(
        &self,
        schema: &str,
        key: &KeyValue,
    ) -> Result<bool, SchemaError> {
        self.db_ops.metadata().require_replay_scope()?;
        let address = self.replay_record_address(schema, key)?;
        let record = self.replay_preimage(address).await?;
        Ok(record.as_deref().is_some_and(has_attachment))
    }

    /// Clear an observed local write when it is still the latest local intent.
    pub async fn clear_pending_replay_record(
        &self,
        schema: &str,
        key: &KeyValue,
        mutation_id: &str,
    ) -> Result<(), SchemaError> {
        let address = self.replay_record_address(schema, key)?;
        let metadata = self.db_ops.metadata();
        if metadata
            .pending_replay(ReplayTarget::Record(&address))
            .await?
            .as_deref()
            == Some(mutation_id)
        {
            self.release_replay_baseline(schema, &address).await?;
        }
        metadata
            .clear_pending_replay(ReplayTarget::Record(&address), mutation_id)
            .await?;
        metadata
            .clear_pending_replay(ReplayTarget::SchemaRecords(schema), mutation_id)
            .await
    }

    /// Stage a greater cloud stamp. Commit the winning value in the same replay scope.
    pub async fn accept_replay_record(
        &self,
        schema: &str,
        key: &KeyValue,
        stamp: &ReplayStamp,
    ) -> Result<bool, SchemaError> {
        let address = self.replay_record_address(schema, key)?;
        self.db_ops
            .metadata()
            .accept_replay(ReplayTarget::Record(&address), stamp)
            .await
    }

    /// Read the greatest accepted cloud stamp for a record in this replay scope.
    pub async fn replay_record_winner(
        &self,
        schema: &str,
        key: &KeyValue,
    ) -> Result<Option<ReplayStamp>, SchemaError> {
        let address = self.replay_record_address(schema, key)?;
        self.db_ops
            .metadata()
            .replay_winner(ReplayTarget::Record(&address))
            .await
    }

    /// Read the encrypted candidate deferred while a local edit is pending.
    pub async fn deferred_replay_record(
        &self,
        schema: &str,
        key: &KeyValue,
    ) -> Result<Option<Vec<u8>>, SchemaError> {
        let address = self.replay_record_address(schema, key)?;
        self.db_ops.metadata().deferred_replay(&address).await
    }

    /// Set or clear the authenticated candidate in the current native replay scope.
    pub async fn stage_deferred_replay_record(
        &self,
        schema: &str,
        key: &KeyValue,
        envelope: Option<Vec<u8>>,
    ) -> Result<(), SchemaError> {
        let address = self.replay_record_address(schema, key)?;
        self.db_ops
            .metadata()
            .stage_deferred_replay(&address, envelope)
            .await
    }

    /// Materialize a winning whole-record change in the caller's native replay scope.
    /// `writer` is the log entry's writer key. A caller that already set one keeps it.
    pub async fn apply_replay_record(
        &self,
        mut mutation: Mutation,
        writer: &str,
    ) -> Result<(), SchemaError> {
        if mutation.writer.is_none() {
            mutation.writer = Some(writer.to_owned());
        }
        self.db_ops.metadata().require_replay_scope()?;
        let address = self.replay_record_address(&mutation.schema_name, &mutation.key_value)?;
        let before = self.replay_preimage(address).await?;
        let live = before.as_ref().filter(|record| record.is_live());
        match (&mutation.mutation_type, live) {
            (MutationType::Delete | MutationType::Purge, None) => return Ok(()),
            (MutationType::Delete | MutationType::Purge, Some(current)) => {
                mutation.alternates.clone_from(&current.alternates);
            }
            (_, Some(_)) => mutation.mutation_type = MutationType::Replace,
            (_, None) => mutation.mutation_type = MutationType::Create,
        }
        mutation.expected =
            live.map_or(CasExpectation::Absent, |current| CasExpectation::Revision {
                revision: current.stamp.revision(),
            });
        self.write_mutations_receipt(vec![mutation]).await?;
        Ok(())
    }
}

pub(crate) fn as_mutation(schema: &str, key: KeyValue, record: Option<StoredRecord>) -> Mutation {
    let mut target = RecordTarget {
        schema: schema.to_owned(),
        key,
        alternates: BTreeMap::new(),
    };
    let kind = if record.is_some() {
        MutationType::Create
    } else {
        MutationType::Delete
    };
    let mut fields = HashMap::new();
    let mut meta = None;
    let mut copied_authorship = None;
    if let Some(record) = record {
        target.alternates = record.alternates;
        fields = record.fields.into_iter().collect();
        meta = record.meta;
        copied_authorship = Some((record.created_by, record.updated_by));
    }
    let mut mutation = Mutation::with_uuid(
        uuid::Uuid::new_v4().to_string(),
        target,
        fields,
        kind,
        CasExpectation::Absent,
    );
    if let Some(meta) = meta {
        mutation.source_file_name = meta.source_file_name;
        mutation.metadata = Some(meta.metadata.into_iter().collect());
    }
    mutation.copied_authorship = copied_authorship;
    mutation
}
