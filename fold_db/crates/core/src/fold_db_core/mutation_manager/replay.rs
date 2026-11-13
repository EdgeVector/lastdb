//! Read and update signed-log progress inside the native replay write scope.

mod baseline;

use super::{AuthoredMutation, MutationManager, ReplayStamp};
use crate::schema::types::{cas::CasExpectation, KeyValue, Mutation, MutationType};
use crate::schema::{Schema, SchemaError, SchemaState};
use crate::storage::disk_backup::accepted;
use crate::storage::disk_pack::{transaction, PackRuntime};
use crate::storage::format::metadata::ReplayTarget;
use crate::storage::format::records::RecordAddress;
use lastdb_model::record_value::StoredRecord;
use std::sync::Arc;

impl MutationManager {
    /// Raise the durable author clock past a verified cloud ordering time.
    ///
    /// The caller supplies the clamped time from an authenticated entry.
    pub async fn observe_replay_time(&self, ordering_ms: u64) -> Result<(), SchemaError> {
        self.db_ops.metadata().require_replay_scope()?;
        let floor = ordering_ms
            .checked_add(1)
            .and_then(|milliseconds| milliseconds.checked_mul(1_000_000))
            .ok_or_else(|| SchemaError::InvalidData("replay author clock exhausted".into()))?;
        let writer = self.signer.public_key_base64();
        let metadata = self.db_ops.metadata();
        let mut state = metadata.author_clock(&writer).await?.unwrap_or_default();
        if state.physical_nanos < floor {
            state.observe_remote(floor, 0);
            metadata.stage_author_clock(&writer, &state).await?;
        }
        Ok(())
    }

    pub(super) async fn stage_local_replay_records(
        &self,
        runtime: &PackRuntime,
        schema: &str,
        addresses: &[RecordAddress],
        mutations: &[AuthoredMutation],
        preimages: &[Option<Arc<StoredRecord>>],
    ) -> Result<(), SchemaError> {
        if !accepted::capture_enabled(runtime) {
            return Ok(());
        }
        let mut targets: Vec<_> = addresses
            .iter()
            .zip(mutations)
            .map(|(address, item)| {
                (
                    ReplayTarget::Record(address.as_str()),
                    item.mutation.uuid.as_str(),
                )
            })
            .collect();
        if let Some(item) = mutations.last() {
            targets.push((
                ReplayTarget::SchemaRecords(schema),
                item.mutation.uuid.as_str(),
            ));
        }
        let metadata = self.db_ops.metadata();
        self.stage_replay_baselines(addresses, preimages).await?;
        metadata.stage_local_replay_targets(targets).await?;
        metadata
            .stage_pending_replay_attachments(
                addresses
                    .iter()
                    .zip(mutations)
                    .zip(preimages)
                    .filter(|((_, item), before)| {
                        affects_attachment(&item.mutation, before.as_deref())
                    })
                    .map(|((address, _), _)| address.as_str()),
            )
            .await
    }

    /// Protect old, already committed spool items before submitting their unchanged bodies.
    /// New capture installs its own markers in the original write transaction.
    #[doc(hidden)]
    pub async fn stage_legacy_replay_pending(
        &self,
        items: &[(String, Option<KeyValue>, &str, bool)],
    ) -> Result<(), SchemaError> {
        if items.is_empty() {
            return Ok(());
        }
        let runtime = self.db_ops.native_runtime();
        transaction::request_scope(Arc::clone(&runtime), async {
            let mut addresses = Vec::with_capacity(items.len());
            for (schema, key, _, _) in items {
                let address = match key {
                    Some(key) if self.schema_manager.get_schema_metadata(schema)?.is_some() => {
                        Some(self.replay_record_address(schema, key)?)
                    }
                    _ => None,
                };
                addresses.push(address);
            }
            let mut targets = Vec::with_capacity(items.len() * 2);
            let mut attachments = Vec::new();
            for ((schema, key, id, attachment), address) in items.iter().zip(&addresses) {
                if let Some(address) = address {
                    targets.push((ReplayTarget::Record(address), *id));
                    targets.push((ReplayTarget::SchemaRecords(schema), *id));
                    if *attachment {
                        attachments.push(address.as_str());
                    }
                } else if key.is_none() {
                    targets.push((ReplayTarget::Schema(schema), *id));
                }
            }
            let metadata = self.db_ops.metadata();
            metadata.stage_legacy_replay_targets(targets).await?;
            metadata.stage_pending_replay_attachments(attachments).await
        })
        .await
    }

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
    pub async fn apply_replay_record(&self, mut mutation: Mutation) -> Result<(), SchemaError> {
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

    /// Latest local schema definition change awaiting cloud delivery.
    pub async fn pending_replay_schema(&self, schema: &str) -> Result<Option<String>, SchemaError> {
        self.db_ops
            .metadata()
            .pending_replay(ReplayTarget::Schema(schema))
            .await
    }

    /// An identical schema put is a durable no-op, even while a local put is pending.
    pub async fn pending_replay_schema_matches(
        &self,
        definition: &Schema,
    ) -> Result<bool, SchemaError> {
        self.db_ops.metadata().require_replay_scope()?;
        let schemas = self.db_ops.schemas();
        if schemas.get_schema_state(&definition.name).await? == Some(SchemaState::Blocked) {
            return Ok(false);
        }
        Ok(schemas
            .get_schema(&definition.name)
            .await?
            .is_some_and(|installed| installed.to_wire() == definition.to_wire()))
    }

    /// Settle an authenticated own record that precedes this device's pending schema drop.
    pub async fn clear_replay_record_after_schema_drop(
        &self,
        schema: &str,
        mutation_id: &str,
    ) -> Result<bool, SchemaError> {
        let metadata = self.db_ops.metadata();
        metadata.require_replay_scope()?;
        let schemas = self.db_ops.schemas();
        if !schemas.pending_local_drop(schema).await?
            || schemas.get_schema_state(schema).await?.is_some()
            || schemas.get_schema(schema).await?.is_some()
        {
            return Ok(false);
        }
        metadata
            .clear_pending_replay(ReplayTarget::SchemaRecords(schema), mutation_id)
            .await?;
        Ok(true)
    }

    /// Clear the matching local schema change during replay.
    pub async fn clear_pending_replay_schema(
        &self,
        schema: &str,
        mutation_id: &str,
    ) -> Result<(), SchemaError> {
        self.db_ops
            .metadata()
            .clear_pending_replay(ReplayTarget::Schema(schema), mutation_id)
            .await
    }

    /// Whether two entry keys resolve to the same native record.
    pub fn replay_record_key_matches(
        &self,
        schema: &str,
        a: &KeyValue,
        b: &KeyValue,
    ) -> Result<bool, SchemaError> {
        Ok(self.replay_record_address(schema, a)? == self.replay_record_address(schema, b)?)
    }

    fn replay_record_address(&self, schema: &str, key: &KeyValue) -> Result<String, SchemaError> {
        let schema = self.schema_manager.require_schema_metadata(schema)?;
        let profile = schema
            .require_record_keys()?
            .primary
            .range_profile()
            .map_err(SchemaError::InvalidData)?;
        let mut key = key.clone();
        crate::schema::types::key_comparison::canonical_key(profile, &mut key)?;
        self.db_ops.records().record_key(&schema, &key)
    }

    async fn replay_preimage(
        &self,
        address: String,
    ) -> Result<Option<Arc<StoredRecord>>, SchemaError> {
        let [before]: [_; 1] = self
            .db_ops
            .records()
            .read_records(&[address])
            .await?
            .try_into()
            .map_err(|_| SchemaError::InvalidData("incomplete replay preimage read".into()))?;
        Ok(before)
    }
}

fn affects_attachment(mutation: &Mutation, before: Option<&StoredRecord>) -> bool {
    mutation.file_write.is_some()
        || mutation.inline_write.is_some()
        || (matches!(
            mutation.mutation_type,
            MutationType::Delete | MutationType::Purge
        ) && before.is_some_and(has_attachment))
}

fn has_attachment(record: &StoredRecord) -> bool {
    record.is_live() && (record.file.is_some() || record.inline_file.is_some())
}
