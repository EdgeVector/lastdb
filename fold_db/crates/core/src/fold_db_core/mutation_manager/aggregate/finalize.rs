//! Aggregate summary finalize path.

use super::rules::{association_matches, summary_mutation, target_owner_matches};
use super::*;

mod rules;
use rules::FinalizeAction;

impl MutationManager {
    /// Mark one independently repaired summary valid if no source write raced.
    pub async fn finalize_aggregate_summary_with_access_receipt(
        &self,
        finalize: AggregateFinalize,
        access_context: &crate::access::AccessContext,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        self.refuse_logical_capture("aggregate operations")?;
        rules::validate_request(&finalize)?;
        let target_key = self.resolve_aggregate_target_key(
            &finalize.target_schema_name,
            &finalize.target_key_value,
        )?;
        let storage_prefix = self
            .resolve_aggregate_storage_prefix(
                &finalize.source_schema_name,
                &finalize.target_schema_name,
                &finalize.member_schema_name,
                access_context,
            )
            .await?;
        let source_hash = finalize
            .source_partition_key_value
            .hash
            .as_deref()
            .expect("checked hash");
        let guard_key = aggregate_guard_key(
            storage_prefix.as_deref(),
            &finalize.source_schema_name,
            source_hash,
        );
        let _guard_lock = self
            .aggregate_named_lock(format!("aggregate_guard_lock\u{1f}{guard_key}"))
            .write_owned()
            .await;
        self.with_aggregate_guards(_guard_lock, async {
            let source_hash = finalize
                .source_partition_key_value
                .hash
                .as_deref()
                .expect("checked hash");
            let guard = self
                .read_reconciled_aggregate_guard(&guard_key, storage_prefix.as_deref())
                .await?
                .ok_or_else(|| {
                    SchemaError::InvalidData("aggregate source partition is not enrolled".into())
                })?;
            guard.validate().map_err(SchemaError::InvalidData)?;
            if !association_matches(
                &guard,
                &finalize.source_schema_name,
                source_hash,
                &finalize.target_schema_name,
                &target_key,
                &finalize.member_schema_name,
            ) {
                return Err(SchemaError::InvalidData(
                    "aggregate finalize association does not match the enrolled guard".into(),
                ));
            }
            self.validate_enrolled_aggregate_schemas(&guard)?;

            let target_lock_key = Self::aggregate_target_lock_key(
                storage_prefix.as_deref(),
                &finalize.target_schema_name,
                &target_key,
            );
            let _target_lock = self
                .aggregate_named_lock(target_lock_key)
                .write_owned()
                .await;
            self.with_aggregate_guards(_target_lock, async {
                self.check_finalize_target_owner(&guard, storage_prefix.as_deref())
                    .await?;
                self.finalize_with_repair_grant(
                    finalize,
                    target_key,
                    &guard,
                    storage_prefix.as_deref(),
                )
                .await
            })
            .await
        })
        .await
    }

    /// Check the durable repair grant against the summary, then mark the
    /// summary valid (or accept an already finalized one) and retire the grant.
    async fn finalize_with_repair_grant(
        &self,
        finalize: AggregateFinalize,
        target_key: KeyValue,
        guard: &AggregateGuard,
        storage_prefix: Option<&str>,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        let grant_key = Self::aggregate_repair_grant_key(
            storage_prefix,
            &guard.target_schema_name,
            &guard.target_key,
        );
        let finalized_key = Self::aggregate_finalized_grant_key(
            storage_prefix,
            &guard.target_schema_name,
            &guard.target_key,
        );
        let active_grant = self
            .db_ops
            .metadata()
            .get_typed::<AggregateRepairGrant>(&grant_key)
            .await?;
        let finalized_grant = self
            .db_ops
            .metadata()
            .get_typed::<AggregateRepairGrant>(&finalized_key)
            .await?;
        let grant = active_grant
            .as_ref()
            .or(finalized_grant.as_ref())
            .ok_or_else(|| {
                SchemaError::InvalidData(
                    "aggregate finalize requires a durable repair grant".into(),
                )
            })?;
        let status = self
            .read_finalize_summary(&finalize, &target_key, guard, storage_prefix)
            .await?;
        let action =
            rules::finalize_action(&finalize, guard, grant, &status, active_grant.is_some())?;
        if matches!(action, FinalizeAction::AlreadyValid) {
            if active_grant.is_some() {
                self.db_ops
                    .metadata()
                    .put_typed_durable(&finalized_key, grant)
                    .await?;
                self.db_ops
                    .metadata()
                    .delete_typed_durable(&grant_key)
                    .await?;
            }
            let mut receipt = ResidentCommitReceipt::empty();
            receipt.durability = ResidentDurability::Durable;
            return Ok(receipt);
        }
        let receipt = self
            .write_finalize_marker(finalize, target_key, storage_prefix)
            .await?;
        self.db_ops
            .metadata()
            .put_typed_durable(&finalized_key, grant)
            .await?;
        self.db_ops
            .metadata()
            .delete_typed_durable(&grant_key)
            .await?;
        Ok(receipt)
    }

    /// The committed target owner must be the guard being finalized.
    async fn check_finalize_target_owner(
        &self,
        guard: &AggregateGuard,
        storage_prefix: Option<&str>,
    ) -> Result<(), SchemaError> {
        let reverse_key = aggregate_target_guard_key(
            storage_prefix,
            &guard.target_schema_name,
            &guard.target_key,
        );
        let reverse = self
            .read_active_target_guard(&reverse_key, storage_prefix)
            .await?
            .ok_or_else(|| {
                SchemaError::InvalidData("aggregate target has no committed owner".into())
            })?;
        if !target_owner_matches(&reverse, guard) {
            return Err(SchemaError::InvalidData(
                "aggregate target owner does not match the finalize source partition".into(),
            ));
        }
        Ok(())
    }

    /// Read the current summary. Corruption invalidates the guard before refusal;
    /// the caller then evaluates token, metric, grant and valid-marker rules.
    async fn read_finalize_summary(
        &self,
        finalize: &AggregateFinalize,
        target_key: &KeyValue,
        guard: &AggregateGuard,
        storage_prefix: Option<&str>,
    ) -> Result<BTreeMap<String, serde_json::Value>, SchemaError> {
        let probe = Self::aggregate_probe(finalize.target_schema_name.clone(), target_key.clone());
        let mut summary_field_names = guard.metric_fields.clone();
        summary_field_names.push(AGGREGATE_VALID_FIELD.to_string());
        summary_field_names.push(AGGREGATE_GUARD_TOKEN_FIELD.to_string());
        let status = match self
            .read_current_row_fields(&probe, &summary_field_names, storage_prefix)
            .await?
        {
            CurrentRowFields::Absent => {
                return Err(SchemaError::InvalidData(
                    "aggregate finalize summary is absent".into(),
                ));
            }
            CurrentRowFields::Present(fields) => fields,
            CurrentRowFields::Corrupt { field, atom_uuid } => {
                self.invalidate_guard_targets_durable(
                    &[guard],
                    &format!("finalize-corrupt:{}", uuid::Uuid::new_v4()),
                    "",
                    storage_prefix,
                    WriteOrigin::Replay,
                )
                .await?;
                return Err(SchemaError::InvalidData(format!(
                    "aggregate finalize found unresolved summary field '{field}' atom '{atom_uuid}'"
                )));
            }
        };
        Ok(status)
    }

    /// Write the `valid=1` marker mutation and return its durable receipt.
    async fn write_finalize_marker(
        &self,
        finalize: AggregateFinalize,
        target_key: KeyValue,
        storage_prefix: Option<&str>,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        let mut summary = summary_mutation(
            finalize.target_schema_name,
            target_key,
            HashMap::new(),
            true,
            finalize.expected_guard_token,
            String::new(),
        );
        let clock = self.prepare_mutation_author_clocks(std::slice::from_mut(&mut summary))?;
        let revision = summary.logical_counter;
        if let Some((reservation, state)) = clock {
            reservation.submit_and_wait(state).await?;
        }
        let result = self
            .write_mutations_batch_inner(vec![summary], storage_prefix, WriteOrigin::Request)
            .await;
        let mut receipt = Self::require_durable_receipt(result?, "finalize marker")?;
        receipt.revision = Some(revision);
        Ok(receipt)
    }
}
