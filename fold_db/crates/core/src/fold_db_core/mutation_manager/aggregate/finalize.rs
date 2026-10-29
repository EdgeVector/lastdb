//! Aggregate summary finalize path.

use super::enrolled::pin_enrolled;
use super::rules::summary_mutation;
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
        self.with_enrolled_aggregate_target(
            finalize,
            access_context,
            "checked hash",
            "aggregate finalize association does not match the enrolled guard",
            |finalize, manager, target_key, guard, storage_prefix| {
                pin_enrolled(async move {
                    manager
                        .check_aggregate_target_owner(
                            guard,
                            storage_prefix,
                            "aggregate target owner does not match the finalize source partition",
                        )
                        .await?;
                    manager
                        .finalize_with_repair_grant(finalize, target_key, guard, storage_prefix)
                        .await
                })
            },
        )
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

    /// Read the current summary. Corruption invalidates the guard before refusal;
    /// the caller then evaluates token, metric, grant and valid-marker rules.
    async fn read_finalize_summary(
        &self,
        finalize: &AggregateFinalize,
        target_key: &KeyValue,
        guard: &AggregateGuard,
        storage_prefix: Option<&str>,
    ) -> Result<BTreeMap<String, serde_json::Value>, SchemaError> {
        let mut summary_field_names = guard.metric_fields.clone();
        summary_field_names.push(AGGREGATE_VALID_FIELD.to_string());
        summary_field_names.push(AGGREGATE_GUARD_TOKEN_FIELD.to_string());
        self.read_summary_fields(
            finalize.target_schema_name.clone(),
            target_key,
            &summary_field_names,
            guard,
            storage_prefix,
            "finalize",
        )
        .await?
        .ok_or_else(|| SchemaError::InvalidData("aggregate finalize summary is absent".into()))
    }

    /// Write the `valid=1` marker mutation and return its durable receipt.
    async fn write_finalize_marker(
        &self,
        finalize: AggregateFinalize,
        target_key: KeyValue,
        storage_prefix: Option<&str>,
    ) -> Result<ResidentCommitReceipt, SchemaError> {
        let summary = summary_mutation(
            finalize.target_schema_name,
            target_key,
            HashMap::new(),
            true,
            finalize.expected_guard_token,
            String::new(),
        );
        self.commit_clocked_mutations(
            vec![summary],
            storage_prefix,
            WriteOrigin::Request,
            "finalize marker",
            true,
        )
        .await
    }
}
