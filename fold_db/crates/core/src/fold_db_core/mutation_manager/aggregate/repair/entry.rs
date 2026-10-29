//! Source and target ownership covers every durable repair generation.

use super::super::rules::association_matches;
use super::*;

impl MutationManager {
    /// Rebuild a summary under source and target ownership, leaving valid=0.
    /// Finalization separately verifies the complete durable repair generation.
    pub async fn repair_aggregate_summary_with_access_receipt(
        &self,
        repair: AggregateRepair,
        access_context: &crate::access::AccessContext,
    ) -> Result<AggregateRepairReceipt, SchemaError> {
        self.refuse_logical_capture("aggregate operations")?;
        rules::validate_request(&repair)?;
        let target_key = self
            .resolve_aggregate_target_key(&repair.target_schema_name, &repair.target_key_value)?;
        let storage_prefix = self
            .resolve_aggregate_storage_prefix(
                &repair.source_schema_name,
                &repair.target_schema_name,
                &repair.member_schema_name,
                access_context,
            )
            .await?;
        let source_hash = repair
            .source_partition_key_value
            .hash
            .as_deref()
            .expect("checked source hash");
        let guard_key = aggregate_guard_key(
            storage_prefix.as_deref(),
            &repair.source_schema_name,
            source_hash,
        );
        let _guard_lock = self
            .aggregate_named_lock(format!("aggregate_guard_lock\u{1f}{guard_key}"))
            .write_owned()
            .await;
        self.with_aggregate_guards(_guard_lock, async {
            let source_hash = repair
                .source_partition_key_value
                .hash
                .as_deref()
                .expect("checked source hash");
            let guard = self
                .read_reconciled_aggregate_guard(&guard_key, storage_prefix.as_deref())
                .await?
                .ok_or_else(|| {
                    SchemaError::InvalidData("aggregate source partition is not enrolled".into())
                })?;
            guard.validate().map_err(SchemaError::InvalidData)?;
            if !association_matches(
                &guard,
                &repair.source_schema_name,
                source_hash,
                &repair.target_schema_name,
                &target_key,
                &repair.member_schema_name,
            ) {
                return Err(SchemaError::InvalidData(
                    "aggregate repair association does not match the enrolled guard".into(),
                ));
            }
            self.validate_enrolled_aggregate_schemas(&guard)?;

            let target_lock_key = Self::aggregate_target_lock_key(
                storage_prefix.as_deref(),
                &repair.target_schema_name,
                &target_key,
            );
            let _target_lock = self
                .aggregate_named_lock(target_lock_key)
                .write_owned()
                .await;
            self.with_aggregate_guards(_target_lock, async {
                self.repair_guarded_summary(repair, target_key, &guard, storage_prefix.as_deref())
                    .await
            })
            .await
        })
        .await
    }

    async fn repair_guarded_summary(
        &self,
        repair: AggregateRepair,
        target_key: KeyValue,
        guard: &AggregateGuard,
        storage_prefix: Option<&str>,
    ) -> Result<AggregateRepairReceipt, SchemaError> {
        self.check_repair_target_owner(guard, storage_prefix)
            .await?;
        self.check_repair_guard_token(&repair, &target_key, guard, storage_prefix)
            .await?;
        let repair_token = format!("repair:{}", uuid::Uuid::new_v4());
        self.invalidate_guard_targets_durable(
            &[guard],
            &format!("repair-in-progress:{}", uuid::Uuid::new_v4()),
            "",
            storage_prefix,
            WriteOrigin::Replay,
        )
        .await?;

        let rows = self
            .collect_repair_member_rows(guard, storage_prefix)
            .await?;
        let source_winners = self
            .collect_repair_source_winners(guard, storage_prefix)
            .await?;
        let plan = rules::reconcile_members(guard, rows, &source_winners)?;
        if storage_prefix.is_some() && !plan.orphan_purges.is_empty() {
            return Err(SchemaError::InvalidData(
                "scoped aggregate repair found orphan members; scoped hard cleanup is not supported".into(),
            ));
        }
        // Preserve cleanup before the final completeness refusal. Computing the
        // completeness fact must not move that refusal ahead of durable cleanup.
        if !plan.orphan_purges.is_empty() {
            self.enqueue_hard_erasures_on_lanes(
                plan.orphan_purges,
                storage_prefix,
                crate::fold_db_core::purge::PurgeMissingPolicy::Skip,
                crate::fold_db_core::purge::HardEraseVerb::Purge,
                None,
            )
            .await?;
        }
        if !plan.complete {
            return Err(SchemaError::InvalidData(
                "aggregate repair source/member key sets are not a bijection".into(),
            ));
        }
        self.write_repair_summary(
            repair.target_schema_name,
            target_key,
            guard,
            plan.totals,
            repair_token,
            storage_prefix,
        )
        .await
    }
}
