//! Source and target ownership covers every durable repair generation.

use super::super::enrolled::pin_enrolled;
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
        self.with_enrolled_aggregate_target(
            repair,
            access_context,
            "checked source hash",
            "aggregate repair association does not match the enrolled guard",
            |repair, manager, target_key, guard, storage_prefix| {
                pin_enrolled(async move {
                    manager
                        .repair_guarded_summary(repair, target_key, guard, storage_prefix)
                        .await
                })
            },
        )
        .await
    }

    async fn repair_guarded_summary(
        &self,
        repair: AggregateRepair,
        target_key: KeyValue,
        guard: &AggregateGuard,
        storage_prefix: Option<&str>,
    ) -> Result<AggregateRepairReceipt, SchemaError> {
        self.check_aggregate_target_owner(
            guard,
            storage_prefix,
            "aggregate target owner does not match the repair source partition",
        )
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
